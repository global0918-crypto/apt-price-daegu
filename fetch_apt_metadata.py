#!/usr/bin/env python3
"""
대구 아파트 단지 메타데이터 수집 (세대수/동수/최고층).
API 1: AptListService2/getLegaldongAptList  → 단지명 + kaptCode
API 2: AptBasisInfoServiceV3/getAphusBassInfoV3 → 세대수/동수/사용승인일/최고층
결과: data/apt_metadata.json
"""
import os, json, time, requests
import xml.etree.ElementTree as ET
from pathlib import Path

API_KEY = os.environ.get("API_KEY", "")
if not API_KEY:
    import sys
    print("오류: API_KEY 환경변수가 설정되지 않았습니다.", file=sys.stderr)
    sys.exit(1)

LIST_URL = "http://apis.data.go.kr/1613000/AptListService2/getLegaldongAptList"
INFO_URL = "http://apis.data.go.kr/1613000/AptBasisInfoServiceV3/getAphusBassInfoV3"
OUTPUT   = Path(__file__).parent / "data" / "apt_metadata.json"

DAEGU_DISTRICTS = [
    ("중구",   "27110"), ("동구",   "27140"), ("서구",   "27170"),
    ("남구",   "27200"), ("북구",   "27230"), ("수성구", "27260"),
    ("달서구", "27290"), ("달성군", "27710"), ("군위군", "27720"),
]


def _text(el, tag):
    found = el.find(tag)
    return found.text.strip() if found is not None and found.text else ""


def _int(val):
    try:
        return int(str(val).replace(",", "").strip())
    except Exception:
        return None


def fetch_apt_list(bjd_code, page=1, num_rows=1000):
    try:
        r = requests.get(LIST_URL, params={
            "serviceKey": API_KEY, "bjdCode": bjd_code,
            "numOfRows": num_rows, "pageNo": page,
        }, timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = []
        for item in root.iter("item"):
            code = _text(item, "kaptCode")
            name = _text(item, "kaptName")
            if code and name:
                items.append({"kaptCode": code, "kaptName": name})
        total_count_el = root.find(".//totalCount")
        total = int(total_count_el.text) if total_count_el is not None and total_count_el.text else 0
        return items, total
    except Exception as e:
        print(f"  [오류] {bjd_code} p{page}: {e}")
        return [], 0


def fetch_apt_info(kapt_code):
    try:
        r = requests.get(INFO_URL, params={
            "serviceKey": API_KEY, "kaptCode": kapt_code,
        }, timeout=30)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        item = root.find(".//item")
        if item is None:
            return None
        return {
            "households": _int(_text(item, "kaptdaCnt")),
            "buildings":  _int(_text(item, "kaptDongCnt")),
            "use_date":   _text(item, "kaptUsedate"),
            "floor_max":  _int(_text(item, "kaptTopFloor")),
            "address":    _text(item, "kaptAddr"),
        }
    except Exception as e:
        print(f"  [오류] {kapt_code}: {e}")
        return None


def main():
    print("=== 대구 아파트 단지 메타데이터 수집 ===\n")

    # 기존 캐시 로드
    existing = {}
    if OUTPUT.exists():
        with open(OUTPUT, encoding="utf-8") as f:
            data = json.load(f)
        existing = data.get("items", data)
        print(f"기존 캐시: {len(existing)}건\n")

    # Step 1: 구군별 단지 목록 수집
    all_apts = []
    for gu, code in DAEGU_DISTRICTS:
        print(f"[목록] {gu} ({code}) ...", end=" ", flush=True)
        page, num_rows = 1, 1000
        while True:
            items, total = fetch_apt_list(code, page=page, num_rows=num_rows)
            all_apts.extend({"gu": gu, **it} for it in items)
            fetched = (page - 1) * num_rows + len(items)
            if fetched >= total or not items:
                break
            page += 1
            time.sleep(0.2)
        print(f"{len([a for a in all_apts if a['gu']==gu])}개")
        time.sleep(0.3)

    # kaptCode 중복 제거 (구군 코드가 겹칠 수 있음)
    seen_codes = {}
    for apt in all_apts:
        seen_codes[apt["kaptCode"]] = apt
    unique_apts = list(seen_codes.values())
    print(f"\n총 {len(unique_apts)}개 단지\n")

    # Step 2: 단지별 상세 정보 수집
    results = dict(existing)
    new_count = 0
    for i, apt in enumerate(unique_apts, 1):
        name     = apt["kaptName"]
        code     = apt["kaptCode"]
        gu       = apt["gu"]

        # 이미 세대수 있으면 스킵
        if name in existing and existing[name].get("households"):
            continue

        if i % 100 == 0:
            print(f"  [{i}/{len(unique_apts)}] 진행 중...")

        info = fetch_apt_info(code)
        if info:
            results[name] = {"kaptCode": code, "gu": gu, **info}
            new_count += 1
        else:
            results[name] = {"kaptCode": code, "gu": gu,
                             "households": None, "buildings": None,
                             "use_date": None, "floor_max": None}

        if new_count > 0 and new_count % 100 == 0:
            with open(OUTPUT, "w", encoding="utf-8") as f:
                json.dump({"updatedAt": time.strftime("%Y-%m-%dT%H:%M:%S"),
                           "totalCount": len(results), "items": results},
                          f, ensure_ascii=False, indent=2)
            print(f"  [중간저장] {len(results)}건")

        time.sleep(0.3)

    OUTPUT.parent.mkdir(exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump({"updatedAt": time.strftime("%Y-%m-%dT%H:%M:%S"),
                   "totalCount": len(results), "items": results},
                  f, ensure_ascii=False, indent=2)

    matched = sum(1 for v in results.values() if v.get("households"))
    print(f"\n완료: {len(results)}건 저장 / 세대수 수집 {matched}건")

    # 샘플 출력
    print("\n=== 샘플 5개 ===")
    sample = [(k, v) for k, v in results.items() if v.get("households")][:5]
    for name, info in sample:
        print(f"  {name}: {info.get('households')}세대 {info.get('buildings')}동 "
              f"{info.get('use_date','')[:6]} ({info.get('gu','')})")


if __name__ == "__main__":
    main()
