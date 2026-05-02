#!/usr/bin/env python3
"""대구 아파트 전월세 실거래 수집 → data/rent_transactions.json 저장"""
import requests
import xml.etree.ElementTree as ET
import json
import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

API_KEY = os.environ.get("MOLIT_API_KEY") or os.environ.get("API_KEY", "")
if not API_KEY:
    print("오류: MOLIT_API_KEY 또는 API_KEY 환경변수가 설정되지 않았습니다.", file=sys.stderr)
    sys.exit(1)

API_URL = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"

BASE   = os.path.dirname(os.path.abspath(__file__))
DATA   = os.path.join(BASE, "data")
OUTPUT = os.path.join(DATA, "rent_transactions.json")

DISTRICTS = [
    ("중구",   "27110"), ("동구",   "27140"), ("서구",   "27170"),
    ("남구",   "27200"), ("북구",   "27230"), ("수성구", "27260"),
    ("달서구", "27290"), ("달성군", "27710"), ("군위군", "27720"),
]


def _v(elem, tag):
    """XML 요소에서 태그 텍스트 안전 추출."""
    found = elem.find(tag)
    if found is None or found.text is None:
        return ""
    return found.text.strip()


def _parse_int(val):
    """문자열 정수 변환. 실패 시 0."""
    if not val:
        return 0
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, AttributeError):
        return 0


def _parse_rgst(raw):
    """신고일 문자열 → YYYY-MM-DD (실패 시 '')"""
    if not raw:
        return ""
    r = raw.replace("-", "").replace(".", "").replace(" ", "")
    if len(r) == 6 and r.isdigit():
        return f"20{r[:2]}-{r[2:4]}-{r[4:]}"
    if len(r) == 8 and r.isdigit():
        return f"{r[:4]}-{r[4:6]}-{r[6:]}"
    if len(raw) == 10 and raw[4] == "-":
        return raw
    return ""


def _rent_tx_key(t):
    return (
        t.get("apt_name", ""),
        t.get("deal_date", ""),
        round(float(t.get("area", 0))),
        int(t.get("floor", 0)),
        int(t.get("deposit", 0)),
        int(t.get("monthly_rent", 0)),
    )


def load_prev_rent_state(path):
    prev_keys, prev_rgst = set(), {}
    if not os.path.exists(path):
        return prev_keys, prev_rgst
    try:
        with open(path, encoding="utf-8") as f:
            prev = json.load(f)
        mi = prev.get("month_items", {})
        all_prev = mi.get("jeonse", []) + mi.get("wolse", [])
        for t in all_prev:
            k = _rent_tx_key(t)
            prev_keys.add(k)
            if t.get("rgst_date"):
                prev_rgst[k] = t["rgst_date"]
        print(f"  이전 데이터: {len(prev_keys)}건 (rgst_date 보유: {len(prev_rgst)}건)")
    except Exception as e:
        print(f"  이전 데이터 로드 실패: {e}")
    return prev_keys, prev_rgst


def fetch_rent_month(lawd_cd, ym, gu_name):
    """
    특정 법정동 코드 + 월의 전월세 거래를 조회해 파싱된 dict 리스트 반환.
    fetch_history_daegu_rent.py에서도 import해서 재사용.
    """
    try:
        resp = requests.get(API_URL, params={
            "serviceKey": API_KEY,
            "LAWD_CD":    lawd_cd,
            "DEAL_YMD":   ym,
            "numOfRows":  1000,
            "pageNo":     1,
        }, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        err_cd = root.findtext(".//errCd") or ""
        if err_cd and err_cd not in ("00", "0"):
            print(f"    API 오류코드: {err_cd} {root.findtext('.//errMsg') or ''}")
            return []

        records = []
        for item in root.findall(".//item"):
            apt = _v(item, "aptNm")
            if not apt:
                continue

            monthly_rent = _parse_int(_v(item, "monthlyRent"))
            deposit      = _parse_int(_v(item, "deposit"))

            dy = _v(item, "dealYear")
            dm = _v(item, "dealMonth").zfill(2)
            dd = _v(item, "dealDay").zfill(2)

            try:    area = round(float(_v(item, "excluUseAr") or 0), 2)
            except: area = 0.0
            try:    floor = int(_v(item, "floor") or 0)
            except: floor = 0
            try:    build_year = int(_v(item, "buildYear") or 0)
            except: build_year = 0

            records.append({
                "gugun":             gu_name,
                "dong":              _v(item, "umdNm"),
                "apt_name":          apt,
                "area":              area,
                "floor":             floor,
                "build_year":        build_year,
                "deal_date":         f"{dy}-{dm}-{dd}",
                "rgst_date":         _parse_rgst(_v(item, "rdealDay")),
                "deposit":           deposit,
                "monthly_rent":      monthly_rent,
                "trade_type":        "wolse" if monthly_rent > 0 else "jeonse",
                "contract_term":     _v(item, "contractTerm"),
                "contract_type":     _v(item, "contractType"),
                "use_rr_right":      _v(item, "useRRRight"),
                "pre_deposit":       _parse_int(_v(item, "preDeposit")),
                "pre_monthly_rent":  _parse_int(_v(item, "preMonthlyRent")),
            })

        return records

    except requests.RequestException as e:
        print(f"    네트워크 오류: {e}")
        return []
    except ET.ParseError as e:
        print(f"    XML 파싱 오류: {e}")
        return []
    except Exception as e:
        print(f"    오류: {e}")
        return []


def main():
    now_kst   = datetime.now(ZoneInfo("Asia/Seoul"))
    today_str = now_kst.strftime("%Y-%m-%d")

    # 당월 + 전월 조회 (월초 전월 말일 신고분 포착)
    months = []
    cur = now_kst.replace(day=1)
    for _ in range(2):
        months.append(cur.strftime("%Y%m"))
        cur = (cur - timedelta(days=1)).replace(day=1)
    ym = months[0]

    print(f"=== 대구 전월세 실거래 수집 ({now_kst.strftime('%Y-%m-%d %H:%M KST')}) ===")
    print(f"조회 년월: {', '.join(months)}\n")

    print("[0] 이전 스냅샷 로드")
    prev_keys, prev_rgst = load_prev_rent_state(OUTPUT)
    is_first_run = len(prev_keys) == 0

    all_items = []
    seen_keys = set()

    for name, code in DISTRICTS:
        for m in months:
            print(f"  [{name}] {m} 조회...", end=" → ")
            items = fetch_rent_month(code, m, name)
            new_items = []
            for item in items:
                k = _rent_tx_key(item)
                if k not in seen_keys:
                    seen_keys.add(k)
                    new_items.append(item)
            jeonse_cnt = sum(1 for i in new_items if i["trade_type"] == "jeonse")
            wolse_cnt  = sum(1 for i in new_items if i["trade_type"] == "wolse")
            print(f"전세 {jeonse_cnt}건, 월세 {wolse_cnt}건")
            all_items.extend(new_items)
            time.sleep(0.3)

    # 신규 거래 감지 & rgst_date 자동 부여
    new_today = 0
    for t in all_items:
        k = _rent_tx_key(t)
        if t["rgst_date"]:
            pass
        elif k in prev_rgst:
            t["rgst_date"] = prev_rgst[k]
        elif not is_first_run and k not in prev_keys:
            t["rgst_date"] = today_str
            new_today += 1

    print(f"\n[신규 거래 감지] {new_today}건 (rgst_date={today_str} 자동 부여)")
    if is_first_run:
        print("  ※ 최초 실행 — 신규 감지 스킵 (다음 실행부터 적용)")

    # 오늘 신고분 필터
    today_items  = [i for i in all_items if i["rgst_date"] == today_str]
    today_jeonse = [i for i in today_items if i["trade_type"] == "jeonse"]
    today_wolse  = [i for i in today_items if i["trade_type"] == "wolse"]
    all_jeonse   = [i for i in all_items   if i["trade_type"] == "jeonse"]
    all_wolse    = [i for i in all_items   if i["trade_type"] == "wolse"]

    print(f"\n=== 결과 ===")
    print(f"신고일 {today_str} 기준: 전세 {len(today_jeonse)}건, 월세 {len(today_wolse)}건")
    print(f"전체:                  전세 {len(all_jeonse)}건, 월세 {len(all_wolse)}건")

    payload = {
        "generated_at": now_kst.isoformat(),
        "report_date":  today_str,
        "query_ym":     ym,
        "total": {
            "today_jeonse": len(today_jeonse),
            "today_wolse":  len(today_wolse),
            "month_jeonse": len(all_jeonse),
            "month_wolse":  len(all_wolse),
        },
        "today_items": {
            "jeonse": today_jeonse,
            "wolse":  today_wolse,
        },
        "month_items": {
            "jeonse": all_jeonse,
            "wolse":  all_wolse,
        },
    }

    os.makedirs(DATA, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    kb = os.path.getsize(OUTPUT) / 1024
    print(f"\n✅ 저장 완료: {OUTPUT} ({kb:.0f} KB)")

    if today_jeonse:
        print(f"\n[오늘 전세 샘플 3건]")
        for item in today_jeonse[:3]:
            print(f"  {item['apt_name']} ({item['gugun']} {item['dong']}) "
                  f"전용 {item['area']}㎡ {item['floor']}층 보증금 {item['deposit']:,}만원")

    if today_wolse:
        print(f"\n[오늘 월세 샘플 3건]")
        for item in today_wolse[:3]:
            print(f"  {item['apt_name']} ({item['gugun']} {item['dong']}) "
                  f"전용 {item['area']}㎡ {item['floor']}층 "
                  f"보증금 {item['deposit']:,}만원 / 월 {item['monthly_rent']:,}만원")


if __name__ == "__main__":
    main()
