#!/usr/bin/env python3
"""대구 아파트 단지 기본정보 수집 -> data/apt_info.json 저장.

두 MOLIT API 사용:
1. AptListService/getLegaldongAptList   : 단지코드 + 단지명 목록
2. AptBasisInfoService/getAphusBassInfo : 세대수, 동수, 건축년도, 난방방식, 시공사 등
"""
import requests
import xml.etree.ElementTree as ET
import json
import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

API_KEY = os.environ.get("MOLIT_API_KEY") or os.environ.get("API_KEY", "")
if not API_KEY:
    print("오류: API_KEY 환경변수가 설정되지 않았습니다.", file=sys.stderr)
    sys.exit(1)

LIST_URL = "http://apis.data.go.kr/1611000/AptListService/getLegaldongAptList"
INFO_URL = "http://apis.data.go.kr/1611000/AptBasisInfoService/getAphusBassInfo"

BASE   = os.path.dirname(os.path.abspath(__file__))
DATA   = os.path.join(BASE, "data")
OUTPUT = os.path.join(DATA, "apt_info.json")

# 대구 법정동코드 (5자리)
DISTRICTS = [
    ("중구",   "27110"), ("동구",   "27140"), ("서구",   "27170"),
    ("남구",   "27200"), ("북구",   "27230"), ("수성구", "27260"),
    ("달서구", "27290"), ("달성군", "27710"), ("군위군", "27720"),
]


def _v(root, tag, default=""):
    el = root.find(f".//{tag}")
    if el is None or el.text is None:
        return default
    return el.text.strip()


def fetch_apt_list(bjd_code, gu_name):
    """법정동코드로 단지 목록 전체 조회 (페이지 자동 순회)."""
    result = []
    page = 1
    while True:
        try:
            resp = requests.get(LIST_URL, params={
                "serviceKey": API_KEY,
                "bjdCode":    bjd_code,
                "numOfRows":  1000,
                "pageNo":     page,
            }, timeout=30)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)

            err_cd = root.findtext(".//errCd") or ""
            if err_cd and err_cd not in ("00", "0", ""):
                print(f"    API 오류({gu_name} p{page}): {err_cd} {root.findtext('.//errMsg') or ''}")
                break

            items = root.findall(".//item")
            if not items:
                break
            for item in items:
                code = _v(item, "kaptCode") or _v(item, "kaptcode")
                name = _v(item, "kaptName") or _v(item, "kaptname")
                if code and name:
                    result.append((code, name))
            if len(items) < 1000:
                break
            page += 1
            time.sleep(0.2)
        except Exception as e:
            print(f"    목록 조회 실패({gu_name} p{page}): {e}")
            break
    return result


def fetch_apt_info(kapt_code):
    """단지코드로 기본정보 조회."""
    try:
        resp = requests.get(INFO_URL, params={
            "serviceKey": API_KEY,
            "kaptCode":   kapt_code,
        }, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        item = root.find(".//item")
        if item is None:
            return None

        use_date   = _v(item, "kaptUsedate")
        build_year = use_date[:4] if len(use_date) >= 4 else ""

        return {
            "kapt_code":   kapt_code,
            "build_year":  build_year,
            "use_date":    use_date,
            "heat":        _v(item, "codeHeatNm"),
            "dong_cnt":    _v(item, "kaptDongCnt"),
            "household":   _v(item, "kaptdaCnt"),
            "constructor": _v(item, "kaptBcompany"),
            "developer":   _v(item, "kaptAcompany"),
            "mgr_type":    _v(item, "codeMgrNm"),
            "hall_type":   _v(item, "codeHallNm"),
            "sale_type":   _v(item, "codeSaleNm"),
            "apt_type":    _v(item, "codeAptNm"),
            "tel":         _v(item, "kaptTel"),
            "url":         _v(item, "kaptUrl"),
            "road_addr":   _v(item, "doroJuso"),
            "area_60":     _v(item, "kaptMparea_60"),
            "area_85":     _v(item, "kaptMparea_85"),
            "area_135":    _v(item, "kaptMparea_135"),
            "area_136":    _v(item, "kaptMparea_136"),
        }
    except Exception as e:
        print(f"    정보 조회 실패({kapt_code}): {e}")
        return None


def main():
    now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
    print(f"=== 대구 아파트 단지 기본정보 수집 ({now_kst.strftime('%Y-%m-%d %H:%M KST')}) ===")

    # 1단계: 단지 목록 수집
    print("\n[1단계] 단지 목록 수집...")
    all_apts = {}   # kaptCode -> kaptName (중복 제거)
    for gu_name, bjd_code in DISTRICTS:
        items = fetch_apt_list(bjd_code, gu_name)
        added = 0
        for code, name in items:
            if code not in all_apts:
                all_apts[code] = name
                added += 1
        print(f"  {gu_name}: {added}개 단지")
        time.sleep(0.3)

    total = len(all_apts)
    print(f"\n총 {total}개 단지 발견")

    # 2단계: 단지별 기본정보 수집
    print(f"\n[2단계] 단지 기본정보 수집 ({total}건)...")
    by_name = {}    # kaptName -> info dict (이름 기준 검색용)
    success = 0
    fail    = 0

    for i, (code, name) in enumerate(all_apts.items(), 1):
        if i % 100 == 0:
            print(f"  [{i}/{total}] 진행 중... (성공 {success} / 실패 {fail})")
        info = fetch_apt_info(code)
        if info:
            by_name[name] = info
            success += 1
        else:
            fail += 1
        time.sleep(0.15)

    print(f"\n완료: 성공 {success}건, 실패 {fail}건")

    payload = {
        "generated_at": now_kst.isoformat(),
        "total":        len(by_name),
        "by_name":      by_name,
    }

    os.makedirs(DATA, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    kb = os.path.getsize(OUTPUT) / 1024
    print(f"[완료] 저장: {OUTPUT} ({kb:.0f} KB)")


if __name__ == "__main__":
    main()
