#!/usr/bin/env python3
"""
대구 아파트 전월세 히스토리 수집 (최근 2년) → data/rent_history.json 저장.
최초 1회 수동 실행. 이후 매일 fetch_daegu_rent.py가 당일 신고분을 갱신.
"""
import json
import os
import sys
import time
from datetime import date
from dateutil.relativedelta import relativedelta

from fetch_daegu_rent import fetch_rent_month, DISTRICTS, DATA

OUTPUT = os.path.join(DATA, "rent_history.json")


def month_range(years=2):
    today = date.today()
    start = (today - relativedelta(years=years)).replace(day=1)
    months = []
    cur = start
    while cur <= today:
        months.append(cur.strftime("%Y%m"))
        cur += relativedelta(months=1)
    return months


def main():
    months = month_range(years=2)
    print(f"=== 대구 전월세 히스토리 수집 ===")
    print(f"기간: {months[0]} ~ {months[-1]} ({len(months)}개월, 9개 자치구)\n")

    all_jeonse = []
    all_wolse  = []
    total      = len(months)

    for idx, ym in enumerate(months, 1):
        month_j = 0
        month_w = 0
        print(f"[{idx:02d}/{total}] {ym}", end="")

        for name, code in DISTRICTS:
            items = fetch_rent_month(code, ym, name)
            for item in items:
                if item["trade_type"] == "jeonse":
                    all_jeonse.append(item)
                    month_j += 1
                else:
                    all_wolse.append(item)
                    month_w += 1
            time.sleep(0.2)

        print(f" → 전세 {month_j}건, 월세 {month_w}건")

    print(f"\n=== 완료 ===")
    print(f"전세 총 {len(all_jeonse)}건, 월세 총 {len(all_wolse)}건")

    payload = {
        "generated_at": date.today().isoformat(),
        "range":        f"{months[0]} ~ {months[-1]}",
        "total": {
            "jeonse": len(all_jeonse),
            "wolse":  len(all_wolse),
        },
        "items": {
            "jeonse": all_jeonse,
            "wolse":  all_wolse,
        },
    }

    os.makedirs(DATA, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    kb = os.path.getsize(OUTPUT) / 1024
    print(f"✅ 저장 완료: {OUTPUT} ({kb:.0f} KB)")


if __name__ == "__main__":
    main()
