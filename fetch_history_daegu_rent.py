#!/usr/bin/env python3
"""대구 아파트 전월세 히스토리 수집 (최근 7년) -> data/rent_history.json 저장.
최초 1회 수동 실행. 이후 CI daily-fetch.yml 이 당월 데이터를 갱신.
출력 형식:
  {by_apt: {apt_name: {jeonse: [[date,area,floor,deposit],...],
                        wolse:  [[date,area,floor,deposit,monthly_rent],...]}}}
"""
import json
import os
import sys
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo
from dateutil.relativedelta import relativedelta

from fetch_daegu_rent import fetch_rent_month, DISTRICTS, DATA

OUTPUT = os.path.join(DATA, "rent_history.json")


def month_range(years=7):
    today = date.today()
    start = (today - relativedelta(years=years)).replace(day=1)
    months = []
    cur = start
    while cur <= today:
        months.append(cur.strftime("%Y%m"))
        cur += relativedelta(months=1)
    return months


def main():
    months = month_range(years=7)
    total_calls = len(months) * len(DISTRICTS)
    print(f"=== 대구 전월세 히스토리 수집 (7년) ===")
    print(f"기간: {months[0]} ~ {months[-1]} ({len(months)}개월 x {len(DISTRICTS)}구군 = {total_calls}회 API 호출)")
    print(f"예상 소요: {total_calls * 0.25 / 60:.1f}분\n")

    by_apt = {}
    seen = set()
    total_j = 0
    total_w = 0

    for idx, ym in enumerate(months, 1):
        month_j = 0
        month_w = 0
        sys.stdout.write(f"[{idx:03d}/{len(months)}] {ym}")
        sys.stdout.flush()

        for name, code in DISTRICTS:
            items = fetch_rent_month(code, ym, name)
            for item in items:
                apt = item.get("apt_name", "")
                if not apt:
                    continue
                area  = round(float(item.get("area", 0)), 2)
                floor = int(item.get("floor", 0))
                dep   = int(item.get("deposit", 0))
                rent  = int(item.get("monthly_rent", 0))
                dt    = item.get("deal_date", "")
                kind  = item.get("trade_type", "")

                dedup_key = (apt, dt, round(area), floor, dep, rent)
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                apt_bucket = by_apt.setdefault(apt, {"jeonse": [], "wolse": []})
                if kind == "jeonse":
                    apt_bucket["jeonse"].append([dt, area, floor, dep])
                    month_j += 1
                    total_j += 1
                else:
                    apt_bucket["wolse"].append([dt, area, floor, dep, rent])
                    month_w += 1
                    total_w += 1

            time.sleep(0.2)

        print(f"  ->  전세 {month_j}건  월세 {month_w}건")

    print(f"\n=== 완료 ===")
    print(f"아파트 종류: {len(by_apt)}개")
    print(f"전세 총 {total_j}건, 월세 총 {total_w}건")

    now_kst = datetime.now(ZoneInfo("Asia/Seoul")).isoformat()
    payload = {
        "generated_at": now_kst,
        "range":        f"{months[0]} ~ {months[-1]}",
        "total": {
            "apts":   len(by_apt),
            "jeonse": total_j,
            "wolse":  total_w,
        },
        "by_apt": by_apt,
    }

    os.makedirs(DATA, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    kb = os.path.getsize(OUTPUT) / 1024
    print(f"[완료] 저장: {OUTPUT} ({kb:.0f} KB)")


if __name__ == "__main__":
    main()
