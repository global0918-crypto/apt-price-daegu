"""마스터 xlsx → data/history.json 변환 (단지별 7년치 거래 이력)"""
import openpyxl, json, os

BASE   = os.path.dirname(os.path.abspath(__file__))
MASTER = os.path.join(BASE, "data", "아파트실거래_마스터.xlsx")
OUTPUT = os.path.join(BASE, "data", "history.json")


def build(master_path=MASTER, output_path=OUTPUT):
    if not os.path.exists(master_path):
        print(f"  [history] 마스터 파일 없음, 스킵: {master_path}")
        return

    print("  [history] 마스터 xlsx 로드 중...")
    wb = openpyxl.load_workbook(master_path, read_only=True, data_only=True)
    ws = wb["대구"]

    history = {}
    count = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not any(row):
            continue
        try:
            # 컬럼 순서: 시도(0) 구군(1) 법정동(2) 아파트명(3) 전용면적(4) 층(5)
            #            건축년도(6) 계약년(7) 계약월(8) 계약일(9) 거래금액(10)
            apt   = str(row[3] or "").strip()
            if not apt:
                continue
            area  = round(float(str(row[4] or 0).strip() or 0))
            floor = int(float(str(row[5] or 0).strip() or 0))
            year  = str(row[7] or "").strip()
            month = str(row[8] or "").strip().zfill(2)
            day   = str(row[9] or "").strip().zfill(2)
            price = int(str(row[10] or 0).replace(",", "").strip() or 0)

            if not year or price <= 0:
                continue

            date = f"{year}-{month}-{day}"
            if apt not in history:
                history[apt] = []
            history[apt].append([date, area, floor, price])
            count += 1
        except Exception:
            continue

    wb.close()

    for trades in history.values():
        trades.sort(key=lambda x: x[0], reverse=True)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, separators=(",", ":"))

    kb = os.path.getsize(output_path) / 1024
    print(f"  [history] 완료: {len(history)}개 단지 {count}건 → {kb:.0f} KB")


if __name__ == "__main__":
    build()
