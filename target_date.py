"""오늘 기준 국토부 신고 데이터 타겟일 계산"""
from datetime import datetime, timedelta


def compute_actual_report_date(transactions, max_fallback=5):
    """
    오늘부터 소급해 신고 데이터가 존재하는 가장 최근 영업일을 반환.
    최대 max_fallback 영업일까지 시도, 모두 실패하면 데이터 내 최신 신고일 반환.
    """
    rgst_dates = {t["rgst_date"] for t in transactions if t.get("rgst_date")}
    if not rgst_dates:
        return None

    today = datetime.now().date()
    tried, cur = 0, today

    while tried < max_fallback:
        cur -= timedelta(days=1)
        if cur.weekday() >= 5:      # 토(5) / 일(6) 건너뜀
            continue
        tried += 1
        candidate = cur.strftime("%Y-%m-%d")
        if candidate in rgst_dates:
            print(f"  타겟일 확정: {candidate} (영업일 {tried}회 소급)")
            return candidate

    fallback = max(rgst_dates)
    print(f"  {max_fallback}영업일 소급 후 데이터 없음 → 최신 신고일 fallback: {fallback}")
    return fallback
