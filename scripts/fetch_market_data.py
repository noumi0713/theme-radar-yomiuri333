from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
SOURCE_CSV = ROOT / "data" / "読売333_相対値動きテーマペア.csv"
OUTPUT_JSON = ROOT / "app" / "data" / "market-data.json"
MAX_WORKERS = 12
JST = ZoneInfo("Asia/Tokyo")
SESSION_CUTOFFS = (
    ("後場引値", 15 * 60 + 30),
    ("前場引値", 11 * 60 + 30),
)


def mean(values: list[float | None]) -> float | None:
    clean = [value for value in values if value is not None and math.isfinite(value)]
    return statistics.fmean(clean) if clean else None


def change(values: list[float], lookback: int) -> float | None:
    if len(values) < 2:
        return None
    start_index = max(0, len(values) - 1 - lookback)
    start = values[start_index]
    return values[-1] / start - 1 if start else None


def fetch_json(url: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ThemeRadar/1.0)",
            "Accept": "application/json",
        },
    )

    last_error = "unknown"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as error:
            last_error = str(error)
            if attempt < 2:
                time.sleep(0.8 * (attempt + 1))
    raise urllib.error.URLError(last_error)


def fetch_latest_session(ticker: str) -> dict | None:
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        "?range=5d&interval=5m&includePrePost=false"
    )
    payload = fetch_json(url)
    result = payload["chart"]["result"][0]
    timestamps = result.get("timestamp") or []
    closes = (
        ((result.get("indicators", {}).get("quote") or [{}])[0].get("close"))
        or []
    )
    points: list[tuple[datetime, float]] = []
    for index, timestamp in enumerate(timestamps):
        close = closes[index] if index < len(closes) else None
        if close is None or not math.isfinite(float(close)) or float(close) <= 0:
            continue
        local_time = datetime.fromtimestamp(timestamp, timezone.utc).astimezone(JST)
        points.append((local_time, float(close)))

    dates = sorted({point[0].date() for point in points}, reverse=True)
    for trading_date in dates:
        day_points = sorted(
            (point for point in points if point[0].date() == trading_date),
            key=lambda point: point[0],
        )
        if not day_points:
            continue
        observed_minute = max(point[0].hour * 60 + point[0].minute for point in day_points)
        for label, cutoff_minute in SESSION_CUTOFFS:
            if observed_minute < cutoff_minute:
                continue
            eligible = [
                point
                for point in day_points
                if point[0].hour * 60 + point[0].minute <= cutoff_minute
            ]
            if not eligible:
                continue
            session_time, session_price = eligible[-1]
            session_minute = session_time.hour * 60 + session_time.minute
            if cutoff_minute - session_minute > 10:
                continue
            return {
                "date": trading_date.isoformat(),
                "label": label,
                "capturedAt": session_time.isoformat(),
                "price": session_price,
            }
    return None


def fetch_stock(row: dict[str, str]) -> dict:
    code = row["コード"].strip()
    ticker = f"{code}.T"
    daily_url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        "?range=1y&interval=1d&events=div%2Csplits"
    )

    last_error = "unknown"
    for attempt in range(3):
        try:
            payload = fetch_json(daily_url)
            result = payload["chart"]["result"][0]
            timestamps = result.get("timestamp") or []
            indicators = result.get("indicators", {})
            adjusted = ((indicators.get("adjclose") or [{}])[0].get("adjclose") or [])
            regular = ((indicators.get("quote") or [{}])[0].get("close") or [])

            points: list[tuple[str, float]] = []
            adjustment_factor = 1.0
            regular_last: float | None = None
            for index, timestamp in enumerate(timestamps):
                adjusted_price = adjusted[index] if index < len(adjusted) else None
                regular_price = regular[index] if index < len(regular) else None
                price = adjusted_price if adjusted_price is not None else regular_price
                if price is None or not math.isfinite(float(price)) or float(price) <= 0:
                    continue
                date = datetime.fromtimestamp(timestamp, timezone.utc).date().isoformat()
                points.append((date, float(price)))
                if (
                    adjusted_price is not None
                    and regular_price is not None
                    and math.isfinite(float(adjusted_price))
                    and math.isfinite(float(regular_price))
                    and float(regular_price) > 0
                ):
                    adjustment_factor = float(adjusted_price) / float(regular_price)
                    regular_last = float(regular_price)

            if len(points) < 20:
                raise ValueError(f"insufficient price history ({len(points)} points)")

            session: dict | None = None
            session_error: str | None = None
            try:
                session = fetch_latest_session(ticker)
            except (urllib.error.URLError, urllib.error.HTTPError, KeyError, ValueError) as error:
                session_error = str(error)

            last_price = regular_last if regular_last is not None else points[-1][1]
            session_date: str | None = None
            session_label: str | None = None
            session_captured_at: str | None = None
            if session and session["date"] >= points[-1][0]:
                session_date = session["date"]
                session_label = session["label"]
                session_captured_at = session["capturedAt"]
                last_price = session["price"]
                adjusted_session_price = session["price"] * adjustment_factor
                if session_date == points[-1][0]:
                    points[-1] = (session_date, adjusted_session_price)
                else:
                    points.append((session_date, adjusted_session_price))

            prices = [price for _, price in points]
            sma50 = mean(prices[-50:])
            sma200 = mean(prices[-200:])
            daily_returns = [
                prices[index] / prices[index - 1] - 1
                for index in range(1, len(prices))
                if prices[index - 1]
            ]
            volatility = (
                statistics.stdev(daily_returns) * math.sqrt(252)
                if len(daily_returns) >= 2
                else None
            )

            return {
                "ok": True,
                "group": row["相対グループ"].strip(),
                "side": row["側"].strip(),
                "theme": row["テーマ"].strip(),
                "code": code,
                "ticker": ticker,
                "name": row["銘柄名"].strip(),
                "lastDate": points[-1][0],
                "lastPrice": last_price,
                "sessionDate": session_date,
                "sessionLabel": session_label,
                "sessionCapturedAt": session_captured_at,
                "sessionError": session_error,
                "return1y": change(prices, len(prices) - 1),
                "return3m": change(prices, 63),
                "return1m": change(prices, 21),
                "return2w": change(prices, 10),
                "return5d": change(prices, 5),
                "above20": bool(mean(prices[-20:]) and prices[-1] > mean(prices[-20:])),
                "above50": bool(sma50 and prices[-1] > sma50),
                "above200": bool(sma200 and prices[-1] > sma200),
                "volatility": volatility,
                "series": points,
            }
        except (urllib.error.URLError, urllib.error.HTTPError, KeyError, ValueError) as error:
            last_error = str(error)
            if attempt < 2:
                time.sleep(0.8 * (attempt + 1))

    return {
        "ok": False,
        "group": row["相対グループ"].strip(),
        "side": row["側"].strip(),
        "theme": row["テーマ"].strip(),
        "code": code,
        "ticker": ticker,
        "name": row["銘柄名"].strip(),
        "error": last_error,
    }


def rounded(value: float | None, digits: int = 6) -> float | None:
    return round(value, digits) if value is not None else None


def sample_series(stocks: list[dict], max_points: int = 38) -> list[dict]:
    normalized_by_date: dict[str, list[float]] = defaultdict(list)
    for stock in stocks:
        series = stock["series"]
        if not series:
            continue
        base = series[0][1]
        if not base:
            continue
        for date, price in series:
            normalized_by_date[date].append(price / base - 1)

    dates = sorted(normalized_by_date)
    if not dates:
        return []
    step = max(1, math.ceil(len(dates) / max_points))
    sampled_dates = dates[::step]
    if sampled_dates[-1] != dates[-1]:
        sampled_dates.append(dates[-1])
    return [
        {"date": date, "value": rounded(mean(normalized_by_date[date]), 5)}
        for date in sampled_dates
    ]


def aggregate_index(stocks: list[dict]) -> dict[str, float]:
    """Create an equal-weight normalized price index for a stock basket."""
    normalized_by_date: dict[str, list[float]] = defaultdict(list)
    for stock in stocks:
        series = stock["series"]
        if not series:
            continue
        base = series[0][1]
        if not base:
            continue
        for date, price in series:
            normalized_by_date[date].append(price / base)
    return {
        date: statistics.fmean(values)
        for date, values in normalized_by_date.items()
        if values
    }


def series_change(
    levels: dict[str, float], common_dates: list[str], date_index: int, lookback: int
) -> float | None:
    if date_index < lookback:
        return None
    current_date = common_dates[date_index]
    prior_date = common_dates[date_index - lookback]
    current = levels.get(current_date)
    prior = levels.get(prior_date)
    if current is None or prior in (None, 0):
        return None
    return current / prior - 1


def series_rsi(
    levels: dict[str, float], common_dates: list[str], date_index: int, period: int = 14
) -> float | None:
    if date_index < period:
        return None
    values = [levels.get(common_dates[index]) for index in range(date_index - period, date_index + 1)]
    if any(value is None for value in values):
        return None
    changes = [
        float(values[index]) / float(values[index - 1]) - 1
        for index in range(1, len(values))
        if values[index - 1]
    ]
    gains = [max(value, 0.0) for value in changes]
    losses = [max(-value, 0.0) for value in changes]
    average_gain = statistics.fmean(gains)
    average_loss = statistics.fmean(losses)
    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    relative_strength = average_gain / average_loss
    return 100.0 - (100.0 / (1.0 + relative_strength))


def breadth_at_date(
    stocks: list[dict], date: str, lookback: int, mode: str = "return"
) -> float | None:
    results: list[float] = []
    for stock in stocks:
        series = stock["series"]
        positions = {point_date: index for index, (point_date, _) in enumerate(series)}
        position = positions.get(date)
        if position is None:
            continue
        price = series[position][1]
        if mode == "return":
            if position < lookback:
                continue
            prior = series[position - lookback][1]
            if prior:
                results.append(1.0 if price > prior else 0.0)
        elif mode == "sma":
            if position + 1 < lookback:
                continue
            moving_average = statistics.fmean(
                value for _, value in series[position + 1 - lookback : position + 1]
            )
            results.append(1.0 if price > moving_average else 0.0)
    return statistics.fmean(results) if results else None


def percentile_ranks(values: dict[str, float | None]) -> dict[str, float]:
    clean = sorted(
        ((name, value) for name, value in values.items() if value is not None),
        key=lambda item: item[1],
    )
    if not clean:
        return {}
    if len(clean) == 1:
        return {clean[0][0]: 0.5}
    return {
        name: index / (len(clean) - 1)
        for index, (name, _) in enumerate(clean)
    }


def weekly_scores(
    theme_names: list[str],
    stocks_by_theme: dict[str, list[dict]],
    theme_levels: dict[str, dict[str, float]],
    side_levels: dict[tuple[str, str], dict[str, float]],
    theme_identity: dict[str, tuple[str, str]],
    common_dates: list[str],
    date_index: int,
) -> dict[str, dict]:
    raw: dict[str, dict] = {}
    for theme_name in theme_names:
        group, side = theme_identity[theme_name]
        opposing_side = "B" if side == "A" else "A"
        levels = theme_levels[theme_name]
        opposing_levels = side_levels[(group, opposing_side)]
        return5d = series_change(levels, common_dates, date_index, 5)
        return20d = series_change(levels, common_dates, date_index, 20)
        return60d = series_change(levels, common_dates, date_index, 60)
        opponent5d = series_change(opposing_levels, common_dates, date_index, 5)
        relative5d = (
            return5d - opponent5d
            if return5d is not None and opponent5d is not None
            else None
        )
        acceleration = (
            return5d - return20d / 4
            if return5d is not None and return20d is not None
            else None
        )
        current_date = common_dates[date_index]
        raw[theme_name] = {
            "return5d": return5d,
            "return20d": return20d,
            "return60d": return60d,
            "relative5d": relative5d,
            "acceleration": acceleration,
            "breadth5d": breadth_at_date(
                stocks_by_theme[theme_name], current_date, 5, "return"
            ),
            "breadth20d": breadth_at_date(
                stocks_by_theme[theme_name], current_date, 20, "sma"
            ),
            "rsi14": series_rsi(levels, common_dates, date_index, 14),
        }

    ranking_fields = [
        "return5d",
        "return20d",
        "relative5d",
        "acceleration",
        "breadth5d",
        "breadth20d",
        "return60d",
    ]
    ranks = {
        field: percentile_ranks(
            {theme_name: raw[theme_name][field] for theme_name in theme_names}
        )
        for field in ranking_fields
    }
    weights = {
        "return5d": 0.22,
        "return20d": 0.18,
        "relative5d": 0.20,
        "acceleration": 0.10,
        "breadth5d": 0.15,
        "breadth20d": 0.10,
        "return60d": 0.05,
    }

    scored: dict[str, dict] = {}
    for theme_name in theme_names:
        score = sum(
            ranks[field].get(theme_name, 0.5) * weight
            for field, weight in weights.items()
        ) * 100
        rsi14 = raw[theme_name]["rsi14"]
        return5d = raw[theme_name]["return5d"]
        acceleration = raw[theme_name]["acceleration"]
        if rsi14 is not None and rsi14 >= 78 and (acceleration or 0) < 0:
            score -= 8
        if return5d is not None and return5d >= 0.12:
            score -= 6
        score = max(0.0, min(100.0, score))

        relative5d = raw[theme_name]["relative5d"]
        breadth5d = raw[theme_name]["breadth5d"]
        breadth20d = raw[theme_name]["breadth20d"]
        weekly_signal = bool(
            score >= 75
            and (return5d or 0) > 0
            and (relative5d or 0) > 0
            and (breadth5d or 0) >= 0.55
            and (breadth20d or 0) >= 0.55
        )
        if weekly_signal and score >= 80:
            weekly_label = "strong"
        elif weekly_signal:
            weekly_label = "bullish"
        elif score >= 58:
            weekly_label = "watch"
        elif score < 35:
            weekly_label = "weak"
        else:
            weekly_label = "neutral"

        reasons: list[str] = []
        if ranks["relative5d"].get(theme_name, 0) >= 0.75:
            reasons.append("5日相対モメンタムが上位25%")
        if breadth5d is not None and breadth5d >= 0.65:
            reasons.append(f"5日上昇銘柄が{breadth5d:.0%}")
        if breadth20d is not None and breadth20d >= 0.65:
            reasons.append(f"20日線超えが{breadth20d:.0%}")
        if acceleration is not None and acceleration > 0:
            reasons.append("短期モメンタムが加速")
        if rsi14 is not None and rsi14 >= 78:
            reasons.append("過熱圏のため追随注意")
        if not reasons:
            reasons.append("短期指標は拮抗")

        scored[theme_name] = {
            **raw[theme_name],
            "weeklyScore": score,
            "weeklySignal": weekly_signal,
            "weeklyLabel": weekly_label,
            "weeklyReasons": reasons[:3],
        }
    return scored


def backtest_weekly_model(
    theme_names: list[str],
    stocks_by_theme: dict[str, list[dict]],
    theme_levels: dict[str, dict[str, float]],
    side_levels: dict[tuple[str, str], dict[str, float]],
    theme_identity: dict[str, tuple[str, str]],
    common_dates: list[str],
) -> dict:
    signal_returns: list[float] = []
    all_returns: list[float] = []
    start_index = 65
    for date_index in range(start_index, len(common_dates) - 5, 5):
        scored = weekly_scores(
            theme_names,
            stocks_by_theme,
            theme_levels,
            side_levels,
            theme_identity,
            common_dates,
            date_index,
        )
        for theme_name in theme_names:
            levels = theme_levels[theme_name]
            current = levels.get(common_dates[date_index])
            future = levels.get(common_dates[date_index + 5])
            if current in (None, 0) or future is None:
                continue
            forward_return = future / current - 1
            all_returns.append(forward_return)
            if scored[theme_name]["weeklySignal"]:
                signal_returns.append(forward_return)

    return {
        "periodStart": common_dates[start_index] if len(common_dates) > start_index else None,
        "periodEnd": common_dates[-1] if common_dates else None,
        "signalSamples": len(signal_returns),
        "hitRate": rounded(
            mean([1.0 if value > 0 else 0.0 for value in signal_returns])
        ),
        "averageForward5d": rounded(mean(signal_returns)),
        "baselineSamples": len(all_returns),
        "baselineHitRate": rounded(
            mean([1.0 if value > 0 else 0.0 for value in all_returns])
        ),
        "baselineAverageForward5d": rounded(mean(all_returns)),
        "note": "現在のテーマ構成を過去に固定した簡易ウォークフォワード検証",
    }


def signal_for(theme: dict) -> tuple[str, int, list[str]]:
    score = 0
    reasons: list[str] = []
    relative3m = theme["relative3m"]
    relative1y = theme["relative1y"]
    return1m = theme["return1m"]
    breadth50 = theme["breadth50"]

    if relative3m is not None:
        if relative3m >= 0.05:
            score += 2
            reasons.append("3カ月の相対差が+5pt以上")
        elif relative3m >= 0.02:
            score += 1
            reasons.append("3カ月の相対差がプラス")
        elif relative3m <= -0.05:
            score -= 2
            reasons.append("3カ月の相対差が-5pt以下")
        elif relative3m <= -0.02:
            score -= 1
            reasons.append("3カ月の相対差がマイナス")

    if return1m is not None:
        if return1m >= 0.02:
            score += 1
            reasons.append("直近1カ月が上向き")
        elif return1m <= -0.02:
            score -= 1
            reasons.append("直近1カ月が下向き")

    if breadth50 is not None:
        if breadth50 >= 0.60:
            score += 1
            reasons.append("50日線超えが60%以上")
        elif breadth50 <= 0.40:
            score -= 1
            reasons.append("50日線超えが40%以下")

    if relative1y is not None:
        if relative1y > 0:
            score += 1
        elif relative1y < 0:
            score -= 1

    if score >= 3:
        return "buy", score, reasons[:3]
    if score <= -2:
        return "sell", score, reasons[:3]
    return "neutral", score, reasons[:3] or ["相対差とモメンタムが拮抗"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="読売333の終値を取得し、テーマ別シグナルを生成します。"
    )
    parser.add_argument("--source", type=Path, default=SOURCE_CSV)
    parser.add_argument("--output", type=Path, default=OUTPUT_JSON)
    parser.add_argument(
        "--skip-unchanged",
        action="store_true",
        help="基準日と前場・後場区分が同じ場合はファイルを書き換えません。",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_csv = args.source.resolve()
    output_json = args.output.resolve()
    with source_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    print(f"Fetching {len(rows)} symbols...")
    fetched: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(fetch_stock, row) for row in rows]
        for index, future in enumerate(as_completed(futures), 1):
            fetched.append(future.result())
            if index % 25 == 0 or index == len(futures):
                print(f"  {index}/{len(futures)}")

    successful = [stock for stock in fetched if stock["ok"]]
    failed = [stock for stock in fetched if not stock["ok"]]
    stocks_by_theme: dict[str, list[dict]] = defaultdict(list)
    stocks_by_group_side: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for stock in successful:
        stocks_by_theme[stock["theme"]].append(stock)
        stocks_by_group_side[(stock["group"], stock["side"])].append(stock)

    theme_names = sorted(stocks_by_theme)
    theme_identity = {
        theme_name: (
            stocks_by_theme[theme_name][0]["group"],
            stocks_by_theme[theme_name][0]["side"],
        )
        for theme_name in theme_names
    }
    theme_levels = {
        theme_name: aggregate_index(stocks_by_theme[theme_name])
        for theme_name in theme_names
    }
    side_levels = {
        group_side: aggregate_index(stocks)
        for group_side, stocks in stocks_by_group_side.items()
    }
    common_date_sets = [set(levels) for levels in theme_levels.values()]
    common_dates = sorted(set.intersection(*common_date_sets))
    latest_weekly = weekly_scores(
        theme_names,
        stocks_by_theme,
        theme_levels,
        side_levels,
        theme_identity,
        common_dates,
        len(common_dates) - 1,
    )
    weekly_ranking = {
        theme_name: rank
        for rank, theme_name in enumerate(
            sorted(
                theme_names,
                key=lambda name: latest_weekly[name]["weeklyScore"],
                reverse=True,
            ),
            1,
        )
    }
    weekly_backtest = backtest_weekly_model(
        theme_names,
        stocks_by_theme,
        theme_levels,
        side_levels,
        theme_identity,
        common_dates,
    )

    theme_rows: list[dict] = []
    for theme_name, stocks in stocks_by_theme.items():
        first = stocks[0]
        opposing_side = "B" if first["side"] == "A" else "A"
        opposing = stocks_by_group_side[(first["group"], opposing_side)]
        theme = {
            "id": f"{first['group']}-{first['side']}-{len(theme_rows) + 1}",
            "group": first["group"],
            "side": first["side"],
            "name": theme_name,
            "constituents": len(stocks),
            "sourceConstituents": sum(1 for row in rows if row["テーマ"].strip() == theme_name),
            "return1y": rounded(mean([stock["return1y"] for stock in stocks])),
            "return3m": rounded(mean([stock["return3m"] for stock in stocks])),
            "return1m": rounded(mean([stock["return1m"] for stock in stocks])),
            "return2w": rounded(mean([stock["return2w"] for stock in stocks])),
            "return5d": rounded(latest_weekly[theme_name]["return5d"]),
            "opponentReturn1y": rounded(mean([stock["return1y"] for stock in opposing])),
            "opponentReturn3m": rounded(mean([stock["return3m"] for stock in opposing])),
            "relative5d": rounded(latest_weekly[theme_name]["relative5d"]),
            "breadth5d": rounded(latest_weekly[theme_name]["breadth5d"]),
            "breadth20d": rounded(latest_weekly[theme_name]["breadth20d"]),
            "breadth50": rounded(mean([1.0 if stock["above50"] else 0.0 for stock in stocks])),
            "breadth200": rounded(mean([1.0 if stock["above200"] else 0.0 for stock in stocks])),
            "rsi14": rounded(latest_weekly[theme_name]["rsi14"], 2),
            "volatility": rounded(mean([stock["volatility"] for stock in stocks])),
            "spark": sample_series(stocks),
            "weeklyScore": rounded(latest_weekly[theme_name]["weeklyScore"], 1),
            "weeklyRank": weekly_ranking[theme_name],
            "weeklySignal": latest_weekly[theme_name]["weeklySignal"],
            "weeklyLabel": latest_weekly[theme_name]["weeklyLabel"],
            "weeklyReasons": latest_weekly[theme_name]["weeklyReasons"],
        }
        theme["relative1y"] = rounded(
            theme["return1y"] - theme["opponentReturn1y"]
            if theme["return1y"] is not None and theme["opponentReturn1y"] is not None
            else None
        )
        theme["relative3m"] = rounded(
            theme["return3m"] - theme["opponentReturn3m"]
            if theme["return3m"] is not None and theme["opponentReturn3m"] is not None
            else None
        )
        signal, score, reasons = signal_for(theme)
        theme["signal"] = signal
        theme["signalScore"] = score
        theme["reasons"] = reasons

        ranked = sorted(
            stocks,
            key=lambda stock: stock["return1y"] if stock["return1y"] is not None else -999,
            reverse=True,
        )
        theme["leaders"] = [
            {
                "code": stock["code"],
                "name": stock["name"],
                "return1y": rounded(stock["return1y"]),
                "return3m": rounded(stock["return3m"]),
                "return5d": rounded(stock["return5d"]),
                "lastPrice": rounded(stock["lastPrice"], 2),
            }
            for stock in ranked[:3]
        ]
        theme["laggards"] = [
            {
                "code": stock["code"],
                "name": stock["name"],
                "return1y": rounded(stock["return1y"]),
                "return3m": rounded(stock["return3m"]),
                "return5d": rounded(stock["return5d"]),
                "lastPrice": rounded(stock["lastPrice"], 2),
            }
            for stock in reversed(ranked[-3:])
        ]
        swing_ranked = sorted(
            stocks,
            key=lambda stock: stock["return5d"]
            if stock["return5d"] is not None
            else -999,
            reverse=True,
        )
        theme["swingLeaders"] = [
            {
                "code": stock["code"],
                "name": stock["name"],
                "return1y": rounded(stock["return1y"]),
                "return3m": rounded(stock["return3m"]),
                "return5d": rounded(stock["return5d"]),
                "lastPrice": rounded(stock["lastPrice"], 2),
            }
            for stock in swing_ranked[:3]
        ]
        theme["swingLaggards"] = [
            {
                "code": stock["code"],
                "name": stock["name"],
                "return1y": rounded(stock["return1y"]),
                "return3m": rounded(stock["return3m"]),
                "return5d": rounded(stock["return5d"]),
                "lastPrice": rounded(stock["lastPrice"], 2),
            }
            for stock in reversed(swing_ranked[-3:])
        ]
        theme_rows.append(theme)

    group_number = lambda name: int("".join(character for character in name if character.isdigit()) or 0)
    theme_rows.sort(key=lambda theme: (group_number(theme["group"]), theme["side"], theme["name"]))

    groups: list[dict] = []
    for group_name in sorted({theme["group"] for theme in theme_rows}, key=group_number):
        group_themes = [theme for theme in theme_rows if theme["group"] == group_name]
        side_a = stocks_by_group_side[(group_name, "A")]
        side_b = stocks_by_group_side[(group_name, "B")]
        a_1y = mean([stock["return1y"] for stock in side_a])
        b_1y = mean([stock["return1y"] for stock in side_b])
        groups.append(
            {
                "id": group_name,
                "label": group_name.replace("相対グループ", "PAIR "),
                "aThemes": [theme["name"] for theme in group_themes if theme["side"] == "A"],
                "bThemes": [theme["name"] for theme in group_themes if theme["side"] == "B"],
                "aReturn1y": rounded(a_1y),
                "bReturn1y": rounded(b_1y),
                "spread1y": rounded(a_1y - b_1y if a_1y is not None and b_1y is not None else None),
            }
        )

    as_of = max(stock["lastDate"] for stock in successful)
    session_counts = Counter(
        (
            stock["sessionDate"],
            stock["sessionLabel"],
            stock["sessionCapturedAt"],
        )
        for stock in successful
        if stock.get("sessionDate")
        and stock.get("sessionLabel")
        and stock.get("sessionCapturedAt")
    )
    dominant_session = session_counts.most_common(1)[0] if session_counts else None
    if dominant_session:
        session_key, session_symbol_count = dominant_session
        session_date, market_session, session_captured_at = session_key
    else:
        session_symbol_count = 0
        session_date = as_of
        market_session = "日足終値"
        session_captured_at = f"{as_of}T15:30:00+09:00"
    payload = {
        "meta": {
            "asOf": as_of,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "marketSession": market_session,
            "sessionDate": session_date,
            "sessionCapturedAt": session_captured_at,
            "sessionSymbols": session_symbol_count,
            "sessionCoverage": rounded(
                session_symbol_count / len(successful) if successful else None
            ),
            "refreshPolicy": "東証営業日の前場引け後と後場引け後に最新の完了済みセッションを採用",
            "source": "Yahoo Finance chart endpoint",
            "sourceUrl": "https://finance.yahoo.co.jp/",
            "sourceRows": len(rows),
            "successfulSymbols": len(successful),
            "failedSymbols": len(failed),
            "themeCount": len(theme_rows),
            "groupCount": len(groups),
            "method": "各銘柄の調整後終値騰落率をテーマ内で単純平均",
            "weeklyMethod": "5日・20日モメンタム、5日相対差、上昇銘柄比率、20日線、加速度を横断順位化",
        },
        "themes": theme_rows,
        "groups": groups,
        "weeklyBacktest": weekly_backtest,
        "failed": [
            {
                "code": stock["code"],
                "name": stock["name"],
                "theme": stock["theme"],
                "error": stock["error"],
            }
            for stock in sorted(failed, key=lambda stock: stock["code"])
        ],
    }

    if args.skip_unchanged and output_json.exists():
        try:
            previous = json.loads(output_json.read_text(encoding="utf-8"))
            previous_meta = previous.get("meta", {})
            if (
                previous_meta.get("asOf") == payload["meta"]["asOf"]
                and previous_meta.get("marketSession") == payload["meta"]["marketSession"]
                and previous_meta.get("sessionCapturedAt")
                == payload["meta"]["sessionCapturedAt"]
            ):
                print(
                    "No completed session change: "
                    f"{payload['meta']['asOf']} {payload['meta']['marketSession']}"
                )
                return
        except (json.JSONDecodeError, OSError):
            pass

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(
        f"Wrote {output_json} "
        f"({len(successful)} successful, {len(failed)} failed, {len(theme_rows)} themes)"
    )


if __name__ == "__main__":
    main()
