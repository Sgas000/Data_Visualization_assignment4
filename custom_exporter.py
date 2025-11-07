import time
import threading
import requests
from datetime import datetime, timezone
from typing import Optional, Tuple, Any, Dict, List
from prometheus_client import start_http_server, Gauge, Counter, Summary

# -------- настройки --------
SCRAPE_INTERVAL = 20
LAT, LON = 51.1694, 71.4491  # Astana (Asia/Almaty)

# -------- метрики --------
# Погода
weather_temperature_c = Gauge("weather_temperature_c", "Air temperature at 2m (C)")
weather_wind_speed_ms = Gauge("weather_wind_speed_ms", "Wind speed at 10m (m/s)")
weather_humidity_percent = Gauge("weather_humidity_percent", "Relative humidity (%)")

# Воздух
air_pm10_ugm3 = Gauge("air_pm10_ugm3", "PM10 ug/m3")
air_pm25_ugm3 = Gauge("air_pm25_ugm3", "PM2.5 ug/m3")
air_us_aqi = Gauge("air_us_aqi", "US AQI")

# Валюты
fx_usd_kzt = Gauge("fx_usd_kzt", "USD/KZT")
fx_eur_kzt = Gauge("fx_eur_kzt", "EUR/KZT")

# Крипта
crypto_btc_usd = Gauge("crypto_btc_usd", "BTC/USD")
crypto_eth_usd = Gauge("crypto_eth_usd", "ETH/USD")

# GitHub (метрики определены — при желании можно дописать сбор)
github_repo_stars = Gauge("github_repo_stars", "GitHub repo stargazers count", ["repo"])
github_repo_open_issues = Gauge("github_repo_open_issues", "GitHub repo open issues", ["repo"])

# Служебные
api_success_total = Counter("api_success_total", "Successful API scrapes", ["api"])
api_fail_total = Counter("api_fail_total", "Failed API scrapes", ["api"])
api_latency_seconds = Summary("api_latency_seconds", "Latency for API calls in seconds", ["api"])
api_last_scrape_timestamp_seconds = Gauge("api_last_scrape_timestamp_seconds", "Last scrape unix time", ["api"])

# Прочее
session = requests.Session()
session.headers.update({"User-Agent": "custom-exporter/1.1"})

# StackOverflow (раз в 10 минут)
LAST_SO_POLL = 0
SO_MIN_PERIOD = 600
so_tag_count = Gauge("so_tag_count", "StackOverflow tag usage count", ["tag"])


# -------- утилиты --------
def log_info(msg: str):
    print(f"[INFO] {time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def log_warn(msg: str):
    print(f"[WARN] {time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def timed_get(api_name: str, url: str, **kwargs) -> Tuple[Optional[Dict[str, Any]], float, Optional[Exception]]:
    start = time.time()
    try:
        r = session.get(url, timeout=10, **kwargs)
        r.raise_for_status()
        data = r.json()
        api_success_total.labels(api_name).inc()
        return data, time.time() - start, None
    except Exception as e:
        api_fail_total.labels(api_name).inc()
        return None, time.time() - start, e


def _parse_iso_ts(ts: str) -> Optional[datetime]:
    """
    Пытаемся распарсить ISO-строку времени Open-Meteo (может быть без 'Z', с оффсетом или без него).
    Возвращаем aware datetime в UTC.
    """
    try:
        # Python 3.11: datetime.fromisoformat понимает 'YYYY-MM-DDTHH:MM' и с оффсетом
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            # считаем, что это локальное по timezone=auto; но для сравнения возьмём как naive->UTC
            # безопаснее трактовать как UTC, чтобы выбирать "прошедшее" относительно текущего UTC
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def _last_past_value(hourly: Dict[str, List[Any]], key: str) -> Optional[float]:
    """
    Берем из hourly массивов Open-Meteo значение 'key' с таймстампом, который
    наибольший, но не превосходит "сейчас" (UTC). Если ничего не нашли — None.
    """
    times = hourly.get("time")
    values = hourly.get(key)
    if not isinstance(times, list) or not isinstance(values, list) or len(times) != len(values) or not times:
        return None

    now_utc = datetime.now(timezone.utc)

    # идём с конца, ищем первое прошедшее
    for i in range(len(times) - 1, -1, -1):
        dt = _parse_iso_ts(times[i])
        if dt is None:
            continue
        if dt <= now_utc:
            v = values[i]
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    return None
    return None


# -------- сборщики --------
def poll_openmeteo_weather():
    api = "openmeteo_weather"
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        "&current=temperature_2m,wind_speed_10m,relative_humidity_2m"
        "&timezone=auto"
    )
    data, latency, err = timed_get(api, url)
    api_latency_seconds.labels(api).observe(latency)
    api_last_scrape_timestamp_seconds.labels(api).set(time.time())
    if err or not data or "current" not in data:
        if err:
            log_warn(f"{api}: {err}")
        return

    c = data["current"]
    try:
        if "temperature_2m" in c:
            weather_temperature_c.set(float(c["temperature_2m"]))
        if "wind_speed_10m" in c:
            # open-meteo в km/h, переводим в m/s
            weather_wind_speed_ms.set(float(c["wind_speed_10m"]) / 3.6)
        if "relative_humidity_2m" in c:
            weather_humidity_percent.set(float(c["relative_humidity_2m"]))
    except Exception as e:
        log_warn(f"{api}: parse current failed: {e}")


def poll_openmeteo_air():
    """
    1) Сначала пробуем current=pm10,pm2_5,us_aqi (если у API доступно).
    2) Если нет current — берём hourly и выбираем ближайшее прошедшее значение (не будущее!).
    """
    api = "openmeteo_air"

    # --- попытка current ---
    url_current = (
        "https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={LAT}&longitude={LON}"
        "&current=pm10,pm2_5,us_aqi&timezone=auto"
    )
    data, latency, err = timed_get(api, url_current)
    api_latency_seconds.labels(api).observe(latency)
    api_last_scrape_timestamp_seconds.labels(api).set(time.time())

    if not err and data and "current" in data:
        c = data["current"]
        try:
            if "pm10" in c and c["pm10"] is not None:
                air_pm10_ugm3.set(float(c["pm10"]))
            if "pm2_5" in c and c["pm2_5"] is not None:
                air_pm25_ugm3.set(float(c["pm2_5"]))
            if "us_aqi" in c and c["us_aqi"] is not None:
                air_us_aqi.set(float(c["us_aqi"]))
            return
        except Exception as e:
            log_warn(f"{api}: parse current failed: {e}")
            # пойдём в hourly как fallback

    # --- fallback: hourly ---
    url_hourly = (
        "https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={LAT}&longitude={LON}"
        "&hourly=pm10,pm2_5,us_aqi&timezone=auto"
    )
    data, latency, err = timed_get(api, url_hourly)
    api_latency_seconds.labels(api).observe(latency)
    api_last_scrape_timestamp_seconds.labels(api).set(time.time())

    if err or not data or "hourly" not in data:
        if err:
            log_warn(f"{api}: {err}")
        return

    h = data["hourly"]
    try:
        v_pm10 = _last_past_value(h, "pm10")
        v_pm25 = _last_past_value(h, "pm2_5")
        v_aqi = _last_past_value(h, "us_aqi")

        if v_pm10 is not None:
            air_pm10_ugm3.set(v_pm10)
        if v_pm25 is not None:
            air_pm25_ugm3.set(v_pm25)
        if v_aqi is not None:
            air_us_aqi.set(v_aqi)
    except Exception as e:
        log_warn(f"{api}: parse hourly failed: {e}")


def poll_fx():
    api = "frankfurter"
    url = "https://api.frankfurter.app/latest?from=USD&to=KZT,EUR"
    data, latency, err = timed_get(api, url)
    api_latency_seconds.labels(api).observe(latency)
    api_last_scrape_timestamp_seconds.labels(api).set(time.time())
    if err or not data or "rates" not in data:
        if err:
            log_warn(f"{api}: {err}")
        return
    try:
        rates = data["rates"]
        if "KZT" in rates and rates["KZT"] is not None:
            fx_usd_kzt.set(float(rates["KZT"]))
        if "EUR" in rates and rates["EUR"] is not None:
            fx_eur_kzt.set(float(rates["EUR"]))
    except Exception as e:
        log_warn(f"{api}: parse failed: {e}")


def poll_crypto():
    api = "binance"
    btc, l1, e1 = timed_get(api, "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
    api_latency_seconds.labels(api).observe(l1)
    api_last_scrape_timestamp_seconds.labels(api).set(time.time())

    eth, l2, e2 = timed_get(api, "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT")
    api_latency_seconds.labels(api).observe(l2)
    api_last_scrape_timestamp_seconds.labels(api).set(time.time())

    if e1:
        log_warn(f"{api} BTC: {e1}")
    if e2:
        log_warn(f"{api} ETH: {e2}")

    try:
        if btc and "price" in btc and btc["price"] is not None:
            crypto_btc_usd.set(float(btc["price"]))
        if eth and "price" in eth and eth["price"] is not None:
            crypto_eth_usd.set(float(eth["price"]))
    except Exception as e:
        log_warn(f"{api}: parse failed: {e}")


def poll_stackoverflow():
    global LAST_SO_POLL
    if time.time() - LAST_SO_POLL < SO_MIN_PERIOD:
        return
    LAST_SO_POLL = time.time()

    api = "stackoverflow"
    tags = ["tensorflow", "linux"]
    url = f"https://api.stackexchange.com/2.3/tags/{';'.join(tags)}/info?site=stackoverflow"

    data, latency, err = timed_get(api, url)
    api_latency_seconds.labels(api).observe(latency)
    api_last_scrape_timestamp_seconds.labels(api).set(time.time())
    if err or not data or "items" not in data:
        if err:
            log_warn(f"{api}: {err}")
        return

    try:
        for item in data["items"]:
            name = item.get("name")
            cnt = item.get("count")
            if name and cnt is not None:
                so_tag_count.labels(tag=name).set(float(cnt))
    except Exception as e:
        log_warn(f"{api}: parse failed: {e}")


def loop():
    while True:
        try:
            poll_openmeteo_weather()
            poll_openmeteo_air()
            poll_fx()
            poll_crypto()
            poll_stackoverflow()
        except Exception as e:
            # чтобы никогда не падать из-за неожиданного исключения
            log_warn(f"main loop error: {e}")
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    log_info("Starting custom-exporter on :8000")
    start_http_server(8000)
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    # «держим» процесс живым
    while True:
        time.sleep(3600)
