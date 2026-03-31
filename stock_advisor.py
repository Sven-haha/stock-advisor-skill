#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股操盘手技能 - 增强版
功能：集合竞价分析、盘中异动监控、下午策略
"""

import os
import sys
import json
import time
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
import pandas as pd
import numpy as np
import akshare as ak
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

def configure_runtime_network():
    # 仅影响当前脚本进程：东财接口走直连，其他数据源仍可沿用系统代理。
    bypass_hosts = [
        "eastmoney.com",
        ".eastmoney.com",
        "push2.eastmoney.com",
        "82.push2.eastmoney.com",
        "48.push2.eastmoney.com",
        "push2his.eastmoney.com",
    ]
    existing = os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or ""
    existing_items = [item.strip() for item in existing.split(",") if item.strip()]
    merged = existing_items[:]
    for host in bypass_hosts:
        if host not in merged:
            merged.append(host)
    merged_value = ",".join(merged)
    os.environ["NO_PROXY"] = merged_value
    os.environ["no_proxy"] = merged_value

configure_runtime_network()

# ================== 配置参数 ==================
DEFAULT_STOCK_POOL = {
    "002249": "大洋电机",
    "300274": "阳光电源",
    "300476": "胜宏科技",
    "600151": "航天机电",
    "300192": "科德教育",
    "603399": "永杉锂业"
}
SPIKE_WINDOW_MIN = 5
SPIKE_PCT = 2.0
SPIKE_VOL_RATIO = 1.5
SPIKE_RELAXED_PCT = 1.2
SPIKE_RELAXED_VOL_RATIO = 1.0
INTRADAY_ALERT_PCT = 3.0
INTRADAY_ALERT_VOL_RATIO = 1.2
MONITOR_INTERVAL = 60
OFF_HOURS_HEARTBEAT_SEC = 600
POST_CLOSE_RUN_HOUR = 15
POST_CLOSE_RUN_MINUTE = 5
MA_SHORT = 5
MA_LONG = 10
STRONG_SECTOR_PCT = 1.0
WEAK_SECTOR_PCT = -1.0
REQUEST_TIMEOUT_SEC = 20
REQUEST_RETRIES = 3
REQUEST_RETRY_DELAY = 2
SPOT_CACHE_TTL_SEC = 90
MARKET_CACHE_TTL_SEC = 90
MA_CACHE_TTL_SEC = 300
CACHE_DIR = ".cache"
SPOT_CACHE_FILE = os.path.join(CACHE_DIR, "spot_snapshot.csv")
MA_CACHE_FILE = os.path.join(CACHE_DIR, "ma_cache.json")
VOLUME_CACHE_FILE = os.path.join(CACHE_DIR, "volume_cache.json")
TASK_STATE_FILE = os.path.join(CACHE_DIR, "task_state.json")
LOG_THROTTLE_SEC = 60

stock_pool = DEFAULT_STOCK_POOL.copy()
last_prices = {}
last_check_time = None
spot_cache = {"data": pd.DataFrame(), "ts": None}
market_cache = {"data": {}, "ts": None}
ma_cache = {}
industry_map = {}
volume_baseline_cache = {}
last_log_time = {}
last_alert_times = {}
last_task_run = {"morning": None, "afternoon": None, "post_close": None}

# ================== 辅助函数 ==================
def load_config():
    global stock_pool, industry_map
    config_file = os.environ.get("STOCK_CONFIG", "stock_config.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if "stock_pool" in config:
                    stock_pool = config["stock_pool"]
                    print(f"已加载外部股票池，共{len(stock_pool)}只个股")
                if "industry_map" in config and isinstance(config["industry_map"], dict):
                    industry_map = {str(k): str(v) for k, v in config["industry_map"].items()}
                    print(f"已加载行业映射，共{len(industry_map)}条")
        except Exception as e:
            print(f"加载配置文件失败: {e}")

def save_config():
    config_file = os.environ.get("STOCK_CONFIG", "stock_config.json")
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump({"stock_pool": stock_pool, "industry_map": industry_map}, f, ensure_ascii=False, indent=2)
    print(f"股票池已保存至 {config_file}")

def is_trading_day(date: datetime.date) -> bool:
    try:
        trade_cal = ak.tool_trade_date_hist_sina()
        if isinstance(trade_cal, pd.DataFrame):
            trade_dates = pd.to_datetime(trade_cal['trade_date']).dt.date.tolist()
            return date in trade_dates
    except Exception:
        pass
    return date.weekday() < 5

def is_trading_time(current_time: datetime.time) -> bool:
    morning_start = datetime.time(9, 30)
    morning_end = datetime.time(11, 30)
    afternoon_start = datetime.time(13, 0)
    afternoon_end = datetime.time(15, 0)
    return (morning_start <= current_time <= morning_end) or (afternoon_start <= current_time <= afternoon_end)

def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)

def _log_throttled(key: str, message: str, cooldown: int = LOG_THROTTLE_SEC):
    now_ts = time.time()
    last_ts = last_log_time.get(key, 0)
    if now_ts - last_ts >= cooldown:
        print(message)
        last_log_time[key] = now_ts

def _cache_valid(ts: datetime.datetime, ttl_sec: int) -> bool:
    if ts is None:
        return False
    if isinstance(ts, (int, float)):
        return (time.time() - ts) < ttl_sec
    if isinstance(ts, datetime.datetime):
        return (datetime.datetime.now() - ts).total_seconds() < ttl_sec
    return False

def _load_json_cache(path: str) -> Dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _save_json_cache(path: str, data: Dict):
    _ensure_cache_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _load_spot_cache_from_disk() -> pd.DataFrame:
    if not os.path.exists(SPOT_CACHE_FILE):
        return pd.DataFrame()
    try:
        df = pd.read_csv(SPOT_CACHE_FILE, dtype={"代码": str})
        return df if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def _save_spot_cache_to_disk(df: pd.DataFrame):
    if df is None or df.empty:
        return
    _ensure_cache_dir()
    df.to_csv(SPOT_CACHE_FILE, index=False)

def _bootstrap_runtime_cache():
    global ma_cache, volume_baseline_cache
    ma_raw = _load_json_cache(MA_CACHE_FILE)
    ma_cache = {}
    for key, item in ma_raw.items():
        if not isinstance(item, dict):
            continue
        value = item.get("value")
        ts = item.get("ts")
        if isinstance(value, list) and len(value) == 2:
            ma_cache[key] = {"value": (float(value[0]), float(value[1])), "ts": float(ts or 0)}
    vol_raw = _load_json_cache(VOLUME_CACHE_FILE)
    volume_baseline_cache = {}
    for key, item in vol_raw.items():
        if not isinstance(item, dict):
            continue
        value = item.get("value")
        ts = item.get("ts")
        if value is not None:
            volume_baseline_cache[key] = {"value": float(value), "ts": float(ts or 0)}

def _load_task_state():
    global last_task_run
    state = _load_json_cache(TASK_STATE_FILE)
    for key in ("morning", "afternoon", "post_close"):
        raw = state.get(key)
        if isinstance(raw, str) and raw:
            try:
                last_task_run[key] = datetime.datetime.strptime(raw, "%Y-%m-%d").date()
            except Exception:
                last_task_run[key] = None

def _save_task_state():
    serializable = {}
    for key, value in last_task_run.items():
        serializable[key] = value.isoformat() if isinstance(value, datetime.date) else ""
    _save_json_cache(TASK_STATE_FILE, serializable)

def _send_notification(title: str, message: str):
    safe_title = str(title).replace('"', '\\"')
    safe_message = str(message).replace('"', '\\"')
    os.system(
        f"osascript -e 'display notification \"{safe_message}\" with title \"{safe_title}\" sound name \"Sosumi\"'"
    )

def _run_with_timeout(func, *args, timeout=REQUEST_TIMEOUT_SEC, **kwargs):
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError as e:
            future.cancel()
            raise TimeoutError(f"调用超时（>{timeout}秒）") from e

def _fetch_with_retries(label: str, func, *args, retries=REQUEST_RETRIES, timeout=REQUEST_TIMEOUT_SEC, **kwargs):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return _run_with_timeout(func, *args, timeout=timeout, **kwargs)
        except Exception as e:
            last_error = e
            if attempt < retries:
                _log_throttled(f"retry:{label}", f"{label}失败，第{attempt}次重试: {e}")
                time.sleep(REQUEST_RETRY_DELAY * attempt)
    raise RuntimeError(f"{label}连续失败{retries}次: {last_error}")

def _normalize_spot_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    normalized = df.copy()
    for col in ['最新价', '开盘价', '昨收', '成交量', '量比', '换手率', '涨跌幅', '振幅']:
        if col in normalized.columns:
            normalized[col] = pd.to_numeric(normalized[col], errors='coerce').fillna(0.0)
    return normalized

def _normalize_sina_spot_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    normalized = df.copy()
    rename_map = {
        "代码": "代码",
        "名称": "名称",
        "最新价": "最新价",
        "今开": "开盘价",
        "昨收": "昨收",
        "成交量": "成交量",
        "涨跌幅": "涨跌幅",
        "最高": "最高",
        "最低": "最低",
        "成交额": "成交额",
    }
    normalized = normalized.rename(columns=rename_map)
    normalized["代码"] = normalized["代码"].astype(str).str[-6:]
    for col in ['最新价', '开盘价', '昨收', '成交量', '涨跌幅', '最高', '最低', '成交额']:
        if col in normalized.columns:
            normalized[col] = pd.to_numeric(normalized[col], errors='coerce').fillna(0.0)
    for col in ['量比', '换手率', '振幅']:
        if col not in normalized.columns:
            normalized[col] = 0.0
    if '行业' not in normalized.columns:
        normalized['行业'] = ''
    if industry_map:
        normalized['行业'] = normalized.apply(
            lambda r: industry_map.get(str(r.get("代码", "")), r.get("行业", "")),
            axis=1
        )
    return normalized

def _intraday_progress(now: datetime.datetime = None) -> float:
    now = now or datetime.datetime.now()
    t = now.time()
    morning_start = datetime.time(9, 30)
    morning_end = datetime.time(11, 30)
    afternoon_start = datetime.time(13, 0)
    afternoon_end = datetime.time(15, 0)

    def minutes(dt_time):
        return dt_time.hour * 60 + dt_time.minute + dt_time.second / 60.0

    total = 240.0
    if t < morning_start:
        return 0.0
    if morning_start <= t <= morning_end:
        passed = minutes(t) - minutes(morning_start)
        return max(0.0, min(1.0, passed / total))
    if morning_end < t < afternoon_start:
        return 120.0 / total
    if afternoon_start <= t <= afternoon_end:
        passed = 120.0 + (minutes(t) - minutes(afternoon_start))
        return max(0.0, min(1.0, passed / total))
    return 1.0

def _get_recent_avg_volume_from_sina(code: str, days: int = 5) -> float:
    cache_key = f"{code}:{days}"
    cached = volume_baseline_cache.get(cache_key)
    if cached and _cache_valid(cached["ts"], MA_CACHE_TTL_SEC):
        return cached["value"]
    try:
        hist = _fetch_daily_from_sina(code)
        if hist.empty or "volume" not in hist.columns:
            return 0.0
        hist = hist.sort_values("date")
        avg_volume = float(hist["volume"].tail(days).mean())
        if np.isnan(avg_volume):
            return 0.0
        volume_baseline_cache[cache_key] = {"value": avg_volume, "ts": datetime.datetime.now()}
        serializable = {
            k: {"value": v["value"], "ts": v["ts"].timestamp() if isinstance(v["ts"], datetime.datetime) else float(v["ts"])}
            for k, v in volume_baseline_cache.items()
        }
        _save_json_cache(VOLUME_CACHE_FILE, serializable)
        return avg_volume
    except Exception:
        return 0.0

def _fetch_spot_from_sina() -> pd.DataFrame:
    _log_throttled("fallback:sina_spot", "东财实时行情不可用，切换到新浪实时行情源")
    last_error = None
    for attempt in range(1, 3):
        try:
            spot_df = ak.stock_zh_a_spot()
            return _normalize_sina_spot_df(spot_df)
        except Exception as e:
            last_error = e
            if attempt < 2:
                _log_throttled("retry:sina_spot", f"获取新浪A股实时行情失败，第{attempt}次重试: {e}")
                time.sleep(2 * attempt)
    raise RuntimeError(f"获取新浪A股实时行情连续失败2次: {last_error}")

def _to_sina_symbol(code: str) -> str:
    code = str(code)
    if code.startswith(("6", "9")):
        return f"sh{code}"
    return f"sz{code}"

def _fetch_daily_from_sina(code: str) -> pd.DataFrame:
    symbol = _to_sina_symbol(code)
    _log_throttled(f"fallback:sina_daily:{code}", f"东财历史行情不可用，切换到新浪日线数据源: {symbol}")
    last_error = None
    daily_df = pd.DataFrame()
    for attempt in range(1, 3):
        try:
            daily_df = ak.stock_zh_a_daily(symbol=symbol)
            break
        except Exception as e:
            last_error = e
            if attempt < 2:
                _log_throttled(f"retry:sina_daily:{code}", f"获取{code}新浪日线失败，第{attempt}次重试: {e}")
                time.sleep(2 * attempt)
    else:
        raise RuntimeError(f"获取{code}新浪日线连续失败2次: {last_error}")
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()
    normalized = daily_df.copy()
    if "date" in normalized.columns:
        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    for col in ["close", "open", "high", "low", "volume", "amount"]:
        if col in normalized.columns:
            normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    return normalized

def get_index_snapshot() -> pd.DataFrame:
    try:
        index_df = ak.stock_zh_index_spot_sina()
        return index_df
    except Exception as e:
        print(f"获取指数实时行情失败: {e}")
        return pd.DataFrame()

def get_spot_snapshot(force_refresh: bool = False) -> pd.DataFrame:
    global spot_cache
    if not force_refresh and _cache_valid(spot_cache["ts"], SPOT_CACHE_TTL_SEC) and not spot_cache["data"].empty:
        return spot_cache["data"].copy()
    try:
        spot_df = _fetch_with_retries("获取A股实时行情", ak.stock_zh_a_spot_em)
        spot_df = _normalize_spot_df(spot_df)
        if not spot_df.empty:
            spot_cache = {"data": spot_df.copy(), "ts": datetime.datetime.now()}
            _save_spot_cache_to_disk(spot_df)
            return spot_df
    except Exception as e:
        _log_throttled("fail:spot_em", f"获取A股实时行情失败: {e}")
    try:
        spot_df = _fetch_spot_from_sina()
        if not spot_df.empty:
            spot_cache = {"data": spot_df.copy(), "ts": datetime.datetime.now()}
            _save_spot_cache_to_disk(spot_df)
            return spot_df
    except Exception as e:
        _log_throttled("fail:spot_sina", f"新浪实时行情也失败: {e}")
    if not spot_cache["data"].empty:
        _log_throttled("fallback:memory_spot", "改用最近一次成功获取的行情缓存数据")
        return spot_cache["data"].copy()
    disk_df = _load_spot_cache_from_disk()
    if not disk_df.empty:
        _log_throttled("fallback:disk_spot", "实时接口不可用，改用本地磁盘缓存行情数据")
        spot_cache["data"] = disk_df.copy()
        spot_cache["ts"] = datetime.datetime.now()
        return disk_df
    return pd.DataFrame()

def get_market_overview() -> Dict:
    global market_cache
    if _cache_valid(market_cache["ts"], MARKET_CACHE_TTL_SEC) and market_cache["data"]:
        return market_cache["data"].copy()
    try:
        spot_df = get_spot_snapshot()
        if spot_df.empty:
            return market_cache["data"].copy() if market_cache["data"] else {}
        indices = {"上证指数": "000001", "深证成指": "399001", "创业板指": "399006"}
        market = {}
        for name, code in indices.items():
            row = spot_df[spot_df['代码'] == code]
            if not row.empty:
                market[name] = {
                    "涨跌幅": float(row.iloc[0]['涨跌幅']),
                    "最新价": float(row.iloc[0]['最新价']),
                    "成交量": float(row.iloc[0]['成交量'])
                }
        if market:
            market_cache = {"data": market.copy(), "ts": datetime.datetime.now()}
            return market
        index_df = get_index_snapshot()
        if not index_df.empty:
            code_map = {"上证指数": "sh000001", "深证成指": "sz399001", "创业板指": "sz399006"}
            for name, code in code_map.items():
                row = index_df[index_df["代码"] == code]
                if not row.empty:
                    market[name] = {
                        "涨跌幅": float(pd.to_numeric(row.iloc[0]["涨跌幅"], errors="coerce")),
                        "最新价": float(pd.to_numeric(row.iloc[0]["最新价"], errors="coerce")),
                        "成交量": float(pd.to_numeric(row.iloc[0]["成交量"], errors="coerce")),
                    }
        if market:
            market_cache = {"data": market.copy(), "ts": datetime.datetime.now()}
        return market
    except Exception as e:
        print(f"获取大盘数据失败: {e}")
        return market_cache["data"].copy() if market_cache["data"] else {}

def get_stock_realtime(stock_codes: List[str]) -> pd.DataFrame:
    try:
        spot_df = get_spot_snapshot()
        if spot_df.empty:
            return pd.DataFrame()
        target_df = spot_df[spot_df['代码'].isin(stock_codes)].copy()
        if target_df.empty:
            return pd.DataFrame()
        target_df.rename(columns={
            '代码': 'code', '名称': 'name', '最新价': 'price', '开盘价': 'open',
            '昨收': 'pre_close', '成交量': 'volume', '量比': 'volume_ratio',
            '换手率': 'turnover', '涨跌幅': 'pct_chg', '振幅': 'amplitude', '行业': 'industry'
        }, inplace=True)
        for col in ['price', 'open', 'pre_close', 'volume', 'volume_ratio', 'turnover', 'pct_chg', 'amplitude']:
            if col in target_df.columns:
                target_df[col] = pd.to_numeric(target_df[col], errors='coerce').fillna(0.0)
        # 新浪兜底常缺量比，按“当日累计成交量 / 近5日平均成交量 * 交易进度校正”估算。
        progress = _intraday_progress()
        if progress > 0 and "volume_ratio" in target_df.columns:
            for idx, row in target_df.iterrows():
                if row.get("volume_ratio", 0) > 0:
                    continue
                avg_volume = _get_recent_avg_volume_from_sina(row["code"], days=5)
                if avg_volume <= 0:
                    continue
                expected = avg_volume * progress
                if expected <= 0:
                    continue
                est_ratio = float(row.get("volume", 0)) / expected
                target_df.at[idx, "volume_ratio"] = max(0.0, round(est_ratio, 2))
        pre_close = target_df['pre_close'].replace(0, np.nan)
        target_df['open_pct'] = ((target_df['open'] - pre_close) / pre_close * 100).fillna(0.0)
        return target_df
    except Exception as e:
        print(f"获取个股实时数据失败: {e}")
        return pd.DataFrame()

def get_sector_performance(industry: str, spot_all: pd.DataFrame) -> float:
    if not industry or spot_all.empty:
        return 0.0
    sector_stocks = spot_all[spot_all['行业'] == industry]
    if sector_stocks.empty:
        return 0.0
    return sector_stocks['涨跌幅'].mean()

def get_ma_values(code: str, end_date: str = None) -> Tuple[float, float]:
    cache_key = f"{code}:{end_date or 'latest'}"
    cached = ma_cache.get(cache_key)
    if cached and _cache_valid(cached["ts"], MA_CACHE_TTL_SEC):
        return cached["value"]
    try:
        hist = _fetch_with_retries(
            f"获取{code}均线数据",
            ak.stock_zh_a_hist,
            symbol=code,
            period="daily",
            start_date="",
            end_date=end_date,
            adjust="qfq"
        )
        if hist.empty or len(hist) < MA_LONG:
            raise ValueError("东财历史数据不足，无法计算均线")
        ma5 = hist['收盘'].rolling(window=MA_SHORT).mean().iloc[-1]
        ma10 = hist['收盘'].rolling(window=MA_LONG).mean().iloc[-1]
        ma_value = (float(ma5), float(ma10))
        ma_cache[cache_key] = {"value": ma_value, "ts": datetime.datetime.now()}
        serializable = {
            k: {"value": list(v["value"]), "ts": v["ts"].timestamp() if isinstance(v["ts"], datetime.datetime) else float(v["ts"])}
            for k, v in ma_cache.items()
        }
        _save_json_cache(MA_CACHE_FILE, serializable)
        return ma_value
    except Exception as e:
        _log_throttled(f"fail:ma_em:{code}", f"获取{code}均线失败: {e}")
    try:
        hist = _fetch_daily_from_sina(code)
        if hist.empty or len(hist) < MA_LONG or "close" not in hist.columns:
            return 0.0, 0.0
        if end_date:
            end_ts = pd.to_datetime(end_date, format="%Y%m%d", errors="coerce")
            if pd.notna(end_ts):
                hist = hist[hist["date"] <= end_ts]
        hist = hist.sort_values("date")
        ma5 = hist["close"].rolling(window=MA_SHORT).mean().iloc[-1]
        ma10 = hist["close"].rolling(window=MA_LONG).mean().iloc[-1]
        ma_value = (float(ma5), float(ma10))
        ma_cache[cache_key] = {"value": ma_value, "ts": datetime.datetime.now()}
        serializable = {
            k: {"value": list(v["value"]), "ts": v["ts"].timestamp() if isinstance(v["ts"], datetime.datetime) else float(v["ts"])}
            for k, v in ma_cache.items()
        }
        _save_json_cache(MA_CACHE_FILE, serializable)
        return ma_value
    except Exception as e:
        _log_throttled(f"fail:ma_sina:{code}", f"获取{code}新浪均线失败: {e}")
        disk_ma = _load_json_cache(MA_CACHE_FILE).get(cache_key, {})
        if isinstance(disk_ma, dict):
            value = disk_ma.get("value")
            if isinstance(value, list) and len(value) == 2:
                _log_throttled(f"fallback:ma_disk:{code}", f"使用{code}本地缓存均线数据")
                return float(value[0]), float(value[1])
        return 0.0, 0.0

def generate_advice(stock_row: pd.Series, market: Dict, sector_pct: float, ma5: float, ma10: float,
                    scenario="morning") -> str:
    code = stock_row['code']
    name = stock_row['name']
    open_pct = stock_row.get('open_pct', 0)
    vol_ratio = stock_row['volume_ratio']
    turnover = stock_row['turnover']
    price = stock_row['price']
    industry = stock_row.get('industry', '未知')
    pct_chg = stock_row.get('pct_chg', 0)

    market_mood = "中性"
    if market:
        cyb = market.get("创业板指", {})
        cyb_pct = cyb.get("涨跌幅", 0)
        if cyb_pct > 0.5:
            market_mood = "偏暖"
        elif cyb_pct < -0.5:
            market_mood = "偏冷"

    sector_desc = "弱势" if sector_pct < WEAK_SECTOR_PCT else ("强势" if sector_pct > STRONG_SECTOR_PCT else "平稳")

    tech_status = ""
    if ma5 > 0 and ma10 > 0:
        if price > ma5 and price > ma10:
            tech_status = "多头排列"
        elif price < ma5 and price < ma10:
            tech_status = "空头排列"
        elif price > ma5 and price < ma10:
            tech_status = "短期突破中期压制"
        else:
            tech_status = "震荡整理"

    if scenario == "morning":
        if open_pct > 2.0 and vol_ratio > 1.5 and sector_pct > 1.0:
            action = "积极关注，可建仓"
            reason = f"高开{open_pct:.1f}%，量比{vol_ratio:.2f}，板块强势({sector_pct:.1f}%)"
            advice = f"若开盘15分钟内站稳开盘价{stock_row['open']:.2f}，可加仓至5成；止损开盘价下3%。"
        elif open_pct < -2.0 and vol_ratio > 1.5 and sector_pct < -1.0:
            action = "减仓或止损"
            reason = f"低开{open_pct:.1f}%，量比{vol_ratio:.2f}，板块弱势，抛压重"
            advice = f"反弹减仓，严格止损设开盘价下2%。"
        else:
            action = "观望"
            reason = "集合竞价无明显信号"
            advice = "等待开盘后方向确认。"
    elif scenario == "afternoon":
        if pct_chg > 3.0 and vol_ratio > 1.2:
            action = "谨慎追高，可部分止盈"
            reason = f"上午上涨{pct_chg:.1f}%，量比{vol_ratio:.2f}，下午或有获利回吐"
            advice = "若下午开盘冲高乏力，可减仓1/3锁定利润；若放量突破上午高点，可继续持有。"
        elif pct_chg < -3.0 and vol_ratio > 1.2:
            action = "等待企稳，不宜杀跌"
            reason = f"上午下跌{pct_chg:.1f}%，量比{vol_ratio:.2f}，恐慌释放中"
            advice = "午后若不再创新低，可小仓位低吸做T；否则观望。"
        else:
            action = "持股观察"
            reason = f"上午涨跌{pct_chg:+.1f}%，量能正常"
            advice = "维持现有仓位，关注下午2点后的方向选择。"
    else:
        action = "短线博弈机会"
        reason = f"快速拉升，{SPIKE_WINDOW_MIN}分钟内涨幅{SPIKE_PCT}%以上，量比{vol_ratio:.2f}"
        advice = f"若拉升放量且板块配合，可小仓位跟随（不超过2成），止损设拉升启动价下方2%。若无量虚拉，则逢高减仓。"

    output = f"""
【{name}({code})】{'【集合竞价】' if scenario=='morning' else '【午后策略】' if scenario=='afternoon' else '【盘中异动】'}
当前价: {price:.2f} | 涨跌幅: {pct_chg:+.1f}% | 量比: {vol_ratio:.2f}
技术位置: MA5={ma5:.2f} MA10={ma10:.2f} ({tech_status})
板块强度: {industry}板块 {sector_pct:+.1f}% ({sector_desc})
市场情绪: {market_mood}
操作建议: {action}
理由: {reason}
策略: {advice}
"""
    return output

def morning_analysis():
    print(f"\n===== 集合竞价分析 {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} =====")
    market = get_market_overview()
    if market:
        print("\n【大盘环境】")
        for idx, data in market.items():
            print(f"{idx}: {data['涨跌幅']:+.2f}% 成交量{data['成交量']/1e8:.1f}亿")

    all_spot = get_spot_snapshot()
    if all_spot.empty:
        print("获取全市场数据失败，且无可用缓存，暂不输出个股建议")
        return

    stock_codes = list(stock_pool.keys())
    stock_df = get_stock_realtime(stock_codes)
    if stock_df.empty:
        print("未获取到目标股票数据")
        return

    print("\n【个股操作建议】")
    for _, row in stock_df.iterrows():
        industry = row.get('industry', '')
        sector_pct = get_sector_performance(industry, all_spot) if industry else 0.0
        ma5, ma10 = get_ma_values(row['code'], datetime.datetime.now().strftime("%Y%m%d"))
        advice = generate_advice(row, market, sector_pct, ma5, ma10, scenario="morning")
        print(advice)
        print("-" * 60)
    print("分析完毕。\n")

def afternoon_advice():
    print(f"\n===== 下午开盘策略 {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} =====")
    market = get_market_overview()
    all_spot = get_spot_snapshot()
    if all_spot.empty:
        print("获取全市场数据失败，且无可用缓存，暂不输出下午策略")
        return

    stock_codes = list(stock_pool.keys())
    stock_df = get_stock_realtime(stock_codes)
    if stock_df.empty:
        print("未获取到目标股票数据")
        return

    print("\n【下午操盘建议】")
    for _, row in stock_df.iterrows():
        industry = row.get('industry', '')
        sector_pct = get_sector_performance(industry, all_spot) if industry else 0.0
        ma5, ma10 = get_ma_values(row['code'], datetime.datetime.now().strftime("%Y%m%d"))
        advice = generate_advice(row, market, sector_pct, ma5, ma10, scenario="afternoon")
        print(advice)
        print("-" * 60)
    print("下午策略发布完毕。\n")

def post_close_review():
    print(f"\n===== 盘后复盘策略 {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')} =====")
    market = get_market_overview()
    if market:
        print("\n【收盘大盘环境】")
        for idx, data in market.items():
            print(f"{idx}: {data['涨跌幅']:+.2f}% 成交量{data['成交量']/1e8:.1f}亿")

    stock_codes = list(stock_pool.keys())
    stock_df = get_stock_realtime(stock_codes)
    if stock_df.empty:
        print("盘后未获取到目标股票数据")
        return

    print("\n【盘后关注清单】")
    top_up = stock_df.sort_values("pct_chg", ascending=False).head(2)
    top_down = stock_df.sort_values("pct_chg", ascending=True).head(2)
    for _, row in pd.concat([top_up, top_down]).drop_duplicates(subset=["code"]).iterrows():
        print(
            f"{row['name']}({row['code']}): 涨跌幅{row['pct_chg']:+.2f}% "
            f"量比{row['volume_ratio']:.2f} 行业{row.get('industry', '未知')}"
        )
    print("盘后复盘输出完毕。\n")

def _can_run_task_once_per_day(task_key: str, now: datetime.datetime) -> bool:
    last_day = last_task_run.get(task_key)
    return last_day != now.date()

def run_auto_scheduled_tasks(now: datetime.datetime):
    if not is_trading_day(now.date()):
        return
    t = now.time()
    if datetime.time(9, 25) <= t < datetime.time(9, 30) and _can_run_task_once_per_day("morning", now):
        print("自动任务触发：盘前策略")
        morning_analysis()
        last_task_run["morning"] = now.date()
        _save_task_state()
    if datetime.time(13, 0) <= t < datetime.time(13, 11) and _can_run_task_once_per_day("afternoon", now):
        print("自动任务触发：盘中策略")
        afternoon_advice()
        last_task_run["afternoon"] = now.date()
        _save_task_state()
    if (
        t >= datetime.time(POST_CLOSE_RUN_HOUR, POST_CLOSE_RUN_MINUTE)
        and _can_run_task_once_per_day("post_close", now)
    ):
        print("自动任务触发：盘后复盘")
        post_close_review()
        last_task_run["post_close"] = now.date()
        _save_task_state()

def check_spike():
    global last_prices, last_check_time
    now = datetime.datetime.now()
    current_time = now.time()
    if not is_trading_time(current_time):
        return

    stock_codes = list(stock_pool.keys())
    stock_df = get_stock_realtime(stock_codes)
    if stock_df.empty:
        _log_throttled("monitor:empty", "监控心跳：未拉到目标股票实时数据，等待下轮")
        return

    for _, row in stock_df.iterrows():
        code = row['code']
        if code not in last_prices:
            last_prices[code] = {'price': row['price'], 'time': now, 'alerted': False}

    if last_check_time is None or last_check_time.date() != now.date():
        last_prices = {code: {'price': row['price'], 'time': now, 'alerted': False} for _, row in stock_df.iterrows()}
        last_check_time = now

    all_spot = get_spot_snapshot()
    triggered = 0
    checked = 0

    for _, row in stock_df.iterrows():
        checked += 1
        code = row['code']
        name = row['name']
        price = row['price']
        vol_ratio = row['volume_ratio']
        intraday_pct = row.get('pct_chg', 0)
        last = last_prices.get(code)
        if last is None:
            continue
        time_diff = (now - last['time']).total_seconds() / 60
        if time_diff <= SPIKE_WINDOW_MIN and not last['alerted']:
            pct_change = (price - last['price']) / last['price'] * 100
            is_classic_spike = pct_change >= SPIKE_PCT and vol_ratio >= SPIKE_VOL_RATIO
            is_relaxed_spike = pct_change >= SPIKE_RELAXED_PCT and vol_ratio >= SPIKE_RELAXED_VOL_RATIO
            is_intraday_breakout = intraday_pct >= INTRADAY_ALERT_PCT and vol_ratio >= INTRADAY_ALERT_VOL_RATIO
            if is_classic_spike or is_relaxed_spike or is_intraday_breakout:
                market = get_market_overview()
                industry = row.get('industry', '')
                sector_pct = get_sector_performance(industry, all_spot) if industry else 0.0
                ma5, ma10 = get_ma_values(code, datetime.datetime.now().strftime("%Y%m%d"))
                advice = generate_advice(row, market, sector_pct, ma5, ma10, scenario="spike")
                print(f"\n🚨 异动提醒 {now.strftime('%H:%M:%S')} 🚨")
                print(advice)
                print("-" * 60)
                last_prices[code]['alerted'] = True
                last_alert_times[code] = now
                _send_notification("A股操盘手 异动提醒", f"🚨 {name}({code}) 异动，量比{vol_ratio:.2f}，请关注！")
                triggered += 1
        elif time_diff > SPIKE_WINDOW_MIN:
            last_prices[code] = {'price': price, 'time': now, 'alerted': False}
    _log_throttled("monitor:heartbeat", f"监控心跳：已检查{checked}只，触发{triggered}只")

def monitor_loop():
    while True:
        try:
            now = datetime.datetime.now()
            run_auto_scheduled_tasks(now)
            current_time = now.time()
            if is_trading_time(current_time):
                check_spike()
            else:
                _log_throttled(
                    "monitor:offhours",
                    f"非交易时段心跳：{now.strftime('%Y-%m-%d %H:%M:%S')} 服务运行中",
                    cooldown=OFF_HOURS_HEARTBEAT_SEC
                )
                time.sleep(30)
            time.sleep(MONITOR_INTERVAL)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"监控异常: {e}")
            time.sleep(60)

def run_scheduled_tasks():
    """根据环境变量或命令行参数执行一次性任务（适配 Qclaw 调度）"""
    now = datetime.datetime.now()
    hour = now.hour
    minute = now.minute
    if hour == 9 and minute >= 25 and minute < 30:
        morning_analysis()
    elif hour == 13 and (minute == 0 or minute == 10):
        afternoon_advice()
    else:
        morning_analysis()

if __name__ == "__main__":
    _bootstrap_runtime_cache()
    _load_task_state()
    load_config()
    if not os.path.exists("stock_config.json"):
        save_config()
    if len(sys.argv) > 1 and sys.argv[1] == "--test-notify":
        _send_notification("A股操盘手 自检", "通知通道正常，可接收异动提醒。")
        print("通知自检已发送，请查看系统通知中心。")
        sys.exit(0)
    if len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        print(
            f"已进入监控模式：交易时段每{MONITOR_INTERVAL}秒检查一次，"
            f"非交易时段每{OFF_HOURS_HEARTBEAT_SEC//60}分钟输出心跳"
        )
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        while True:
            time.sleep(60)
    else:
        run_scheduled_tasks()
