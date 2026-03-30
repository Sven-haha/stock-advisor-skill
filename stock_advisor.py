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
import pandas as pd
import numpy as np
import akshare as ak
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

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
MONITOR_INTERVAL = 60
MA_SHORT = 5
MA_LONG = 10
STRONG_SECTOR_PCT = 1.0
WEAK_SECTOR_PCT = -1.0

stock_pool = DEFAULT_STOCK_POOL.copy()
last_prices = {}
last_check_time = None

# ================== 辅助函数 ==================
def load_config():
    global stock_pool
    config_file = os.environ.get("STOCK_CONFIG", "stock_config.json")
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                if "stock_pool" in config:
                    stock_pool = config["stock_pool"]
                    print(f"已加载外部股票池，共{len(stock_pool)}只个股")
        except Exception as e:
            print(f"加载配置文件失败: {e}")

def save_config():
    config_file = os.environ.get("STOCK_CONFIG", "stock_config.json")
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump({"stock_pool": stock_pool}, f, ensure_ascii=False, indent=2)
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

def get_market_overview() -> Dict:
    try:
        spot_df = ak.stock_zh_a_spot_em()
        indices = {"上证指数": "000001", "深证成指": "399001", "创业板指": "399006"}
        market = {}
        for name, code in indices.items():
            row = spot_df[spot_df['代码'] == code]
            if not row.empty:
                market[name] = {
                    "涨跌幅": row.iloc[0]['涨跌幅'],
                    "最新价": row.iloc[0]['最新价'],
                    "成交量": row.iloc[0]['成交量']
                }
        return market
    except Exception as e:
        print(f"获取大盘数据失败: {e}")
        return {}

def get_stock_realtime(stock_codes: List[str]) -> pd.DataFrame:
    try:
        spot_df = ak.stock_zh_a_spot_em()
        target_df = spot_df[spot_df['代码'].isin(stock_codes)].copy()
        if target_df.empty:
            return pd.DataFrame()
        target_df.rename(columns={
            '代码': 'code', '名称': 'name', '最新价': 'price', '开盘价': 'open',
            '昨收': 'pre_close', '成交量': 'volume', '量比': 'volume_ratio',
            '换手率': 'turnover', '涨跌幅': 'pct_chg', '振幅': 'amplitude', '行业': 'industry'
        }, inplace=True)
        target_df['open_pct'] = (target_df['open'] - target_df['pre_close']) / target_df['pre_close'] * 100
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
    try:
        hist = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="", end_date=end_date, adjust="qfq")
        if hist.empty or len(hist) < MA_LONG:
            return 0.0, 0.0
        ma5 = hist['收盘'].rolling(window=MA_SHORT).mean().iloc[-1]
        ma10 = hist['收盘'].rolling(window=MA_LONG).mean().iloc[-1]
        return ma5, ma10
    except Exception as e:
        print(f"获取{code}均线失败: {e}")
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

    try:
        all_spot = ak.stock_zh_a_spot_em()
    except Exception as e:
        print(f"获取全市场数据失败: {e}")
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
    try:
        all_spot = ak.stock_zh_a_spot_em()
    except Exception as e:
        print(f"获取全市场数据失败: {e}")
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

def check_spike():
    global last_prices, last_check_time
    now = datetime.datetime.now()
    current_time = now.time()
    if not is_trading_time(current_time):
        return

    stock_codes = list(stock_pool.keys())
    stock_df = get_stock_realtime(stock_codes)
    if stock_df.empty:
        return

    for _, row in stock_df.iterrows():
        code = row['code']
        if code not in last_prices:
            last_prices[code] = {'price': row['price'], 'time': now, 'alerted': False}

    if last_check_time is None or last_check_time.date() != now.date():
        last_prices = {code: {'price': row['price'], 'time': now, 'alerted': False} for _, row in stock_df.iterrows()}
        last_check_time = now

    try:
        all_spot = ak.stock_zh_a_spot_em()
    except:
        all_spot = pd.DataFrame()

    for _, row in stock_df.iterrows():
        code = row['code']
        name = row['name']
        price = row['price']
        vol_ratio = row['volume_ratio']
        last = last_prices.get(code)
        if last is None:
            continue
        time_diff = (now - last['time']).total_seconds() / 60
        if time_diff <= SPIKE_WINDOW_MIN and not last['alerted']:
            pct_change = (price - last['price']) / last['price'] * 100
            if pct_change >= SPIKE_PCT and vol_ratio >= SPIKE_VOL_RATIO:
                market = get_market_overview()
                industry = row.get('industry', '')
                sector_pct = get_sector_performance(industry, all_spot) if industry else 0.0
                ma5, ma10 = get_ma_values(code, datetime.datetime.now().strftime("%Y%m%d"))
                advice = generate_advice(row, market, sector_pct, ma5, ma10, scenario="spike")
                print(f"\n🚨 异动提醒 {now.strftime('%H:%M:%S')} 🚨")
                print(advice)
                print("-" * 60)
                last_prices[code]['alerted'] = True
        elif time_diff > SPIKE_WINDOW_MIN:
            last_prices[code] = {'price': price, 'time': now, 'alerted': False}

def monitor_loop():
    while True:
        try:
            now = datetime.datetime.now()
            current_time = now.time()
            if is_trading_time(current_time):
                check_spike()
            else:
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
    load_config()
    if not os.path.exists("stock_config.json"):
        save_config()
    if len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()
        while True:
            time.sleep(60)
    else:
        run_scheduled_tasks()
