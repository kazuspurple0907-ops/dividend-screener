#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""高配当株スクリーニングアプリ - J-Quants API v2対応"""

from flask import Flask, jsonify, request, render_template
import requests
import json
import os
import time
import csv
import io
import re
import threading
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(BASE_DIR, 'data')
CACHE_DIR  = os.path.join(DATA_DIR, 'cache')
os.makedirs(DATA_DIR,  exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

CONFIG_FILE    = os.path.join(DATA_DIR, 'config.json')
PORTFOLIO_FILE = os.path.join(DATA_DIR, 'portfolio.json')
JQUANTS_V2     = 'https://api.jquants.com/v2'

# J-Quants 同時リクエスト数を 2 に制限（429 Rate limit 対策）
_jquants_sem = threading.Semaphore(2)

# ============================================================
# スクリーニング条件（固定）
# ============================================================

CRITERIA = {
    'min_yield':      3.0,   # 配当利回り下限 %
    'max_yield':      4.5,   # 配当利回り上限 %
    'min_cap':     1000.0,   # 時価総額下限（億円）
    'max_pbr':        1.0,   # PBR上限（倍）
    'min_roe':        8.0,   # ROE下限 %
    'max_per':       15.0,   # PER上限（倍）
    'min_op_margin': 10.0,   # 営業利益率下限 %
    'min_equity':    40.0,   # 自己資本比率下限 %
    'min_payout':    30.0,   # 配当性向下限 %
    'max_payout':    50.0,   # 配当性向上限 %
}

CRITERIA_LABELS = {
    'dividend_yield': ('配当利回り', '3.0〜4.5%'),
    'market_cap':     ('時価総額',   '1,000億円以上'),
    'pbr':            ('PBR',        '1.0倍以下'),
    'roe':            ('ROE',        '8.0%以上'),
    'per':            ('PER',        '15.0倍以下'),
    'op_margin':      ('営業利益率', '10.0%以上'),
    'equity_ratio':   ('自己資本比率','40%以上'),
    'payout_ratio':   ('配当性向',   '30〜50%'),
}

def check_conditions(m):
    dy  = m.get('dividend_yield') or 0
    mc  = m.get('market_cap_oku') or 0
    pbr = m.get('pbr')
    roe = m.get('roe')
    per = m.get('per')
    op  = m.get('op_margin')
    eq  = m.get('equity_ratio')
    pay = m.get('payout_ratio')

    conds = {
        'dividend_yield': CRITERIA['min_yield'] <= dy <= CRITERIA['max_yield'],
        'market_cap':     mc  >= CRITERIA['min_cap'],
        'pbr':            pbr is not None and pbr <= CRITERIA['max_pbr'],
        'roe':            roe is not None and roe >= CRITERIA['min_roe'],
        'per':            per is not None and per <= CRITERIA['max_per'],
        'op_margin':      op  is not None and op  >= CRITERIA['min_op_margin'],
        'equity_ratio':   eq  is not None and eq  >= CRITERIA['min_equity'],
        'payout_ratio':   pay is not None and CRITERIA['min_payout'] <= pay <= CRITERIA['max_payout'],
    }
    met   = sum(conds.values())
    total = len(conds)
    return conds, met, total

# ============================================================
# 設定・ポートフォリオ
# ============================================================

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_config(data):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_portfolio():
    if os.path.exists(PORTFOLIO_FILE):
        with open(PORTFOLIO_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_portfolio(data):
    with open(PORTFOLIO_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_api_key():
    # 優先順位: 環境変数 → config.json
    key = os.environ.get('JQUANTS_API_KEY', '').strip() or load_config().get('api_key', '').strip()
    if not key:
        raise ValueError('APIキーが未設定です。設定タブで入力してください。')
    return key

# ============================================================
# J-Quants v2 API 共通
# ============================================================

def jq_get_single(endpoint, code5, api_key, timeout=10):
    """
    個別銘柄 1件取得
    - セマフォは1リクエスト単位で確保・解放（sleep中は解放して他スレッドを通す）
    - 429 は最大 2 回リトライ（2s / 5s 待機）
    """
    headers = {'x-api-key': api_key}
    code4 = code5[:4]
    for c in [code5, code4]:
        r = None
        try:
            for wait in (0, 2, 5):  # 0=初回, 2s=1回目retry, 5s=2回目retry
                if wait:
                    time.sleep(wait)  # セマフォ解放済みの状態でsleep
                with _jquants_sem:
                    r = requests.get(f'{JQUANTS_V2}{endpoint}', headers=headers,
                                     params={'code': c}, timeout=timeout)
                if r.status_code != 429:
                    break
            if r and r.ok:
                data = r.json().get('data', [])
                if data:
                    return data
        except Exception:
            pass
    return []

def get_cached(key, fetch_fn, ttl=3600):
    cache_file = os.path.join(CACHE_DIR, f'{key}.json')
    if os.path.exists(cache_file) and (time.time() - os.path.getmtime(cache_file)) < ttl:
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    data = fetch_fn()
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    return data

# ============================================================
# ポートフォリオ用：個別銘柄データを並列取得
# ============================================================

def _fetch_master(code4, code5, api_key):
    cache_master = os.path.join(CACHE_DIR, f'ms_{code4}.json')
    if os.path.exists(cache_master) and (time.time() - os.path.getmtime(cache_master)) < 86400:
        with open(cache_master) as f:
            return json.load(f)
    rows = jq_get_single('/equities/master', code5, api_key, timeout=15)
    master = rows[-1] if rows else {}
    if master:
        with open(cache_master, 'w') as f:
            json.dump(master, f, ensure_ascii=False)
    return master

def _fetch_fins(code4, code5, api_key):
    cache_fins = os.path.join(CACHE_DIR, f'fn_{code4}.json')
    if os.path.exists(cache_fins) and (time.time() - os.path.getmtime(cache_fins)) < 21600:
        with open(cache_fins) as f:
            return json.load(f)
    rows = jq_get_single('/fins/summary', code5, api_key, timeout=15)
    fins = sorted(rows, key=lambda x: x.get('DiscDate', ''))[-1] if rows else {}
    if fins:
        with open(cache_fins, 'w') as f:
            json.dump(fins, f, ensure_ascii=False)
    return fins

def _fetch_one_stock(code4, api_key):
    """1銘柄の master / fins / Yahoo価格 を取得（master+fins+yahoo を同時並列）"""
    code5 = code4 + '0'
    with ThreadPoolExecutor(max_workers=3) as inner:
        f_master = inner.submit(_fetch_master, code4, code5, api_key)
        f_fins   = inner.submit(_fetch_fins,   code4, code5, api_key)
        f_rt     = inner.submit(get_realtime_price, code4)
        master = f_master.result()
        fins   = f_fins.result()
        rt     = f_rt.result()
    return code4, master, fins, rt

def get_portfolio_data_parallel(stocks, api_key):
    """ポートフォリオ全銘柄を並列取得（max 3並列 + セマフォ2で同時J-Quants呼び出し抑制）"""
    codes = list({s.get('code', '')[:4] for s in stocks})
    result = {}
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(_fetch_one_stock, c, api_key): c for c in codes}
        for f in as_completed(futures):
            try:
                code4, master, fins, rt = f.result()
                result[code4] = (master, fins, rt)
            except Exception:
                c = futures[f]
                result[c] = ({}, {}, None)
    return result

# ============================================================
# スクリーニング用：バルク取得（全銘柄 / キャッシュ重視）
# ============================================================

def jq_get_bulk(endpoint, params=None):
    """
    バルク取得（セマフォで同時2リクエスト制限）
    - sleep中はセマフォを解放
    - 429は最大3回リトライ（3s / 8s / 15s 待機）
    """
    api_key = get_api_key()
    headers = {'x-api-key': api_key}
    all_data = []
    p = dict(params or {})
    while True:
        r = None
        for wait in (0, 3, 8, 15):
            if wait:
                time.sleep(wait)
            with _jquants_sem:
                r = requests.get(f'{JQUANTS_V2}{endpoint}', headers=headers, params=p, timeout=30)
            if r.status_code != 429:
                break
        r.raise_for_status()
        body = r.json()
        chunk = body.get('data', [])
        all_data.extend(chunk)
        next_token = body.get('pagination_key') or body.get('next_token')
        if not next_token or not chunk:
            break
        p['pagination_key'] = next_token
    return all_data

def get_master_all():
    def fetch():
        rows = jq_get_bulk('/equities/master')
        latest = {}
        for r in rows:
            code = r.get('Code', '')
            if code not in latest or r.get('Date','') > latest[code].get('Date',''):
                latest[code] = r
        return latest
    return get_cached('master_v2', fetch, ttl=86400)

def get_fins_all():
    """全銘柄の財務サマリーを date パラメータで過去90日分累積取得（キャッシュ24h）"""
    def fetch():
        api_key = get_api_key()
        headers = {'x-api-key': api_key}
        all_fins = {}
        for delta in range(90):
            d = (datetime.now() - timedelta(days=delta)).strftime('%Y-%m-%d')
            try:
                with _jquants_sem:
                    r = requests.get(f'{JQUANTS_V2}/fins/summary',
                                     headers=headers, params={'date': d}, timeout=15)
                    for wait in (5, 10, 20):
                        if r.status_code != 429:
                            break
                        time.sleep(wait)
                        r = requests.get(f'{JQUANTS_V2}/fins/summary',
                                         headers=headers, params={'date': d}, timeout=15)
                if not r.ok:
                    continue
                rows = r.json().get('data', [])
                for row in rows:
                    code = row.get('Code', '')
                    if code not in all_fins or row.get('DiscDate', '') > all_fins[code].get('DiscDate', ''):
                        all_fins[code] = row
                # 上場銘柄全体の7割程度を超えたら十分
                if len(all_fins) >= 3000:
                    break
            except Exception:
                continue
        return all_fins
    return get_cached('fins_v2', fetch, ttl=86400)  # 24h

# ============================================================
# Yahoo Finance リアルタイム価格
# ============================================================

def get_realtime_price(code4):
    """Yahoo Finance から現在株価（15分キャッシュ）"""
    cache_file = os.path.join(CACHE_DIR, f'rt_{code4}.json')
    if os.path.exists(cache_file) and (time.time() - os.path.getmtime(cache_file)) < 900:
        with open(cache_file) as f:
            return json.load(f)
    try:
        url = f'https://finance.yahoo.co.jp/quote/{code4}.T'
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        soup = BeautifulSoup(r.text, 'lxml')
        el = (soup.select_one('span[class*="StyledNumber"]') or
              soup.select_one('span._3rXFvKcD') or
              soup.select_one('span[class*="price"]'))
        if el:
            price = float(el.get_text(strip=True).replace(',', ''))
            result = {'price': price, 'source': 'yahoo', 'date': datetime.now().strftime('%Y-%m-%d')}
            with open(cache_file, 'w') as f:
                json.dump(result, f)
            return result
    except Exception:
        pass
    return None

# ============================================================
# 指標計算
# ============================================================

def _f(v):
    try:
        return float(v) if v not in (None, '', 'NA', '-') else 0.0
    except (TypeError, ValueError):
        return 0.0

def build_metrics_from(code4, master, fins, rt=None, adj_c_fallback=0):
    """個別取得データから指標を計算"""
    eps     = _f(fins.get('EPS'))
    bps     = _f(fins.get('BPS'))
    sales   = _f(fins.get('Sales'))
    op      = _f(fins.get('OP'))
    eq_ar   = _f(fins.get('EqAR'))
    div_ann = _f(fins.get('DivAnn'))
    fdiv    = _f(fins.get('FDivAnn'))
    payout  = _f(fins.get('PayoutRatioAnn'))
    sh_out  = _f(fins.get('ShOutFY'))

    dps = fdiv if fdiv > 0 else div_ann

    # 株価：Yahoo Finance 優先
    if rt and rt.get('price', 0) > 0:
        adj_c = rt['price']
        price_source = 'リアルタイム'
        price_date   = rt.get('date', '')
    else:
        adj_c = adj_c_fallback
        price_source = 'J-Quants(遅延)'
        price_date   = ''

    market_cap_oku = round(adj_c * sh_out / 1e8, 0) if adj_c and sh_out else 0

    m = {
        'code':           code4,
        'name':           master.get('CoName', ''),
        'sector':         master.get('S17Nm', ''),
        'market':         master.get('MktNm', ''),
        'current_price':  adj_c,
        'price_source':   price_source,
        'price_date':     price_date,
        'market_cap_oku': market_cap_oku,
        'dps':            dps,
        'div_ann_raw':    div_ann,
        'fdiv_ann_raw':   fdiv,
        'eps':            eps,
        'bps':            bps,
        'net_sales':      sales,
        'op':             op,
        'disc_date':      fins.get('DiscDate', ''),
        'fy_end':         fins.get('CurFYEn', ''),
    }

    m['dividend_yield'] = round(dps / adj_c * 100, 2) if adj_c > 0 and dps > 0 else 0.0
    m['per']            = round(adj_c / eps,        1) if adj_c > 0 and eps > 0 else None
    m['pbr']            = round(adj_c / bps,        2) if adj_c > 0 and bps > 0 else None
    m['roe']            = round(eps   / bps * 100,  1) if bps   > 0 and eps > 0 else None
    m['op_margin']      = round(op    / sales * 100, 1) if sales > 0              else None
    m['equity_ratio']   = round(eq_ar * 100, 1)         if eq_ar > 0              else None
    m['payout_ratio']   = round(payout * 100, 1)        if payout > 0 else (
                          round(dps / eps * 100,    1)  if eps > 0 and dps > 0 else None)

    conds, met, total = check_conditions(m)
    m['conditions']       = conds
    m['conditions_met']   = met
    m['conditions_total'] = total
    m['all_conditions']   = (met == total)
    return m

def build_metrics(code, master_map, prices_map, fins_map):
    """バルクマップから指標計算（スクリーニング用）"""
    code4 = code[:4]
    code5 = code4 + '0'
    info  = master_map.get(code5) or master_map.get(code4) or {}
    price = prices_map.get(code5) or prices_map.get(code4) or {}
    fins  = fins_map.get(code5)   or fins_map.get(code4)   or {}
    # /prices/daily_quotes は Close, AdjClose などのキーを使う
    adj_c = _f(price.get('AdjClose') or price.get('AdjC') or price.get('Close') or price.get('C'))
    return build_metrics_from(code4, info, fins, rt=None, adj_c_fallback=adj_c)

def score_stock(m):
    score   = 0
    reasons = []
    conds   = m.get('conditions', {})

    dy    = m.get('dividend_yield') or 0
    per   = m.get('per')
    pbr   = m.get('pbr')
    roe   = m.get('roe')
    op_mg = m.get('op_margin')
    eq    = m.get('equity_ratio')
    pay   = m.get('payout_ratio')
    fdiv  = m.get('fdiv_ann_raw') or 0
    ddiv  = m.get('div_ann_raw')  or 0

    if conds.get('dividend_yield'):
        score += 20; reasons.append(('ok',   f'配当利回り良好 {dy}%'))
    elif dy > 4.5:
        score += 10; reasons.append(('warn', f'配当利回り高め {dy}% — 株価下落リスク'))
    elif dy >= 2.5:
        score += 8;  reasons.append(('warn', f'配当利回りやや低め {dy}%'))
    else:
        reasons.append(('ng', f'配当利回り不足 {dy}%'))

    if pbr is not None:
        if   conds.get('pbr'):  score += 15; reasons.append(('ok',   f'PBR割安 {pbr}倍'))
        elif pbr <= 1.5:        score += 7;  reasons.append(('warn', f'PBRやや高め {pbr}倍'))
        else:                                 reasons.append(('ng',   f'PBR割高 {pbr}倍'))

    if roe is not None:
        if   roe >= 10:           score += 15; reasons.append(('ok',   f'ROE優秀 {roe}%'))
        elif conds.get('roe'):    score += 10; reasons.append(('ok',   f'ROE良好 {roe}%'))
        elif roe >= 5:            score += 5;  reasons.append(('warn', f'ROE普通 {roe}%'))
        else:                                   reasons.append(('ng',   f'ROE低い {roe}%'))

    if per is not None:
        if   conds.get('per') and per >= 10: score += 10; reasons.append(('ok',   f'PER適正 {per}倍'))
        elif per < 10:                       score += 7;  reasons.append(('warn', f'PER割安 {per}倍 — 業績確認'))
        elif conds.get('per'):               score += 8;  reasons.append(('ok',   f'PER良好 {per}倍'))
        elif per <= 20:                      score += 5;  reasons.append(('warn', f'PERやや高め {per}倍'))
        else:                                             reasons.append(('ng',   f'PER割高 {per}倍'))

    if op_mg is not None:
        if   conds.get('op_margin'): score += 10; reasons.append(('ok',   f'営業利益率 {op_mg}%'))
        elif op_mg >= 7:             score += 5;  reasons.append(('warn', f'営業利益率普通 {op_mg}%'))
        else:                                      reasons.append(('ng',   f'営業利益率低い {op_mg}%'))

    if eq is not None:
        if   eq >= 60:                  score += 10; reasons.append(('ok',   f'自己資本比率 {eq}%'))
        elif conds.get('equity_ratio'): score += 7;  reasons.append(('ok',   f'自己資本比率良好 {eq}%'))
        elif eq >= 30:                  score += 3;  reasons.append(('warn', f'自己資本比率低め {eq}%'))
        else:                                         reasons.append(('ng',   f'自己資本比率低い {eq}%'))

    if pay is not None:
        if   conds.get('payout_ratio'): score += 10; reasons.append(('ok',   f'配当性向適正 {pay}%'))
        elif pay < 30:                  score += 7;  reasons.append(('warn', f'配当性向低め {pay}% — 増配余地'))
        elif pay <= 70:                 score += 3;  reasons.append(('warn', f'配当性向やや高め {pay}%'))
        else:                                         reasons.append(('ng',   f'配当性向過剰 {pay}% — 減配リスク'))

    if fdiv > 0 and ddiv > 0:
        if fdiv >= ddiv:  score += 10; reasons.append(('ok',   '来期増配予想'))
        else:                           reasons.append(('warn', '来期減配予想'))

    verdict = '保有継続' if score >= 75 else ('要観察' if score >= 50 else '売却検討')
    cls     = 'hold'    if score >= 75 else ('watch'  if score >= 50 else 'sell')
    return {'score': score, 'verdict': verdict, 'verdict_class': cls, 'reasons': reasons}

# ============================================================
# TDnet アラート
# ============================================================

TDNET_KW_DIV = ['配当', '増配', '減配', '無配', '配当予想']
TDNET_KW_REV = ['業績予想の修正', '業績予想修正', '通期業績予想']
TDNET_ALL_KW = TDNET_KW_DIV + TDNET_KW_REV

def fetch_tdnet_alerts(codes):
    alerts = []
    ua = {'User-Agent': 'Mozilla/5.0 (compatible; dividend-screener/1.0)'}
    try:
        r    = requests.get('https://www.release.tdnet.info/inbs/I_list_001_00000000.html',
                            headers=ua, timeout=15)
        r.encoding = 'utf-8'
        soup  = BeautifulSoup(r.text, 'lxml')
        table = soup.find('table', id='main-list-table') or soup.find('table')
        if table:
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if len(cells) < 4:
                    continue
                time_txt    = cells[0].get_text(strip=True)
                code_txt    = cells[1].get_text(strip=True)
                company_txt = cells[2].get_text(strip=True)
                title_txt   = (cells[4] if len(cells) > 4 else cells[3]).get_text(strip=True)
                matched = next((c for c in codes if c in code_txt), None)
                if not matched or not any(kw in title_txt for kw in TDNET_ALL_KW):
                    continue
                tag  = cells[4].find('a') if len(cells) > 4 else cells[3].find('a')
                href = ('https://www.release.tdnet.info' + tag['href']) if tag and tag.get('href') else ''
                atype = 'dividend' if any(kw in title_txt for kw in TDNET_KW_DIV) else 'revision'
                alerts.append({'type': atype, 'time': time_txt, 'code': matched,
                               'company': company_txt, 'title': title_txt, 'url': href, 'source': 'TDnet'})
    except Exception as e:
        alerts.append({'type': 'error', 'title': f'TDnet取得エラー: {e}',
                       'code': '', 'time': '', 'source': 'TDnet'})
    seen, unique = set(), []
    for a in alerts:
        k = (a.get('code',''), a.get('title',''))
        if k not in seen:
            seen.add(k)
            unique.append(a)
    return sorted(unique, key=lambda x: x.get('time',''), reverse=True)[:60]

# ============================================================
# スクリーニング（バルク / キャッシュ重視）
# ============================================================

def run_screening():
    """
    スクリーニング（キャッシュ優先・即時返却版）
    - 新規API呼び出しは行わない（タイムアウト回避）
    - ポートフォリオ更新済みの個別キャッシュ（ms_/fn_/rt_）からのみ読み込む
    - バルクマスターキャッシュ（master_v2.json）があればそれも活用
    """
    # 個別銘柄キャッシュを読み込む
    masters, fins_cache, rt_cache = {}, {}, {}
    for fname in os.listdir(CACHE_DIR):
        fpath = os.path.join(CACHE_DIR, fname)
        try:
            if fname.startswith('ms_') and fname.endswith('.json'):
                code4 = fname[3:-5]
                with open(fpath) as fp:
                    masters[code4] = json.load(fp)
            elif fname.startswith('fn_') and fname.endswith('.json'):
                code4 = fname[3:-5]
                with open(fpath) as fp:
                    fins_cache[code4] = json.load(fp)
            elif fname.startswith('rt_') and fname.endswith('.json'):
                code4 = fname[3:-5]
                with open(fpath) as fp:
                    rt_cache[code4] = json.load(fp)
        except Exception:
            pass

    # バルクマスターキャッシュがあれば masters に追加（sector/name 情報補完）
    bulk_cache = os.path.join(CACHE_DIR, 'master_v2.json')
    if os.path.exists(bulk_cache):
        try:
            with open(bulk_cache) as fp:
                bulk = json.load(fp)
            for code5, info in bulk.items():
                code4 = code5[:4]
                if code4 not in masters:
                    masters[code4] = info
        except Exception:
            pass

    if not masters:
        return []  # キャッシュが空（ポートフォリオ更新前）

    results = []
    for code4, info in masters.items():
        fins = fins_cache.get(code4) or {}
        rt   = rt_cache.get(code4)
        try:
            m = build_metrics_from(code4, info, fins, rt=rt)
            dy = m.get('dividend_yield') or 0
            mc = m.get('market_cap_oku') or 0
            if dy < 2.5 or mc < 250:
                continue
            m.update(score_stock(m))
            results.append(m)
        except Exception:
            continue

    return sorted(results,
                  key=lambda x: (x.get('all_conditions', False),
                                 x.get('conditions_met', 0),
                                 x.get('score', 0)),
                  reverse=True)

# ============================================================
# 入替推奨（年間配当比較・含み益除外）
# ============================================================

def get_recommendations(portfolio_metrics, screen_results):
    recs = []
    pool = [s for s in screen_results if s.get('conditions_met', 0) >= 5]

    for pm in portfolio_metrics:
        if pm.get('error'):
            continue

        pf_met   = pm.get('conditions_met', 0)
        pf_cls   = pm.get('verdict_class', '')
        avg_p    = pm.get('avg_price') or 0
        cur_p    = pm.get('current_price') or 0
        shares   = pm.get('shares') or 0
        dps      = pm.get('dps') or 0

        # ① 50%以上含み益の銘柄は除外
        gain_pct = ((cur_p - avg_p) / avg_p * 100) if avg_p > 0 else 0
        if gain_pct >= 50:
            continue

        # ② 取得時の想定利回りが高く、値上がりで利回りが低下したものは除外
        #    （配当は変わっていないが株価上昇で利回りが相対的に低下）
        yield_at_cost = (dps / avg_p * 100) if avg_p > 0 else 0
        cur_yield     = pm.get('dividend_yield') or 0
        if yield_at_cost >= 3.0 and cur_yield < 2.5 and gain_pct >= 20:
            continue

        # ③ 保有継続かつ条件7以上はスキップ
        if pf_cls == 'hold' and pf_met >= 7:
            continue

        sector = pm.get('sector', '')
        code   = pm.get('code', '')

        # 現在の年間配当収入
        annual_div_now = round(dps * shares * 0.8, 0) if dps and shares else 0
        # 売却想定金額（現在値 × 株数）
        sell_amount = cur_p * shares if cur_p and shares else avg_p * shares

        same_sec = [s for s in pool if s.get('sector') == sector and s.get('code') != code]
        candidates_raw = sorted(
            same_sec or [s for s in pool if s.get('code') != code],
            key=lambda x: (x.get('conditions_met', 0), x.get('score', 0)),
            reverse=True
        )[:5]

        # 候補ごとに年間配当収入を試算
        candidates = []
        for c in candidates_raw:
            c_price = c.get('current_price') or 0
            c_dps   = c.get('dps') or 0
            if c_price > 0 and sell_amount > 0:
                c_shares_est  = sell_amount / c_price
                c_annual_div  = round(c_dps * c_shares_est * 0.8, 0)
                div_change    = round(c_annual_div - annual_div_now, 0)
                div_change_pct = round((c_annual_div - annual_div_now) / annual_div_now * 100, 1) if annual_div_now > 0 else 0
            else:
                c_shares_est, c_annual_div, div_change, div_change_pct = 0, 0, 0, 0

            cand = {k: c.get(k) for k in ['code','name','sector','dividend_yield',
                                           'conditions_met','score','verdict','verdict_class',
                                           'per','pbr','roe','payout_ratio','equity_ratio',
                                           'market_cap_oku','current_price','dps']}
            cand.update({
                'est_shares':       round(c_shares_est, 0),
                'est_annual_div':   c_annual_div,
                'div_change':       div_change,
                'div_change_pct':   div_change_pct,
            })
            candidates.append(cand)

        # 条件合致数が現在より多い候補に絞る
        candidates = [c for c in candidates if (c.get('conditions_met') or 0) > pf_met][:3]
        if not candidates:
            continue

        best_met = candidates[0].get('conditions_met', 0)
        recs.append({
            'current': {k: pm.get(k) for k in ['code','name','sector','dividend_yield',
                                                 'conditions_met','score','verdict','verdict_class',
                                                 'per','pbr','roe','payout_ratio','equity_ratio',
                                                 'avg_price','current_price','shares','dps']},
            'current_annual_div': annual_div_now,
            'gain_pct':           round(gain_pct, 1),
            'yield_at_cost':      round(yield_at_cost, 2),
            'sell_amount':        round(sell_amount, 0),
            'replacements':       candidates,
            'gain_conditions':    best_met - pf_met,
        })

    return sorted(recs, key=lambda x: x['gain_conditions'], reverse=True)

# ============================================================
# SBI CSV パーサー
# ============================================================

def parse_sbi_csv(raw_bytes):
    for enc in ('cp932', 'utf-8-sig', 'utf-8'):
        try:
            content = raw_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        content = raw_bytes.decode('cp932', errors='replace')

    lines = content.splitlines()
    CODE_COLS = ['証券コード', 'コード', '銘柄コード', 'code', 'Code']

    header_idx = next(
        (i for i, l in enumerate(lines) if any(c in l for c in CODE_COLS)), None
    )
    if header_idx is None:
        return None, '証券コード列が見つかりません'

    reader = csv.DictReader(io.StringIO('\n'.join(lines[header_idx:])))
    result = []
    for row in reader:
        code = ''
        for col in CODE_COLS:
            val = str(row.get(col, '')).strip().strip('"')
            if re.match(r'^[0-9A-Za-z]{4}$', val):
                code = val.upper()
                break
        if not code:
            continue
        name       = str(row.get('銘柄名称', row.get('銘柄名', row.get('銘柄', '')))).strip().strip('"')
        shares_raw = str(row.get('保有株数', row.get('保有数量', row.get('数量', '0')))).replace(',','').strip()
        avg_raw    = str(row.get('取得単価', row.get('平均取得単価', '0'))).replace(',','').strip()
        try:
            shares = int(float(shares_raw)) if shares_raw else 0
            avg    = float(avg_raw)          if avg_raw    else 0.0
        except Exception:
            shares, avg = 0, 0.0
        if shares <= 0:
            continue
        result.append({'code': code, 'name': name, 'shares': shares, 'avg_price': avg, 'memo': ''})

    return result, None

# ============================================================
# Flask ルーティング
# ============================================================

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/manifest.json')
def manifest():
    return render_template('manifest.json'), 200, {'Content-Type': 'application/manifest+json'}

@app.route('/sw.js')
def service_worker():
    sw = """
const CACHE = 'dividend-screener-v1';
self.addEventListener('install', e => { self.skipWaiting(); });
self.addEventListener('activate', e => { self.clients.claim(); });
self.addEventListener('fetch', e => {
  if (e.request.url.includes('/api/')) return;
  e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
});
"""
    return sw, 200, {'Content-Type': 'application/javascript'}

@app.route('/api/config', methods=['GET', 'POST'])
def api_config():
    if request.method == 'POST':
        data = request.get_json() or {}
        save_config({'api_key': data.get('api_key', '').strip()})
        return jsonify({'ok': True})
    cfg = load_config()
    env_key = os.environ.get('JQUANTS_API_KEY', '').strip()
    cfg_key = cfg.get('api_key', '').strip()
    active_key = env_key or cfg_key
    return jsonify({
        'configured':   bool(active_key),
        'api_key_hint': active_key[:6] + '...' if active_key else ''
    })

@app.route('/api/test_connection')
def api_test():
    try:
        key = get_api_key()
        r   = requests.get(f'{JQUANTS_V2}/equities/master',
                           headers={'x-api-key': key}, params={'code': '86970'}, timeout=10)
        r.raise_for_status()
        info = (r.json().get('data') or [{}])[-1]
        return jsonify({'ok': True, 'message': f'接続成功: {info.get("CoName","") or "J-Quants"} のデータを取得できました'})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 400

@app.route('/api/cache/clear', methods=['POST'])
def api_cache_clear():
    """空またはすべてのキャッシュを削除"""
    mode = (request.get_json() or {}).get('mode', 'empty')
    removed = 0
    for f in os.listdir(CACHE_DIR):
        fpath = os.path.join(CACHE_DIR, f)
        if mode == 'all':
            os.remove(fpath); removed += 1
        elif mode == 'empty':
            try:
                with open(fpath) as fp:
                    d = json.load(fp)
                if not d:
                    os.remove(fpath); removed += 1
            except Exception:
                os.remove(fpath); removed += 1
    return jsonify({'ok': True, 'removed': removed})

@app.route('/api/debug/stock')
def api_debug_stock():
    """デバッグ用: 特定銘柄のJ-Quantsレスポンスを確認"""
    code = request.args.get('code', '1808')
    code4 = code[:4]
    code5 = code4 + '0'
    try:
        key = get_api_key()
        key_hint = key[:6] + '...'
        # master
        r_m = requests.get(f'{JQUANTS_V2}/equities/master',
                           headers={'x-api-key': key}, params={'code': code5}, timeout=10)
        # fins
        r_f = requests.get(f'{JQUANTS_V2}/fins/summary',
                           headers={'x-api-key': key}, params={'code': code5}, timeout=10)
        return jsonify({
            'key_hint': key_hint,
            'master_status': r_m.status_code,
            'master_data': r_m.json() if r_m.ok else r_m.text[:500],
            'fins_status': r_f.status_code,
            'fins_data': (r_f.json().get('data') or [])[-1] if r_f.ok else r_f.text[:500],
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/criteria')
def api_criteria():
    return jsonify({'criteria': CRITERIA, 'labels': CRITERIA_LABELS})

@app.route('/api/portfolio', methods=['GET'])
def api_portfolio_get():
    return jsonify(load_portfolio())

@app.route('/api/portfolio', methods=['POST'])
def api_portfolio_save():
    save_portfolio(request.get_json() or [])
    return jsonify({'ok': True})

def _calc_portfolio_metrics():
    """ポートフォリオ指標を計算して返す（内部共通ロジック）"""
    portfolio = load_portfolio()
    if not portfolio:
        return []
    api_key = get_api_key()
    stock_data = get_portfolio_data_parallel(portfolio, api_key)
    results = []
    for stock in portfolio:
        code  = stock.get('code', '')
        code4 = code[:4]
        try:
            master, fins, rt = stock_data.get(code4, ({}, {}, None))
            m = build_metrics_from(code4, master, fins, rt=rt)
            m['shares']    = stock.get('shares', 0)
            m['avg_price'] = stock.get('avg_price', 0)
            m['memo']      = stock.get('memo', '')
            if stock.get('name') and not m.get('name'):
                m['name'] = stock['name']
            cp  = m.get('current_price') or 0
            ap  = m.get('avg_price')     or 0
            sh  = m.get('shares')        or 0
            dps = m.get('dps')           or 0
            if cp and ap and sh:
                m['unrealized_pnl']     = round((cp - ap) * sh, 0)
                m['unrealized_pnl_pct'] = round((cp - ap) / ap * 100, 1)
            if dps and sh:
                m['annual_dividend']    = round(dps * sh * 0.8, 0)
            if dps and ap:
                m['yield_at_cost']      = round(dps / ap * 100, 2)
            m.update(score_stock(m))
            results.append(m)
        except Exception as e:
            results.append({'code': code, 'name': stock.get('name', ''),
                            'error': str(e), **stock})
    return results

@app.route('/api/portfolio/metrics')
def api_portfolio_metrics():
    return jsonify(_calc_portfolio_metrics())

@app.route('/api/alerts')
def api_alerts():
    portfolio = load_portfolio()
    codes = [s.get('code','') for s in portfolio]
    return jsonify(fetch_tdnet_alerts(codes) if codes else [])

@app.route('/api/screen')
def api_screen():
    try:
        results = run_screening()
        if not results:
            return jsonify({'warning': 'まず「保有銘柄」タブで「データ更新」を実行してください。キャッシュが空です。', 'data': []})
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/screen/prefetch', methods=['POST'])
def api_screen_prefetch():
    """バルク銘柄マスター＋fins を date別に取得してキャッシュ（バックグラウンド向け）"""
    def do_fetch():
        try:
            get_master_all()   # master_v2.json にキャッシュ
        except Exception:
            pass
        try:
            get_fins_all()     # fins_v2.json にキャッシュ
        except Exception:
            pass
    t = threading.Thread(target=do_fetch, daemon=True)
    t.start()
    return jsonify({'ok': True, 'message': 'バックグラウンドで取得開始しました（完了まで数分かかります）'})

@app.route('/api/recommendations')
def api_recommendations():
    try:
        pf_metrics = _calc_portfolio_metrics()
        if not pf_metrics:
            return jsonify([])
        screen_results = run_screening()
        return jsonify(get_recommendations(pf_metrics, screen_results))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/import/csv', methods=['POST'])
def api_import_csv():
    try:
        file = request.files.get('file')
        if not file:
            return jsonify({'ok': False, 'message': 'ファイルが選択されていません'}), 400
        result, err = parse_sbi_csv(file.read())
        if err:
            return jsonify({'ok': False, 'message': err}), 400
        if not result:
            return jsonify({'ok': False, 'message': '株式銘柄が見つかりませんでした（投資信託は対象外です）'}), 400
        existing = load_portfolio()
        existing_map = {s['code']: s for s in existing}
        for r in result:
            existing_map[r['code']] = r
        merged = list(existing_map.values())
        save_portfolio(merged)
        return jsonify({'ok': True, 'imported': len(result), 'total': len(merged)})
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500

@app.route('/api/clear_cache', methods=['POST'])
def api_clear_cache():
    for f in os.listdir(CACHE_DIR):
        try:
            os.remove(os.path.join(CACHE_DIR, f))
        except Exception:
            pass
    return jsonify({'ok': True, 'message': 'キャッシュをクリアしました'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    host = os.environ.get('HOST', '0.0.0.0')   # LAN/クラウド両対応
    print('=' * 55)
    print(f'  高配当株スクリーニングアプリ (J-Quants v2)')
    print(f'  http://localhost:{port} を開いてください')
    print('=' * 55)
    app.run(debug=False, port=port, host=host)
