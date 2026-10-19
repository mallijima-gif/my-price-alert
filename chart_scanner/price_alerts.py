"""Durable, one-shot price alerts. No AI or order API is called here."""
from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


SOURCES = {
    'kiwoom': ('국장 · 키움 · KRW', 'KR', 'KRW'),
    'naver': ('국장 · 네이버 · KRW', 'KR', 'KRW'),
    'yahoo': ('미장 · Yahoo 정규장 · USD', 'US', 'USD'),
    'binance': ('코인 · 바이낸스 현물 · USDT', 'CRYPTO', 'USDT'),
    'binance_usdc': ('코인 · 바이낸스 현물 · USDC', 'CRYPTO', 'USDC'),
    'binance_futures': ('코인 · 바이낸스 선물 · USDT', 'CRYPTO', 'USDT'),
    'bithumb': ('코인 · 빗썸 · KRW', 'CRYPTO', 'KRW'),
    'coinone': ('코인 · 코인원 · KRW', 'CRYPTO', 'KRW'),
}


def positive(value):
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError('목표가는 양수로 입력하세요.') from None
    if not number.is_finite() or not Decimal('0') < number <= Decimal('1000000000000000'):
        raise ValueError('목표가는 유한한 양수로 입력하세요.')
    if number.as_tuple().exponent < -12:
        raise ValueError('소수점은 12자리까지 입력할 수 있습니다.')
    return format(number, 'f')


def read_json(url, body=None):
    raw = None if body is None else json.dumps(body).encode('utf-8')
    request = Request(url, data=raw, headers={'User-Agent': 'TradingRoom/1.0', 'Content-Type': 'application/json'})
    with urlopen(request, timeout=8) as response:
        return json.load(response)


class PriceFeeds:
    def __init__(self):
        self.kiwoom = None
        self.kr_lock = threading.Lock()

    def quote(self, source, symbol):
        if source in ('binance', 'binance_usdc', 'binance_futures'):
            base = 'https://fapi.binance.com/fapi/v1' if source == 'binance_futures' else 'https://data-api.binance.vision/api/v3'
            row = read_json(base + '/trades?' + urlencode({'symbol': symbol, 'limit': 1}))[-1]
            price, stamp = row['price'], int(row['time']) / 1000
        elif source == 'bithumb':
            row = read_json('https://api.bithumb.com/v1/ticker?' + urlencode({'markets': 'KRW-' + symbol}))[0]
            price = row['trade_price']
            stamp = datetime.strptime(row['trade_date'] + row['trade_time'], '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc).timestamp()
        elif source == 'coinone':
            row = read_json('https://api.coinone.co.kr/public/v2/ticker_new/KRW/' + symbol)['tickers'][0]
            price, stamp = row['last'], int(row['timestamp']) / 1000
        elif source == 'yahoo':
            row = read_json('https://query1.finance.yahoo.com/v8/finance/chart/' + quote(symbol.replace('.', '-'), safe='') + '?interval=1d&range=5d')['chart']['result'][0]['meta']
            if row.get('currency') != 'USD' or row.get('instrumentType') not in ('EQUITY', 'ETF'):
                raise ValueError('미국 주식·ETF의 USD 시세만 지원합니다.')
            price, stamp = row['regularMarketPrice'], row['regularMarketTime']
        elif source == 'naver':
            row = read_json('https://polling.finance.naver.com/api/realtime/domestic/stock/' + symbol)['datas'][0]
            if row.get('itemCode') != symbol or row.get('currencyType', {}).get('code') != 'KRW':
                raise ValueError('네이버 종목·통화 불일치')
            price, stamp = row['closePriceRaw'], datetime.fromisoformat(row['localTradedAt']).timestamp()
        elif source == 'kiwoom':
            from .providers import KiwoomProvider
            with self.kr_lock:
                self.kiwoom = self.kiwoom or KiwoomProvider()
                try:
                    row = self.kiwoom._page('ka10079', '/api/dostk/chart', {'stk_cd': symbol, 'tic_scope': '1', 'upd_stkpc_tp': '0'})[0]['stk_tic_chart_qry'][0]
                except Exception:
                    self.kiwoom._token = ''
                    raise
                price = abs(Decimal(str(row['cur_prc']).replace(',', '')))
                stamp = datetime.strptime(row['cntr_tm'], '%Y%m%d%H%M%S').replace(tzinfo=ZoneInfo('Asia/Seoul')).timestamp()
        else:
            raise ValueError('지원하지 않는 가격 출처입니다.')
        return {'price': positive(price), 'source_ts': float(stamp), 'source': source, 'currency': SOURCES[source][2]}


class TelegramFailure(Exception):
    def __init__(self, message, uncertain=False, retry_after=300):
        super().__init__(message)
        self.uncertain, self.retry_after = uncertain, retry_after


class Telegram:
    def call(self, config, method, body=None, sending=False):
        try:
            data = read_json('https://api.telegram.org/bot' + config['token'] + '/' + method, body or {})
        except HTTPError as exc:
            delay = 300
            if exc.code == 429:
                try:
                    delay = min(3600, max(2, int(json.load(exc).get('parameters', {}).get('retry_after', 60)) + 1))
                except Exception:
                    pass
            exc.close()
            raise TelegramFailure(f'텔레그램 요청 거절 ({exc.code})', uncertain=sending and exc.code>=500, retry_after=delay) from None
        except Exception:
            # The server might have accepted a timed-out send. Never replay automatically.
            raise TelegramFailure('텔레그램 응답을 확인하지 못했습니다.', uncertain=sending) from None
        if data.get('ok') is not True:
            raise TelegramFailure('텔레그램에서 요청을 거절했습니다.')
        return data['result']

    def verify(self, token, chat_id):
        config = {'token': token, 'chat_id': chat_id}
        bot = self.call(config, 'getMe')
        chat = self.call(config, 'getChat', {'chat_id': chat_id})
        if not bot.get('is_bot') or str(chat.get('id')) != chat_id:
            raise TelegramFailure('봇과 수신 채팅을 확인하세요.')
        return {'bot_name': bot.get('username', ''), 'verified_at': time.time()}

    def chats(self, token):
        config = {'token': token}
        bot = self.call(config, 'getMe')
        if not bot.get('is_bot'):
            raise TelegramFailure('봇 토큰을 확인하세요.')
        if self.call(config, 'getWebhookInfo').get('url'):
            raise TelegramFailure('기존 봇이 웹훅을 사용 중입니다. 기존 설정의 Chat ID를 직접 입력하세요.')
        # Omit offset and allowed_updates: do not acknowledge pending commands
        # or change the old worker's subscription. Never return message bodies.
        updates = self.call(config, 'getUpdates', {'limit': 100, 'timeout': 0})
        chats = {}
        for update in updates:
            message = update.get('message') or update.get('channel_post') or update.get('edited_message') or {}
            chat = message.get('chat', {})
            chat_id = str(chat.get('id', ''))
            if not re.fullmatch(r'-?[0-9]{5,20}', chat_id):
                continue
            name = chat.get('title') or ' '.join(str(chat.get(k, '')) for k in ('first_name', 'last_name')).strip() or chat.get('username') or '수신 채팅'
            chats[chat_id] = {'id': chat_id, 'name': str(name)[:100], 'type': str(chat.get('type', ''))[:20]}
        return {'bot_name': bot.get('username', ''), 'chats': list(chats.values())}

    def send(self, config, text):
        return self.call(config, 'sendMessage', {'chat_id': config['chat_id'], 'text': text}, sending=True)['message_id']


class PriceAlerts:
    def __init__(self, directory, feeds=None, telegram=None, vault=None, clock=time.time, load_cloud=True):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / 'alerts.sqlite3'
        if vault is None:
            from engine.bot.vault import Vault
            vault = Vault(self.directory / 'credentials')
        self.vault = vault
        self.feeds, self.telegram, self.clock = feeds or PriceFeeds(), telegram or Telegram(), clock
        self.run_lock, self.config_lock = threading.Lock(), threading.RLock()
        self.stop_event, self.wake = threading.Event(), threading.Event()
        self.thread, self.instance = None, None
        self.quote_cache = {}
        self.last_send = 0
        self.cloud = None
        self.remote_worker = False
        self.checkpoint = None
        self.load_cloud = load_cloud
        with self.connect() as db:
            db.executescript('''
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS alerts(
                    id TEXT PRIMARY KEY,request_id TEXT UNIQUE NOT NULL,source TEXT NOT NULL,symbol TEXT NOT NULL,
                    name TEXT NOT NULL,target TEXT NOT NULL,condition TEXT NOT NULL,channel TEXT NOT NULL,
                    status TEXT NOT NULL,version INTEGER NOT NULL DEFAULT 1,review_required INTEGER NOT NULL DEFAULT 0,
                    legacy_symbol TEXT NOT NULL DEFAULT '',last_price TEXT,source_ts REAL,last_checked REAL,
                    error TEXT NOT NULL DEFAULT '',created_at REAL NOT NULL,updated_at REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS alerts_status ON alerts(status,created_at);
                CREATE TABLE IF NOT EXISTS deliveries(
                    id TEXT PRIMARY KEY,alert_id TEXT UNIQUE,channel TEXT NOT NULL,body TEXT NOT NULL,
                    status TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,next_try REAL NOT NULL DEFAULT 0,
                    error TEXT NOT NULL DEFAULT '',message_id TEXT NOT NULL DEFAULT '',is_test INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,updated_at REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS deliveries_queue ON deliveries(status,next_try);
                CREATE TABLE IF NOT EXISTS imports(fingerprint TEXT PRIMARY KEY,alert_id TEXT NOT NULL);
            ''')
        if load_cloud:
            with self.connect() as db:
                cloud_mode = self._config(db, 'cloud_mode', False)
            if cloud_mode and self.vault.exists('cloud-github'):
                from .alert_cloud import CloudAlerts
                self.cloud = CloudAlerts(self)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def _config(self, db, key, default=None):
        row = db.execute('SELECT value FROM config WHERE key=?', (key,)).fetchone()
        return json.loads(row['value']) if row else default

    def _put(self, db, key, value):
        db.execute('INSERT INTO config VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, json.dumps(value)))

    def overview(self):
        with self.connect() as db:
            alerts = [dict(r) for r in db.execute("SELECT * FROM alerts ORDER BY CASE status WHEN 'triggered' THEN 0 WHEN 'active' THEN 1 WHEN 'paused' THEN 2 WHEN 'sent' THEN 3 ELSE 4 END,created_at DESC LIMIT 300")]
            logs = [dict(r) for r in db.execute('SELECT id,alert_id,channel,body,status,error,created_at,updated_at,is_test FROM deliveries ORDER BY created_at DESC,rowid DESC LIMIT 100')]
            configured = self.vault.exists('telegram')
            telegram = {'configured': configured, 'enabled': self._config(db, 'telegram_enabled', False),
                        'legacy_confirmed': self._config(db, 'legacy_confirmed', False),
                        'verification_pending': self._config(db, 'telegram_verification_pending', False),
                        **self._config(db, 'telegram_info', {})}
            worker = {'running': bool(self.thread and self.thread.is_alive()), 'last_check': self._config(db, 'last_check'),
                      'error': self._config(db, 'worker_error', ''), 'interval': 30}
            missing_cloud = self.load_cloud and self._config(db, 'cloud_mode', False) and not self.cloud
        result = {'alerts': alerts, 'logs': logs, 'telegram': telegram, 'worker': worker,
                'sources': [{'id': key, 'label': val[0], 'market': val[1], 'currency': val[2]} for key, val in SOURCES.items()]}
        if missing_cloud:
            result['cloud'] = {'enabled': True, 'last_check': None, 'stale': True, 'interval': 300,
                               'error': '클라우드 연결 키를 확인하세요. PC에서 대신 발송하지 않습니다.'}
        return self.cloud.overview(result) if self.cloud else result

    def configure_telegram(self, payload):
        if self.cloud:
            return self.cloud.configure_telegram(payload)
        token = str(payload.get('token', '')).strip()
        chat_id = str(payload.get('chat_id', '')).strip() or (self.vault.get('telegram') or {}).get('chat_id', '')
        if not re.fullmatch(r'[0-9]{5,20}:[A-Za-z0-9_-]{25,120}', token) or not re.fullmatch(r'-?[0-9]{5,20}', chat_id):
            raise ValueError('봇 토큰과 숫자 Chat ID를 확인하세요.')
        with self.config_lock:
            try:
                info = self.telegram.verify(token, chat_id)
            except TelegramFailure as exc:
                raise ValueError(str(exc)) from None
            self.vault.put('telegram', {'token': token, 'chat_id': chat_id})
            with self.connect() as db:
                self._put(db, 'telegram_info', info)
                self._put(db, 'telegram_verification_pending', False)
                self._put(db, 'telegram_enabled', False)
                self._put(db, 'legacy_confirmed', False)
        return {'message': '봇·수신 채팅 확인 후 암호화 저장했습니다. 테스트 발송과 이전 실행 중지를 확인한 뒤 알림을 켜세요.'}

    def stage_telegram(self, payload):
        with self.config_lock, self.connect() as db:
            self._put(db, 'telegram_enabled', False)
            self._put(db, 'legacy_confirmed', False)
            self._put(db, 'telegram_verification_pending', True)
            self._put(db, 'worker_error', '')
        return {'message': '새 봇 연결을 암호화 저장했습니다. 클라우드에서 봇·수신 채팅을 확인한 뒤 테스트와 가격 알림을 켜세요.'}

    def find_telegram_chats(self, payload):
        token = str(payload.get('token', '')).strip()
        if not re.fullmatch(r'[0-9]{5,20}:[A-Za-z0-9_-]{25,120}', token):
            raise ValueError('기존 봇 토큰을 먼저 입력하세요.')
        with self.config_lock:
            try:
                return self.telegram.chats(token)
            except TelegramFailure as exc:
                raise ValueError(str(exc)) from None

    def telegram_mode(self, payload):
        if self.cloud:
            return self.cloud.mutate('telegram_mode', payload)
        enabled = payload.get('enabled')
        if type(enabled) is not bool:
            raise ValueError('알림 사용 여부를 확인하세요.')
        with self.config_lock, self.connect() as db:
            if enabled:
                if self._config(db, 'telegram_verification_pending', False):
                    raise ValueError('클라우드에서 봇·수신 채팅 확인을 마친 뒤 테스트하세요.')
                if not self.vault.exists('telegram') or payload.get('legacy_confirmed') is not True:
                    raise ValueError('텔레그램 연결과 기존 알람 실행 중지를 먼저 확인하세요.')
                verified = self._config(db,'telegram_info',{}).get('verified_at',0)
                if not db.execute("SELECT 1 FROM deliveries WHERE is_test=1 AND status='sent' AND updated_at>=?",(verified,)).fetchone():
                    raise ValueError('테스트 알림을 보내 실제 발송 성공을 먼저 확인하세요.')
                self._put(db, 'legacy_confirmed', True)
            if enabled and not self._config(db, 'telegram_enabled', False):
                # Re-arm after a deliberate monitoring pause; do not use prices from before it.
                db.execute("UPDATE alerts SET last_price=NULL,source_ts=NULL,last_checked=NULL,error='',version=version+1 WHERE channel='telegram' AND condition='touch' AND status='active'")
            self._put(db, 'telegram_enabled', enabled)
        self.wake.set()
        return {'message': '텔레그램 가격 알림을 켰습니다.' if enabled else '텔레그램 발송을 중지했습니다. 알림은 보존됩니다.'}

    def save(self, payload):
        if self.cloud and (payload.get('channel') == 'telegram' or self.cloud.owns(payload.get('id'))):
            return self.cloud.mutate('save', payload)
        source, symbol = payload.get('source'), str(payload.get('symbol', '')).strip().upper()
        if source not in SOURCES:
            raise ValueError('가격 출처를 선택하세요.')
        if source in ('kiwoom', 'naver'):
            valid = re.fullmatch(r'[0-9]{6}', symbol)
        elif source in ('bithumb', 'coinone'):
            valid = re.fullmatch(r'[A-Z0-9]{1,20}', symbol)
        elif source.startswith('binance'):
            valid = re.fullmatch(r'[A-Z0-9]{1,30}', symbol) and symbol.endswith('USDC' if source == 'binance_usdc' else 'USDT')
        else:
            valid = re.fullmatch(r'[A-Z0-9][A-Z0-9.-]{0,19}', symbol)
        if not valid:
            raise ValueError('선택한 가격 출처의 종목 코드를 확인하세요.')
        target, condition, channel = positive(payload.get('target')), payload.get('condition', 'touch'), payload.get('channel')
        if condition not in ('touch', 'above', 'below') or channel not in ('screen', 'telegram'):
            raise ValueError('가격 도달·이상·이하 조건과 알림 수단을 선택하세요.')
        request_id = str(payload.get('request_id', ''))
        if not re.fullmatch(r'[A-Za-z0-9_-]{8,80}', request_id):
            raise ValueError('요청 번호를 확인하세요.')
        now, alert_id = self.clock(), payload.get('id')
        status = 'active' if payload.get('active', True) is True else 'paused'
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            if not alert_id:
                prior = db.execute('SELECT * FROM alerts WHERE request_id=?', (request_id,)).fetchone()
                if prior:
                    if any(prior[key]!=value for key,value in {'source':source,'symbol':symbol,'target':target,'condition':condition,'channel':channel}.items()):
                        raise ValueError('이전 저장 요청의 내용이 다릅니다. 목록에서 처리 결과를 확인하고 새 알림 창을 여세요.')
                    return {'id': prior['id'], 'message': '이미 등록한 알림입니다.'}
            if channel == 'telegram' and not self.vault.exists('telegram'):
                raise ValueError('연결·설정에서 텔레그램 봇을 먼저 연결하세요.')
            name = str(payload.get('name', '')).strip()[:80] or symbol
            if alert_id:
                updated = db.execute('''UPDATE alerts SET source=?,symbol=?,name=?,target=?,condition=?,channel=?,
                    status=?,version=version+1,review_required=0,last_price=NULL,source_ts=NULL,last_checked=NULL,error='',updated_at=?
                    WHERE id=? AND version=? AND status IN ('active','paused')''',
                    (source,symbol,name,target,condition,channel,status,now,alert_id,payload.get('version')))
                if not updated.rowcount:
                    raise ValueError('알림 상태가 변경됐습니다. 목록을 새로 확인하세요.')
            else:
                if db.execute("SELECT COUNT(*) FROM alerts WHERE status IN ('active','paused','triggered')").fetchone()[0] >= 200:
                    raise ValueError('가격 알림은 200개까지 등록할 수 있습니다.')
                alert_id = uuid.uuid4().hex
                db.execute('''INSERT INTO alerts(id,request_id,source,symbol,name,target,condition,channel,status,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)''', (alert_id,request_id,source,symbol,name,target,condition,channel,status,now,now))
        self.wake.set()
        return {'id': alert_id, 'message': '가격 알림을 저장했습니다.'}

    def action(self, payload):
        if self.cloud and self.cloud.owns(payload.get('id')):
            return self.cloud.mutate('action', payload)
        alert_id, action, now = payload.get('id'), payload.get('action'), self.clock()
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM alerts WHERE id=?', (alert_id,)).fetchone()
            if not row:
                raise ValueError('알림을 찾을 수 없습니다.')
            if action in ('pause', 'resume'):
                expected, new = ('active','paused') if action == 'pause' else ('paused','active')
                if row['status'] != expected or (action == 'resume' and row['review_required']):
                    raise ValueError('수정 화면에서 가격 출처·통화를 확인하고 저장하세요.')
            elif action == 'archive':
                if db.execute("SELECT 1 FROM deliveries WHERE alert_id=? AND status='sending'",(alert_id,)).fetchone():
                    raise ValueError('전송 중입니다. 발송 결과를 확인한 뒤 보관하세요.')
                new = 'archived'
                db.execute("UPDATE deliveries SET status='cancelled',updated_at=? WHERE alert_id=? AND status IN ('pending','failed')",(now,alert_id))
            elif action == 'restore' and row['status'] == 'archived':
                if db.execute('SELECT 1 FROM deliveries WHERE alert_id=?',(alert_id,)).fetchone():
                    raise ValueError('발동한 알림은 기록으로 보존됩니다. 새 알림으로 등록하세요.')
                new = 'paused'
            else:
                raise ValueError('지원하지 않는 변경입니다.')
            if action == 'resume' and row['condition'] == 'touch':
                db.execute("UPDATE alerts SET last_price=NULL,source_ts=NULL,last_checked=NULL,error='' WHERE id=?",(alert_id,))
            db.execute('UPDATE alerts SET status=?,version=version+1,updated_at=? WHERE id=?',(new,now,alert_id))
        self.wake.set()
        return {'message': '알림 상태를 변경했습니다.'}

    def retry(self, payload):
        if self.cloud and self.cloud.owns(payload.get('id'), delivery=True):
            return self.cloud.mutate('retry', payload)
        if payload.get('confirm_duplicate') is not True:
            raise ValueError('텔레그램에서 기존 발송 여부를 확인하세요. 재전송하면 중복될 수 있습니다.')
        with self.connect() as db:
            row = db.execute('SELECT * FROM deliveries WHERE id=?',(payload.get('id'),)).fetchone()
            if not row or row['status'] not in ('uncertain','failed'):
                raise ValueError('재전송할 발송 기록을 확인하세요.')
            db.execute("UPDATE deliveries SET status='pending',next_try=0,error='',updated_at=? WHERE id=? AND status IN ('uncertain','failed')",(self.clock(),row['id']))
        self.wake.set()
        return {'message': '확인한 알림을 다시 전송 대기합니다.'}

    def test_telegram(self, payload):
        if self.cloud:
            return self.cloud.mutate('test_telegram', payload)
        request_id = str(payload.get('request_id',''))
        if not re.fullmatch(r'[A-Za-z0-9_-]{8,80}',request_id) or not self.vault.exists('telegram'):
            raise ValueError('텔레그램 연결과 요청 번호를 확인하세요.')
        now = self.clock()
        with self.connect() as db:
            if self._config(db, 'telegram_verification_pending', False):
                raise ValueError('클라우드에서 봇·수신 채팅 확인을 마친 뒤 테스트하세요.')
            db.execute("INSERT OR IGNORE INTO deliveries(id,channel,body,status,is_test,created_at,updated_at) VALUES(?,'telegram',?,'pending',1,?,?)",
                       ('test-'+request_id,'트레이딩룸 가격 알림 연결 테스트입니다. 이 메시지를 받았다면 수신 채팅까지 발송됐습니다.',now,now))
        self.wake.set()
        return {'message': '테스트 발송을 요청했습니다. 아래 발송 기록과 텔레그램 수신을 확인하세요.'}

    def _quote(self, key):
        cached = self.quote_cache.get(key)
        if cached and self.clock()-cached[0] < (15 if cached[1] else 60) and (not cached[1] or -10 <= self.clock()-cached[1]['source_ts'] <= 180):
            return cached[1], cached[2]
        try:
            value = self.feeds.quote(*key)
            if value['source'] != key[0] or value['currency'] != SOURCES[key[0]][2]:
                raise ValueError('가격 출처 또는 통화 불일치')
            value['price'] = positive(value['price'])
            value['source_ts'] = float(value['source_ts'])
            age = self.clock()-value['source_ts']
            if not math.isfinite(age) or not -10 <= age <= 180:
                raise ValueError('장 마감·휴장 또는 지연 시세라 발동하지 않았습니다.')
            self.quote_cache[key] = (self.clock(),value,'')
            return value, ''
        except Exception as exc:
            error = str(exc) if isinstance(exc, ValueError) else '가격 조회 실패 · 연결과 종목 코드를 확인하세요.'
            self.quote_cache[key] = (self.clock(),None,error)
            return None, error
        finally:
            while len(self.quote_cache)>256:
                self.quote_cache.pop(next(iter(self.quote_cache)))

    def run_once(self):
        if not self.run_lock.acquire(blocking=False):
            return False
        try:
            with self.connect() as db:
                # No other cycle can send while run_lock is held. A leftover claim
                # means the prior cycle failed to commit an outcome after its send.
                db.execute("UPDATE deliveries SET status='uncertain',error='이전 발송 결과 저장 미확정 · 수신 확인 후 수동 재전송',updated_at=? WHERE status='sending'",(self.clock(),))
                cloud_mode = self._config(db, 'cloud_mode', False) and not self.remote_worker
                rows = [dict(r) for r in db.execute("SELECT * FROM alerts WHERE status='active' AND review_required=0 AND (channel='screen' OR ?=1) AND (?=0 OR channel='screen') AND (?=0 OR channel='telegram') LIMIT 200",(int(self._config(db,'telegram_enabled',False)),int(cloud_mode),int(self.remote_worker)))]
            keys = list(dict.fromkeys((r['source'],r['symbol']) for r in rows))
            with ThreadPoolExecutor(max_workers=4) as pool:
                prices = dict(zip(keys,pool.map(self._quote,keys))) if keys else {}
            for row in rows:
                value,error = prices[(row['source'],row['symbol'])]
                now = self.clock()
                with self.connect() as db:
                    db.execute('BEGIN IMMEDIATE')
                    current = db.execute('SELECT status,version FROM alerts WHERE id=?',(row['id'],)).fetchone()
                    if not current or current['status']!='active' or current['version']!=row['version']:
                        continue
                    if row['channel']=='telegram' and not self._config(db, 'telegram_enabled', False):
                        continue
                    if value and row['source_ts'] is not None and value['source_ts'] <= row['source_ts']:
                        # An older/same market tick must not move the crossing baseline backwards.
                        db.execute('UPDATE alerts SET last_checked=? WHERE id=?',(now,row['id']))
                        continue
                    db.execute('UPDATE alerts SET last_checked=?,error=?,last_price=?,source_ts=? WHERE id=?',
                               (now,error,value['price'] if value else row['last_price'],value['source_ts'] if value else row['source_ts'],row['id']))
                    if not value:
                        continue
                    price,target = Decimal(value['price']),Decimal(row['target'])
                    if row['condition'] == 'touch':
                        previous = Decimal(row['last_price']) if row['last_price'] is not None else None
                        hit = price == target or (previous is not None and
                              ((previous < target < price) or (price < target < previous)))
                    else:
                        hit = price>=target if row['condition']=='above' else price<=target
                    if not hit:
                        continue
                    condition_text = {'touch':'도달·통과 (위·아래 모두)','above':'이상','below':'이하'}[row['condition']]
                    text = f"가격 도달 · {row['name']} ({row['symbol']})\n{SOURCES[row['source']][0]}\n현재가 {value['price']} {value['currency']} · 목표 {row['target']} {condition_text}\n시세 시각 {datetime.fromtimestamp(value['source_ts'],ZoneInfo('Asia/Seoul')).isoformat(timespec='seconds')}"
                    db.execute("INSERT OR IGNORE INTO deliveries(id,alert_id,channel,body,status,created_at,updated_at) VALUES(?,?,?,?,'pending',?,?)",
                               (uuid.uuid4().hex,row['id'],row['channel'],text,now,now))
                    db.execute("UPDATE alerts SET status='triggered',version=version+1,updated_at=? WHERE id=?",(now,row['id']))
            self._deliver()
            with self.connect() as db:
                self._put(db,'last_check',self.clock())
                self._put(db,'worker_error','')
            return True
        finally:
            self.run_lock.release()

    def _deliver(self):
        with self.connect() as db:
            rows = [dict(r) for r in db.execute("SELECT * FROM deliveries WHERE status IN ('pending','failed') AND next_try<=? ORDER BY created_at LIMIT 10",(self.clock(),))]
        for row in rows:
            if self.stop_event.is_set():
                return
            with self.config_lock:
                with self.connect() as db:
                    db.execute('BEGIN IMMEDIATE')
                    if row['channel']=='telegram' and self._config(db, 'cloud_mode', False) and not self.remote_worker:
                        continue
                    if row['channel']=='screen' and self.remote_worker:
                        continue
                    if row['alert_id'] and not db.execute("SELECT 1 FROM alerts WHERE id=? AND status='triggered'",(row['alert_id'],)).fetchone():
                        continue
                    if row['channel']=='telegram' and (not self.vault.exists('telegram') or (not row['is_test'] and not self._config(db,'telegram_enabled',False))):
                        continue
                    changed = db.execute("UPDATE deliveries SET status='sending',attempts=attempts+1,updated_at=? WHERE id=? AND status IN ('pending','failed')",(self.clock(),row['id']))
                    if not changed.rowcount:
                        continue
                # A cloud claim must be durable before a message leaves this process.
                if self.checkpoint:
                    self.checkpoint()
                state,error,message_id,delay = 'sent','','',0
                try:
                    if row['channel']=='telegram':
                        if self.stop_event.wait(max(0,1.1-(time.monotonic()-self.last_send))):
                            raise TelegramFailure('종료 중 발송 결과 미확정 · 수신 여부를 확인하세요.',uncertain=True)
                        self.last_send = time.monotonic()
                        message_id = str(self.telegram.send(self.vault.get('telegram'),row['body']))
                except TelegramFailure as exc:
                    state = 'uncertain' if exc.uncertain else 'failed'
                    error,delay = str(exc),exc.retry_after
                except Exception:
                    state,error = 'uncertain','발송 결과를 확인하지 못했습니다. 수신 여부를 확인하세요.'
                with self.connect() as db:
                    db.execute('UPDATE deliveries SET status=?,error=?,message_id=?,next_try=?,updated_at=? WHERE id=?',
                               (state,error,message_id,self.clock()+delay,self.clock(),row['id']))
                    if state=='sent' and row['alert_id']:
                        db.execute("UPDATE alerts SET status='sent',updated_at=? WHERE id=? AND status='triggered'",(self.clock(),row['alert_id']))
                if self.checkpoint:
                    self.checkpoint()

    def connect_cloud(self, payload):
        from .alert_cloud import connect_cloud
        return connect_cloud(self, payload)

    def import_legacy(self, records, label):
        """Import snapshots once, paused. Never changes the running legacy service."""
        count = 0
        with self.connect() as db:
            for symbol,conditions in records.items():
                raw = str(symbol).upper()
                for condition in conditions:
                    fingerprint = hashlib.sha256(json.dumps([label,raw,condition],sort_keys=True).encode()).hexdigest()
                    if db.execute('SELECT 1 FROM imports WHERE fingerprint=?',(fingerprint,)).fetchone():
                        continue
                    target = positive(condition['target'])
                    if condition['condition'] not in ('above','below'):
                        raise ValueError('이전 알림의 조건을 확인하세요.')
                    ticker = raw.split(':')[-1]
                    source = 'yahoo'
                    if raw.startswith('KR:') or ticker.isdigit():
                        source = 'kiwoom'
                    elif ticker.endswith('.P'):
                        source,ticker = 'binance_futures',ticker[:-2]
                    elif raw.startswith('COIN:') or ticker.endswith(('USDT','USDC')) or raw.startswith('KRW-') or raw in ('BTC','ETH','SOL','BLUR'):
                        ticker = ticker.removeprefix('KRW-')
                        source = 'binance_usdc' if ticker.endswith('USDC') else 'binance'
                        if not ticker.endswith(('USDT','USDC')):
                            ticker += 'USDT'
                    alert_id,now = uuid.uuid4().hex,self.clock()
                    db.execute('''INSERT INTO alerts(id,request_id,source,symbol,name,target,condition,channel,status,review_required,legacy_symbol,error,created_at,updated_at)
                        VALUES(?,?,?,?,?,?,?,'telegram','paused',1,?,?,?,?)''',
                        (alert_id,'legacy-'+fingerprint,source,ticker,raw,target,condition['condition'],raw,'이전 알림 · 가격 출처와 통화를 확인한 뒤 수정·저장하세요.',now,now))
                    db.execute('INSERT INTO imports VALUES(?,?)',(fingerprint,alert_id))
                    count += 1
        return count

    def start(self):
        from engine.bot.storage import InstanceLock
        if self.thread and self.thread.is_alive():
            return
        self.instance = InstanceLock(self.directory / 'worker.lock')
        try:
            self.instance.acquire()
        except Exception:
            self.instance.close()
            raise RuntimeError('다른 가격 알림 감시가 실행 중입니다.') from None
        with self.connect() as db:
            db.execute("UPDATE deliveries SET status='uncertain',error='재시작 전 발송 결과 미확정 · 수신 확인 후 수동 재전송',updated_at=? WHERE status='sending'",(self.clock(),))
        def run():
            try:
                while not self.stop_event.is_set():
                    self.wake.clear()
                    try:
                        self.run_once()
                    except Exception:
                        try:
                            with self.connect() as db:
                                self._put(db,'worker_error','가격 알림 감시 오류 · 다음 주기에 재확인합니다.')
                        except Exception:
                            pass
                    self.wake.wait(30)
            finally:
                self.instance.close()
        self.thread = threading.Thread(target=run,name='price-alerts',daemon=True)
        self.thread.start()

    def close(self):
        self.stop_event.set()
        self.wake.set()
        if self.thread:
            self.thread.join(timeout=2)

