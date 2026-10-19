"""Encrypted GitHub-backed price alerts; never uploads trading credentials."""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sqlite3
import tempfile
import threading
import time
import traceback
import zlib
from contextlib import closing, contextmanager
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from cryptography.fernet import Fernet

DEFAULT_REPOSITORY = 'mallijima-gif/my-price-alert'
STATE_PATH = 'trading-room/state.enc'
MAX_STATE = 8 * 1024 * 1024


class Conflict(ValueError):
    pass


class GitHubState:
    def __init__(self, config):
        self.config = config
        self.repository = config['repository']
        # GitHub installation tokens are opaque and may use long JWT format.
        token = config.get('token', '')
        if not isinstance(token, str) or not 20 <= len(token) <= 16384 or any(ord(c) <= 32 or ord(c) >= 127 for c in token):
            raise ValueError('GitHub 연결 키 형식을 확인하세요.')
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', self.repository):
            raise ValueError('GitHub 저장소 이름을 확인하세요.')
        self.branch = config.get('branch', 'main')
        self.cipher = Fernet(config['state_key'].encode())

    def request(self, path='', body=None, method=None, missing=False):
        raw = None if body is None else json.dumps(body).encode()
        req = Request('https://api.github.com/repos/' + self.repository + path, data=raw,
                      method=method or ('GET' if body is None else 'PUT'),
                      headers={'Authorization': 'Bearer ' + self.config['token'],
                               'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json',
                               'User-Agent': 'TradingRoom', 'X-GitHub-Api-Version': '2022-11-28'})
        try:
            with urlopen(req, timeout=12) as response:
                content = response.read(MAX_STATE * 2 + 1)
                if len(content) > MAX_STATE * 2:
                    raise ValueError('클라우드 응답 크기를 확인하세요.')
                return json.loads(content) if content else {}
        except HTTPError as exc:
            code = exc.code
            exc.close()
            if missing and code == 404:
                return None
            if code in (409, 422):
                raise Conflict('클라우드 상태가 변경됐습니다. 최신 상태에서 다시 확인합니다.') from None
            raise ValueError(f'클라우드 연결 실패 ({code}) · GitHub 연결 권한을 확인하세요.') from None
        except (ValueError, Conflict):
            raise
        except Exception:
            raise ValueError('클라우드 응답을 확인하지 못했습니다. 저장·발송 결과를 확인하세요.') from None

    def encode(self, blob):
        if len(blob) > MAX_STATE:
            raise ValueError('클라우드 알림 기록이 너무 큽니다.')
        return self.cipher.encrypt(zlib.compress(blob))

    def decode(self, encrypted):
        try:
            compressed = self.cipher.decrypt(encrypted)
            unpack = zlib.decompressobj()
            blob = unpack.decompress(compressed, MAX_STATE + 1)
            if len(blob) > MAX_STATE or not unpack.eof or unpack.unused_data:
                raise ValueError()
            return blob
        except Exception:
            raise ValueError('클라우드 암호화 자료를 확인하지 못했습니다. 기존 자료는 보존됩니다.') from None

    def read(self, missing=False):
        item = self.request('/contents/' + STATE_PATH + '?ref=' + quote(self.branch, safe=''), missing=missing)
        if item is None:
            return None, None
        try:
            encrypted = base64.b64decode(item['content'])
            return self.decode(encrypted), item['sha']
        except ValueError:
            raise
        except Exception:
            raise ValueError('클라우드 자료 형식을 확인하세요.') from None

    def write(self, blob, sha):
        payload = {'message': 'Update encrypted price alert state', 'branch': self.branch,
                   'content': base64.b64encode(self.encode(blob)).decode()}
        if sha:
            payload['sha'] = sha
        result = self.request('/contents/' + STATE_PATH, payload)
        return result['content']['sha']

    def secret(self, name, value):
        try:
            from nacl.public import PublicKey, SealedBox
        except ImportError:
            raise ValueError('클라우드 연결용 PyNaCl 설치가 필요합니다.') from None
        public = self.request('/actions/secrets/public-key')
        encrypted = SealedBox(PublicKey(base64.b64decode(public['key']))).encrypt(value.encode())
        self.request('/actions/secrets/' + name,
                     {'key_id': public['key_id'], 'encrypted_value': base64.b64encode(encrypted).decode()})


class MemoryVault:
    def __init__(self, telegram):
        self.values = {'telegram': telegram} if telegram else {}

    def exists(self, name):
        return name in self.values

    def get(self, name):
        return self.values.get(name)

    def put(self, name, value):
        self.values[name] = value


def snapshot(service):
    with service.connect() as source, closing(sqlite3.connect(':memory:')) as target:
        source.backup(target)
        return target.serialize()


@contextmanager
def session(blob, telegram, feeds=None):
    from .price_alerts import PriceAlerts
    with tempfile.TemporaryDirectory(prefix='tradingroom-alert-') as directory:
        path = Path(directory) / 'alerts.sqlite3'
        path.write_bytes(blob)
        service = PriceAlerts(directory, feeds=feeds, vault=MemoryVault(telegram), load_cloud=False)
        try:
            with service.connect() as db:
                if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    raise ValueError('클라우드 알림 DB 무결성 확인 실패')
            yield service
        finally:
            service.close()


class CloudAlerts:
    def __init__(self, local, client=None):
        self.local = local
        self.client = client or GitHubState(local.vault.get('cloud-github'))
        self.lock = threading.RLock()
        self.cached = None
        self.checked = 0
        self.error = ''

    def refresh(self, force=False):
        with self.lock:
            if not force and time.monotonic() - self.checked < 60:
                return
            self.checked = time.monotonic()
            try:
                blob, _ = self.client.read()
                with session(blob, self.local.vault.get('telegram')) as service:
                    self.cached = service.overview()
                self.error = ''
            except Exception as exc:
                self.error = str(exc) if isinstance(exc, ValueError) else '클라우드 동기화 실패'

    def overview(self, base):
        self.refresh()
        remote = self.cached
        if remote:
            base['alerts'] = [a for a in remote['alerts'] if a['channel'] == 'telegram'] + [a for a in base['alerts'] if a['channel'] != 'telegram']
            base['logs'] = sorted([r for r in remote['logs'] if r['channel'] == 'telegram'] + [r for r in base['logs'] if r['channel'] != 'telegram'], key=lambda r: r['created_at'], reverse=True)[:100]
            base['telegram'].update(remote['telegram'])
        last = remote['worker']['last_check'] if remote else None
        base['cloud'] = {'enabled': True, 'last_check': last, 'stale': not last or time.time() - last > 1200,
                         'error': self.error or (remote['worker']['error'] if remote else ''),
                         'interval': 300, 'repository': self.client.repository}
        return base

    def owns(self, identifier, delivery=False):
        if not identifier:
            return False
        self.refresh()
        if not self.cached:
            # Do not mutate a stale local Telegram record during a cloud outage.
            with self.local.connect() as db:
                table = 'deliveries' if delivery else 'alerts'
                return bool(db.execute(f"SELECT 1 FROM {table} WHERE id=? AND channel='telegram'", (identifier,)).fetchone())
        return any(row['id'] == identifier and row['channel'] == 'telegram'
                   for row in self.cached['logs' if delivery else 'alerts'])

    def mutate(self, operation, payload):
        if operation == 'save':
            if payload.get('channel') != 'telegram':
                raise ValueError('클라우드 알림은 텔레그램으로 보냅니다. 화면 알림은 새 알림으로 등록하세요.')
            if payload.get('source') == 'kiwoom':
                raise ValueError('키움 시세는 이 PC의 인증이 필요합니다. 클라우드 국장 알림은 네이버 KRW를 선택하세요.')
        with self.lock:
            for attempt in range(3):
                blob, sha = self.client.read()
                with session(blob, self.local.vault.get('telegram')) as service:
                    if operation == 'action' and payload.get('action') == 'resume':
                        with service.connect() as db:
                            if db.execute("SELECT 1 FROM alerts WHERE id=? AND source='kiwoom'", (payload.get('id'),)).fetchone():
                                raise ValueError('클라우드 국장 알림은 수정 화면에서 네이버 KRW를 선택하세요.')
                    result = getattr(service, operation)(payload)
                    updated = snapshot(service)
                    view = service.overview()
                try:
                    self.client.write(updated, sha)
                except Conflict:
                    if attempt < 2:
                        continue
                    raise
                self.cached, self.checked, self.error = view, time.monotonic(), ''
                return result
        raise ValueError('클라우드 저장 상태를 확인하세요.')

    def configure_telegram(self, payload):
        token = str(payload.get('token', '')).strip()
        chat_id = str(payload.get('chat_id', '')).strip() or (self.local.vault.get('telegram') or {}).get('chat_id', '')
        if not re.fullmatch(r'[0-9]{5,20}:[A-Za-z0-9_-]{25,120}', token) or not re.fullmatch(r'-?[0-9]{5,20}', chat_id):
            raise ValueError('봇 토큰과 Chat ID를 확인하세요.')
        self.mutate('telegram_mode', {'enabled': False})
        config = {'token': token, 'chat_id': chat_id}
        self.client.secret('TRADING_ROOM_TELEGRAM', json.dumps(config))
        # Keep the verified replacement only after GitHub accepted the secret.
        self.local.vault.put('telegram', config)
        return self.mutate('stage_telegram', {})


def connect_cloud(service, payload):
    from .price_alerts import Telegram
    config = service.vault.get('cloud-github') or {}
    token = str(payload.get('token', '')).strip() or config.get('token', '')
    repository = str(payload.get('repository', '')).strip() or config.get('repository', DEFAULT_REPOSITORY)
    if not token:
        raise ValueError('GitHub 연결 권한이 필요합니다. 저장된 로그인 사용을 승인하거나 연결 키를 입력하세요.')
    key = config.get('state_key') or Fernet.generate_key().decode()
    updated = {'token': token, 'repository': repository, 'branch': config.get('branch', 'main'), 'state_key': key}
    client = GitHubState(updated)
    repo = client.request()
    if not repo.get('permissions', {}).get('push'):
        raise ValueError('해당 저장소의 설정 저장 권한이 필요합니다.')
    updated['branch'] = repo['default_branch']
    client = GitHubState(updated)
    workflow = client.request('/contents/.github/workflows/monitor.yml?ref=' + quote(client.branch, safe=''))
    code = base64.b64decode(workflow['content']).decode()
    if 'chart_scanner.alert_cloud' not in code or 'schedule:' not in code:
        raise ValueError('통합 클라우드 감시 코드 배포가 먼저 필요합니다.')
    telegram = service.vault.get('telegram')
    if not telegram:
        raise ValueError('기존 텔레그램 봇을 먼저 연결하세요.')
    Telegram().verify(telegram['token'], telegram['chat_id'])
    with service.connect() as db:
        if db.execute("SELECT 1 FROM alerts WHERE channel='telegram' AND status='active' AND source='kiwoom'").fetchone():
            raise ValueError('키움 알림을 네이버 KRW로 변경한 뒤 클라우드를 연결하세요.')
    # Preserve the key even if a later response is lost; never silently create a
    # different key for a state that might already have been uploaded.
    service.vault.put('cloud-github', updated)
    client.secret('TRADING_ROOM_ALERT_KEY', key)
    client.secret('TRADING_ROOM_TELEGRAM', json.dumps(telegram))
    with service.run_lock, service.config_lock:
        existing, sha = client.read(missing=True)
        with service.connect() as db:
            service._put(db, 'cloud_mode', True)
            if existing is None:
                service._put(db, 'last_check', None)
                service._put(db, 'worker_error', '')
        service.cloud = CloudAlerts(service, client)
        if existing is None:
            client.write(snapshot(service), None)
        # A repeat connect must not overwrite cloud events with an old local DB.
        service.cloud.refresh(force=True)
        if service.cloud.error:
            raise ValueError(service.cloud.error)
    return {'message': '클라우드에 저장했습니다. PC가 꺼져 있어도 기본 5분 주기로 감시합니다. 최초 클라우드 실행 시각을 확인하세요.'}


def run_cloud_once(client, telegram, feeds=None):
    blob, sha = client.read()
    with session(blob, telegram, feeds) as service:
        service.remote_worker = True

        def checkpoint():
            nonlocal sha
            sha = client.write(snapshot(service), sha)

        service.checkpoint = checkpoint
        with service.connect() as db:
            verify_pending = service._config(db, 'telegram_verification_pending', False)
        if verify_pending:
            from .price_alerts import TelegramFailure
            try:
                info = service.telegram.verify(telegram['token'], telegram['chat_id'])
            except TelegramFailure:
                with service.connect() as db:
                    service._put(db, 'worker_error', '클라우드에서 봇·수신 채팅 확인에 실패했습니다. 새 토큰과 수신 채팅을 확인하세요.')
                checkpoint()
                raise
            with service.connect() as db:
                service._put(db, 'telegram_info', info)
                service._put(db, 'telegram_verification_pending', False)
                service._put(db, 'worker_error', '')
            checkpoint()
        service.run_once()
        checkpoint()
        return service.overview()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', action='store_true', required=True)
    parser.parse_args()
    try:
        config = {'token': os.environ['GITHUB_TOKEN'], 'repository': os.environ['GITHUB_REPOSITORY'],
                  'branch': os.environ.get('GITHUB_REF_NAME', 'main'), 'state_key': os.environ['TRADING_ROOM_ALERT_KEY']}
        telegram = json.loads(os.environ['TRADING_ROOM_TELEGRAM'])
        run_cloud_once(GitHubState(config), telegram)
    except Exception as exc:
        # Only allowlisted source names and line numbers; exception text may contain secrets.
        frames = traceback.extract_tb(exc.__traceback__)
        location = ' > '.join(f'{Path(frame.filename).name}:{frame.lineno}' for frame in frames
                              if Path(frame.filename).name in ('alert_cloud.py', 'price_alerts.py'))
        print(f'Cloud alert cycle failed ({type(exc).__name__} at {location}); encrypted state and delivery claims are preserved.')
        raise SystemExit(1) from None
    print('Cloud alert cycle completed.')


if __name__ == '__main__':
    main()

