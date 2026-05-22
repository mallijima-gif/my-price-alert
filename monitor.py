import os
import json
import requests

ALARMS_FILE = 'alarms.json'
STATE_FILE = 'bot_state.json'

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

def load_json(filepath, default):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except:
                return default
    return default

def save_json(filepath, data):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram 설정 누락")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message}
    try:
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"텔레그램 발송 실패: {e}")

def get_telegram_updates(last_id):
    if not TELEGRAM_TOKEN: return []
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    try:
        res = requests.get(url, params={'offset': last_id + 1, 'timeout': 5}, timeout=10).json()
        if res.get('ok'):
            return res.get('result', [])
    except Exception as e:
        print(f"텔레그램 업데이트 실패: {e}")
    return []

def fetch_binance_prices(symbols):
    prices = {}
    for sym in symbols:
        # 간단한 변환 로직: KRW-BTC -> BTCUSDT
        upper = sym.upper().replace("KRW-", "")
        if not upper.endswith("USDT") and not upper.endswith("USDC"):
            binance_sym = f"{upper}USDT"
        else:
            binance_sym = upper
            
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={binance_sym}"
            res = requests.get(url, timeout=5).json()
            if 'price' in res:
                # 바이낸스는 달러 기준이지만, 여기서는 단순 처리. 실제 달러 알람을 맞추는 용도로 사용.
                # 한국 원화 기준을 원한다면 원래대로 야후나 업비트를 써야하지만, 사용자가 바이낸스를 요청함.
                prices[sym] = float(res['price'])
        except Exception as e:
            print(f"바이낸스 에러 ({sym}): {e}")
    return prices

def fetch_naver_prices(symbols):
    prices = {}
    for sym in symbols:
        try:
            url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{sym}"
            res = requests.get(url, timeout=5).json()
            raw_price = res.get('datas', [{}])[0].get('closePriceRaw')
            if raw_price:
                prices[sym] = float(raw_price)
        except Exception as e:
            print(f"네이버 에러 ({sym}): {e}")
    return prices

def process_commands(alarms, state):
    updates = get_telegram_updates(state.get('last_update_id', 0))
    changed = False
    
    for update in updates:
        state['last_update_id'] = update['update_id']
        changed = True
        
        msg = update.get('message', {})
        text = msg.get('text', '').strip()
        chat_id = str(msg.get('chat', {}).get('id', ''))
        
        # 보안: 설정된 CHAT_ID에서 온 메시지만 처리
        if chat_id != str(TELEGRAM_CHAT_ID):
            continue
            
        parts = text.split()
        if not parts: continue
        
        cmd = parts[0]
        
        if cmd == '/추가' and len(parts) >= 4:
            symbol = parts[1].upper()
            try:
                target = float(parts[2])
                condition = 'above' if parts[3].lower() in ['이상', 'above', '상승'] else 'below'
                
                if symbol not in alarms:
                    alarms[symbol] = []
                alarms[symbol].append({'target': target, 'condition': condition})
                send_telegram(f"✅ 알람 추가 완료!\n종목: {symbol}\n목표가: {target} ({condition})")
            except:
                send_telegram("❌ 잘못된 형식입니다.\n사용법: /추가 BTCUSDT 90000 above")
                
        elif cmd == '/목록':
            if not alarms:
                send_telegram("📝 등록된 알람이 없습니다.")
            else:
                msg_lines = ["📝 현재 알람 목록:"]
                for sym, conds in alarms.items():
                    msg_lines.append(f"\n[{sym}]")
                    for c in conds:
                        msg_lines.append(f"- {c['target']} ({c['condition']})")
                send_telegram("\n".join(msg_lines))
                
        elif cmd == '/삭제' and len(parts) >= 2:
            symbol = parts[1].upper()
            if symbol in alarms:
                del alarms[symbol]
                send_telegram(f"🗑️ {symbol} 알람이 모두 삭제되었습니다.")
            else:
                send_telegram(f"❌ {symbol} 알람을 찾을 수 없습니다.")

    return changed

def main():
    alarms = load_json(ALARMS_FILE, {})
    state = load_json(STATE_FILE, {"last_update_id": 0})
    
    # 1. 텔레그램 명령어 처리
    state_changed = process_commands(alarms, state)
    
    # 2. 가격 조회 준비
    crypto_symbols = [s for s in alarms.keys() if not s.isdigit()] # 숫자가 아니면 코인(또는 해외주식)으로 취급
    stock_symbols = [s for s in alarms.keys() if s.isdigit()]      # 숫자로만 구성되면 국장으로 취급
    
    current_prices = {}
    current_prices.update(fetch_binance_prices(crypto_symbols))
    current_prices.update(fetch_naver_prices(stock_symbols))
    
    # 3. 알람 검사 및 발송/삭제
    alarms_changed = False
    
    for symbol in list(alarms.keys()):
        price = current_prices.get(symbol)
        if price is None:
            continue
            
        remaining_conditions = []
        for cond in alarms[symbol]:
            target = cond['target']
            ctype = cond['condition']
            
            triggered = False
            if ctype == 'above' and price >= target:
                triggered = True
            elif ctype == 'below' and price <= target:
                triggered = True
                
            if triggered:
                msg = f"🚨 [가격 도달!] {symbol}\n현재가: {price:,}\n설정가: {target:,} ({ctype})"
                send_telegram(msg)
                alarms_changed = True
                # Triggered alarm is NOT added back to remaining_conditions
            else:
                remaining_conditions.append(cond)
                
        if remaining_conditions:
            alarms[symbol] = remaining_conditions
        else:
            del alarms[symbol]
            alarms_changed = True
            
    # 4. 파일 저장
    if state_changed:
        save_json(STATE_FILE, state)
        
    if alarms_changed or state_changed:
        save_json(ALARMS_FILE, alarms)
        print("상태/알람 업데이트 성공")

if __name__ == "__main__":
    main()
