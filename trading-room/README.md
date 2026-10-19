# Trading Room cloud price alerts

The local Trading Room UI manages encrypted settings and results. GitHub Actions monitors Telegram alerts while the PC is off. No order engine code, trading credentials, account data, or AI connection is deployed here.

State: trading-room/state.enc (compressed SQLite authenticated with an independent Fernet key). Store TRADING_ROOM_ALERT_KEY and TRADING_ROOM_TELEGRAM in repository Actions Secrets, never in source or logs.

A durable cloud claim is committed before sending. Uncertain sends are never automatically replayed. GitHub file SHA checks prevent UI edits and cloud results overwriting one another. Local Telegram sending stays off after cloud handoff, including during an outage.

The schedule requests a run every five minutes and GitHub can delay runs. Check the cloud heartbeat and delivery status in Trading Room. The original monitor.py and alarms.json are preserved for recovery and must not be scheduled together with this worker.
