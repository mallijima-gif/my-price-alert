# Trading Room cloud price alerts

The local Trading Room UI manages encrypted settings and results. GitHub Actions monitors Telegram alerts while the PC is off. No order engine code, trading credentials, account data, or AI connection is deployed here.

State: trading-room/state.enc (compressed SQLite authenticated with an independent Fernet key). Store TRADING_ROOM_ALERT_KEY and TRADING_ROOM_TELEGRAM in repository Actions Secrets, never in source or logs.

A durable cloud claim is committed before sending. Uncertain sends are never automatically replayed. GitHub file SHA checks prevent UI edits and cloud results overwriting one another. Local Telegram sending stays off after cloud handoff, including during an outage.

The schedule requests a run every five minutes and GitHub can delay runs. Check the cloud heartbeat and delivery status in Trading Room. The original monitor.py and alarms.json are preserved for recovery and must not be scheduled together with this worker.


Token replacement retains the existing recipient when Chat ID is left blank. The replacement is encrypted locally and in Actions Secrets, then the cloud verifies the bot and recipient before tests or price delivery can be enabled. Verify a new test delivery before enabling monitoring again. Credentials are never stored in SQLite state or printed in worker errors.

GitHub installation tokens are treated as opaque values, including the current long stateless token format. The local sync credential needs Contents, Actions, and Secrets read/write for this repository only. The worker uses the per-run GITHUB_TOKEN with Contents write permission.


New Trading Room alerts default to target touches in either direction. The first valid quote establishes a baseline, with equality triggering immediately. A subsequent valid quote reaching or crossing the target from above or below triggers once. The baseline persists across cloud runs; editing or resuming an alert starts it anew. Existing above/below thresholds keep their behavior. Out-of-order and stale quotes cannot move the crossing baseline. Sampling cannot detect a touch that reverses between checks.
