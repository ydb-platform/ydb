# Alert Queued Jobs

`alert_queued_jobs.py` monitors the GitHub Actions queue and sends Telegram alerts when runs are stuck longer than configured thresholds. Used by [alert_queued_jobs.yml](../../workflows/alert_queued_jobs.yml) (every 30 min + manual `workflow_dispatch`).

**Flow:** Fetch `queued` runs from GitHub API → filter by blacklist → compare wait time to thresholds per workflow type → send 1–2 messages to Telegram (optionally with a call string at the start for a duty bot). Can also send an “all good” message when the queue is empty or no jobs are stuck.

---

## GitHub: Secrets & Variables

**Settings → Secrets and variables → Actions.**

| Type | Name | Description |
|------|------|-------------|
| Secret | `TELEGRAM_YDBOT_TOKEN` | Bot token (passed as `TELEGRAM_BOT_TOKEN` in the workflow). |
| Variable | `GH_ALERTS_TG_LOGINS` | Call string at the **start** of alert messages so the duty bot reacts (e.g. `"/duty ydb-ci"` or `"@user"`). Empty = no call. |
| Variable | `GH_ALERTS_RUNS_BLACK_LIST` | Space-separated run IDs to ignore (passed as `--blacklist`). |

Workflow also uses: `--chat-id 3017506311`, `--send-when-all-good`, `--notify-on-api-errors`.

---

## Script config (top of file)

**WORKFLOW_THRESHOLDS:** list of `(pattern, display_name, threshold_spec)`. `threshold_spec` = fixed hours (e.g. `6`) or time-based `[(start_utc, end_utc, hours), ...]` (overnight when `start > end`).

**Defaults:** PR-check — 1 h (08:00–20:00 UTC), 3 h (20:00–08:00 UTC); Postcommit — 6 h fixed. Other constants: API URL, timeouts, max jobs per message, dashboard link.

---

## Local testing

**Dry-run** (no token/chat needed; prints messages that would be sent):

```bash
pip install requests
python .github/scripts/telegram/alert_queued_jobs.py --dry-run --chat-id 0
```

With call string (e.g. for duty bot):

```bash
GH_ALERTS_TG_LOGINS="/duty ydb-ci" python .github/scripts/telegram/alert_queued_jobs.py --dry-run --chat-id 0
```

Output shows `--- Message 1 ---` / `--- Message 2 ---`. The second message (call + stuck list) only appears when there are runs over the threshold.

**Test Telegram connection** (token + chat required):

```bash
python .github/scripts/telegram/alert_queued_jobs.py --test-connection --bot-token "..." --chat-id "..."
```

---

## CLI & env

| Option / Env | Description |
|--------------|-------------|
| `--dry-run` / `DRY_RUN=true` | Print messages, do not send. |
| `--chat-id`, `--channel` | Telegram chat/channel ID. |
| `--bot-token` | Or `TELEGRAM_BOT_TOKEN`. |
| `--thread-id` | Or `TELEGRAM_THREAD_ID`. |
| `--test-connection` | Only check chat access (getChat). |
| `--send-when-all-good` | Send even when queue is fine. |
| `--notify-on-api-errors` | Notify on GitHub API errors. |
| `--blacklist` | Space-separated run IDs to ignore. |
| `GH_ALERTS_TG_LOGINS` | Same as repo variable (call at message start). |
| `GITHUB_REPOSITORY` | Default `ydb-platform/ydb` for links. |

---

## Message format

- If `GH_ALERTS_TG_LOGINS` is set, **alert and API-error messages start with that string** (so the duty bot sees the command first).
- Message 1: summary (queue size, stuck count, per-workflow stats).
- Message 2 (only when there are stuck jobs): call (if set), then stuck list and dashboard link.

**Requirements:** Python 3.7+, `requests`.
