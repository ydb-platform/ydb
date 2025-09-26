# Team Issues Parser and Sender

Script for parsing GitHub issues creation results and sending separate messages for each team to Telegram.

## Quick Start

```bash
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --file "created_issues.txt" \
  --bot-token "YOUR_BOT_TOKEN" \
  --team-channels '{"channels": {"main": "1234567890/1"}, "teams": {"team1": {"channel": "main", "responsible": ["@lead1"]}}, "default_channel": "main"}'
```

## Parameters

- `--file` - Path to results file (required)
- `--bot-token` - Telegram bot token (or TELEGRAM_BOT_TOKEN env var)
- `--team-channels` - JSON string mapping teams to their channel configurations (or TEAM_CHANNELS env var) (required)
- `--message-thread-id` - Thread ID for group messages (optional)
- `--delay` - Delay between messages in seconds (default: 2)
- `--dry-run` - Parse only without sending messages
- `--max-retries` - Maximum number of retry attempts for failed messages (default: 5)
- `--retry-delay` - Delay between retry attempts in seconds (default: 10)

## Message Format

```
🔇 **07-09-24 new muted tests in `main` for [team-name](https://github.com/orgs/ydb-platform/teams/team-name)** #teamname

 🎯 `Issue Title` [#12345](https://github.com/...)
 🎯 `Another Issue Title` [#12346](https://github.com/...)

📊 **[Total muted tests: 150](https://datalens.yandex/4un3zdm0zcnyr?owner_team=team-name) 🔴+5 muted /🟢-2 unmuted**

fyi: @responsible1 @responsible2
```

## Team Channels Configuration

```json
{
  "channels": {
    "main": "1234567890/1",
    "secondary": "1234567890/5"
  },
  "teams": {
    "engineering": {
      "channel": "secondary",
      "responsible": ["@engineering-lead"]
    },
    "appteam": {
      "channel": "secondary",
      "responsible": ["@appteam-lead"]
    },
    "docs": {
      "channel": "main",
      "responsible": ["@docs-lead"]
    }
  },
  "default_channel": "main"
}
```

### Channel Selection Logic

1. **Team-specific channel**: If team exists in `teams` → use its `channel` from `channels`
2. **Default channel**: If team not found → use `default_channel` from `channels`
3. **Fallback**: If no `TEAM_CHANNELS` or `default_channel` → use `--chat-id` parameter

### Configuration Structure

- **`channels`** - Maps channel names to actual chat IDs
- **`teams`** - Maps team names to their channel and responsible users
- **`default_channel`** - Default channel name for teams not specified

### Responsible Users Logic

1. **From `teams[team].responsible`**: If team has `responsible` → use it
2. **Empty**: If no responsible found → no responsible users in message

## Supported Input File Format

```
🆕 **CREATED ISSUES**
─────────────────────────────

👥 **TEAM** @ydb-platform/team1
   🎯 https://github.com/ydb-platform/ydb/issues/12345 - `Issue Title 1`
   🎯 https://github.com/ydb-platform/ydb/issues/12346 - `Issue Title 2`
```

## Features

- ✅ Separate messages for each team
- ✅ Markdown formatting with proper escaping
- ✅ Responsible user mentions in messages
- ✅ Message thread support
- ✅ Dry run mode for testing
- ✅ Automatic retry mechanism (5 retries with 10s delay by default)
- ✅ Detailed error logging with failed message content
- ✅ Automatic chat_id/thread_id parsing from "2018419243/1" format