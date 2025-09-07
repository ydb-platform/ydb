# Team Issues Parser and Sender

Script for parsing GitHub issues creation results and sending separate messages for each team to Telegram.

## Quick Start

```bash
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --file "created_issues.txt" \
  --bot-token "YOUR_BOT_TOKEN" \
  --chat-id "CHAT_ID"
```

## Parameters

- `--file` - Path to results file (required)
- `--bot-token` - Telegram bot token (or TELEGRAM_BOT_TOKEN env var)
- `--chat-id` - Chat/channel ID (or TELEGRAM_CHAT_ID env var)
- `--team-responsible` - JSON string mapping teams to responsible users (or TEAM_RESPONSIBLE env var)
- `--message-thread-id` - Thread ID for group messages (optional)
- `--delay` - Delay between messages in seconds (default: 2)
- `--dry-run` - Parse only without sending messages

## Message Format

```
🆕 **New muted tests for [team-name](https://github.com/orgs/ydb-platform/teams/team-name)** @responsible

 - 🎯 [Issue URL](Issue URL) - `Issue Title`
 - 🎯 [Issue URL](Issue URL) - `Issue Title`

```

## Responsible Users Configuration

```json
{
  "team1": "@username",
  "team2": ["@user1", "@user2"],
  "team3": "@user3"
}
```

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
- ✅ Markdown formatting with links
- ✅ Responsible user mentions in messages
- ✅ Message thread support
- ✅ Dry run mode for testing
- ✅ Automatic chat_id/thread_id parsing from "2018419243/1" format