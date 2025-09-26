# Parse and Send Team Issues

Script to parse GitHub issues results and send separate messages for each team with muted tests statistics. Supports two modes: immediate updates and periodic trend reports.

## Quick Start

### Mode 1: On-Mute-Change Updates (Default)
Send immediate notifications about new muted tests:

```bash
# Basic usage with YDB statistics
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --on-mute-change-update \
  --file "formatted_results.txt" \
  --team-channels '{"teams": {"team-name": {"responsible": ["@user1", "@user2"], "channel": "channel-name"}}, "channels": {"channel-name": "123456789/1"}}' \
  --bot-token "YOUR_BOT_TOKEN"

# With custom YDB settings
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --on-mute-change-update \
  --file "formatted_results.txt" \
  --team-channels '{"teams": {"team-name": {"responsible": ["@user1"], "channel": "channel-name"}}, "channels": {"channel-name": "123456789/1"}}' \
  --bot-token "YOUR_BOT_TOKEN" \
  --ydb-endpoint "grpcs://ydb.serverless.yandexcloud.net:2135" \
  --ydb-database "/ru-central1/b1g8ejbrie0sfh5k0j2j/etn8l4e3hbti8k4n5g2e" \
  --ydb-credentials "path/to/credentials.json"

# Dry run (show messages without sending)
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --on-mute-change-update \
  --file "formatted_results.txt" \
  --team-channels '{"teams": {"team-name": {"responsible": ["@user1"], "channel": "channel-name"}}, "channels": {"channel-name": "123456789/1"}}' \
  --dry-run

# Skip statistics fetch
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --on-mute-change-update \
  --file "formatted_results.txt" \
  --team-channels '{"teams": {"team-name": {"responsible": ["@user1"], "channel": "channel-name"}}, "channels": {"channel-name": "123456789/1"}}' \
  --bot-token "YOUR_BOT_TOKEN" \
  --no-stats
```

### Mode 2: Periodic Trend Updates
Send weekly or monthly trend reports with statistics and charts:

```bash
# Weekly trend updates
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --period-update week \
  --team-channels '{"default_channel": "main_channel", "teams": {"team-name": {"responsible": ["@user1"], "channel": "main_channel"}}, "channels": {"main_channel": "123456789/1"}}' \
  --bot-token "YOUR_BOT_TOKEN" \
  --ydb-endpoint "grpcs://ydb.serverless.yandexcloud.net:2135" \
  --ydb-database "/ru-central1/b1g8ejbrie0sfh5k0j2j/etn8l4e3hbti8k4n5g2e"

# Monthly trend updates with debug plots
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --period-update month \
  --team-channels '{"default_channel": "main_channel", "teams": {"team-name": {"responsible": ["@user1"], "channel": "main_channel"}}, "channels": {"main_channel": "123456789/1"}}' \
  --bot-token "YOUR_BOT_TOKEN" \
  --ydb-endpoint "grpcs://ydb.serverless.yandexcloud.net:2135" \
  --ydb-database "/ru-central1/b1g8ejbrie0sfh5k0j2j/etn8l4e3hbti8k4n5g2e" \
  --debug-plots-dir "/path/to/debug/plots" \
  --dry-run
```

## Parameters

### Mode Selection (Required - Choose One)
- `--on-mute-change-update` - Default mode: send updates about new muted tests (requires --file)
- `--period-update {week,month}` - Send periodic trend updates (no --file required)

### Required
- `--team-channels` - JSON string mapping teams to their channel configurations (or use TEAM_CHANNELS env var)
- `--file` - Path to file with formatted results (required only for --on-mute-change-update mode)

### Optional
- `--bot-token` - Telegram bot token (or use TELEGRAM_BOT_TOKEN env var)
- `--delay` - Delay between messages in seconds (default: 2)
- `--dry-run` - Parse and show teams without sending messages
- `--test-connection` - Test Telegram connection only
- `--message-thread-id` - Thread ID for group messages (optional)
- `--max-retries` - Maximum number of retry attempts for failed messages (default: 5)
- `--retry-delay` - Delay between retry attempts in seconds (default: 10)

### YDB Statistics
- `--ydb-endpoint` - YDB database endpoint (or use YDB_ENDPOINT env var)
- `--ydb-database` - YDB database path (or use YDB_DATABASE env var)
- `--ydb-credentials` - Path to YDB service account credentials JSON file (or use YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS env var)
- `--no-stats` - Skip fetching muted tests statistics from YDB (only for --on-mute-change-update)
- `--include-plots` - Include trend plots in messages (requires matplotlib)
- `--debug-plots-dir` - Directory to save debug plot files (enables debug mode)
- `--use-yesterday` - Use yesterday's data for development convenience

## Features

### On-Mute-Change Updates Mode
- ✅ Automatic parsing of team issues from formatted results
- ✅ Separate messages for each team
- ✅ Muted tests statistics from YDB (total, today, and minus today counts)
- ✅ Monthly trend plots with matplotlib
- ✅ Team-specific channel routing
- ✅ Responsible user mentions
- ✅ Markdown formatting support
- ✅ Delay between messages to avoid API limits
- ✅ Message thread support
- ✅ Automatic retry mechanism
- ✅ Dry run mode for testing
- ✅ Connection testing

### Periodic Trend Updates Mode
- ✅ Weekly and monthly trend reports
- ✅ Automatic team discovery from YDB data
- ✅ Fallback to default channel for teams without specific configuration
- ✅ Trend charts with 30-day history
- ✅ Period-over-period change calculations
- ✅ Color-coded statistics (red for increases, green for decreases)
- ✅ Dashboard links for each team
- ✅ Responsible user mentions (if team configured)
- ✅ Team hashtags for easy filtering

## Message Format

### On-Mute-Change Updates Mode
The script sends messages in the following format:

```
🔇 **13-01-25 new muted tests for [team-name](https://github.com/orgs/ydb-platform/teams/team-name)** #teamname

📊 **[Total muted tests: 150](https://datalens.yandex/4un3zdm0zcnyr?owner_team=team-name) (🔴+5/🟢-2)**

fyi: @user1 @user2

 - 🎯 [https://github.com/...](https://github.com/...) - `Issue Title`
 - 🎯 [https://github.com/...](https://github.com/...) - `Another Issue Title`
```

**Statistics explanation:**
- `📊 **[Total muted tests: N](dashboard_url) (🔴+M/🟢-K)**` - Total muted tests with today's changes
- `📊 **[Total muted tests: N](dashboard_url) (🔴+M)**` - Total muted tests with today's additions only
- `📊 **[Total muted tests: N](dashboard_url) (🟢-K)**` - Total muted tests with today's unmutes only
- `📊 **[Total muted tests: N](dashboard_url)**` - Total muted tests (no changes today)

### Periodic Trend Updates Mode
The script sends trend reports in the following format:

```
📈 **Изменения неделя к неделе для команды [team-name](https://github.com/orgs/ydb-platform/teams/team-name)** #teamname

📊 **[Total muted tests: 150](https://datalens.yandex/4un3zdm0zcnyr?owner_team=team-name) (🔴+10 vs 7 days ago)**

fyi: @user1 @user2

График показывает динамику замьюченных тестов за последние 30 дней.
```

**Period statistics explanation:**
- `(🔴+N vs X days ago)` - Increase compared to previous period
- `(🟢-N vs X days ago)` - Decrease compared to previous period
- `(0 vs X days ago)` - No change compared to previous period

## Team Channels Configuration

The `--team-channels` parameter expects a JSON configuration:

```json
{
  "default_channel": "default-channel-name",
  "teams": {
    "team-name": {
      "responsible": ["@user1", "@user2"],
      "channel": "specific-channel-name"
    },
    "another-team": {
      "responsible": "@single-user",
      "channel": "another-channel"
    }
  },
  "channels": {
    "default-channel-name": "123456789/1",
    "specific-channel-name": "987654321/2",
    "another-channel": "555666777"
  }
}
```

## Environment Variables

- `TELEGRAM_BOT_TOKEN` - Telegram bot token
- `TEAM_CHANNELS` - Team channels configuration JSON
- `YDB_ENDPOINT` - YDB database endpoint
- `YDB_DATABASE` - YDB database path
- `YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS` - Path to YDB credentials file

## Dependencies

- `ydb` - YDB Python client library
- `requests` - HTTP library for Telegram API
- `telegram` - Telegram bot functionality (from send_telegram_message.py)
- `matplotlib` - For creating trend plots (optional, only if --include-plots is used)

## Examples with Plots

### On-Mute-Change Updates with Plots
```bash
# Send messages with trend plots
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --on-mute-change-update \
  --file "formatted_results.txt" \
  --team-channels '{"teams": {"team-name": {"responsible": ["@user1"], "channel": "channel-name"}}, "channels": {"channel-name": "123456789/1"}}' \
  --bot-token "YOUR_BOT_TOKEN" \
  --include-plots \
  --use-yesterday

# Send messages with debug plots saved to custom directory
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --on-mute-change-update \
  --file "formatted_results.txt" \
  --team-channels '{"teams": {"team-name": {"responsible": ["@user1"], "channel": "channel-name"}}, "channels": {"channel-name": "123456789/1"}}' \
  --bot-token "YOUR_BOT_TOKEN" \
  --include-plots \
  --debug-plots-dir "/path/to/debug/plots"
```

### Periodic Trend Updates (Always Include Plots)
```bash
# Weekly trend updates with debug plots
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --period-update week \
  --team-channels '{"default_channel": "main_channel", "teams": {"team-name": {"responsible": ["@user1"], "channel": "main_channel"}}, "channels": {"main_channel": "123456789/1"}}' \
  --bot-token "YOUR_BOT_TOKEN" \
  --ydb-endpoint "grpcs://ydb.serverless.yandexcloud.net:2135" \
  --ydb-database "/ru-central1/b1g8ejbrie0sfh5k0j2j/etn8l4e3hbti8k4n5g2e" \
  --debug-plots-dir "/path/to/debug/plots"

# Monthly trend updates (dry run)
python .github/scripts/telegram/parse_and_send_team_issues.py \
  --period-update month \
  --team-channels '{"default_channel": "main_channel", "teams": {"team-name": {"responsible": ["@user1"], "channel": "main_channel"}}, "channels": {"main_channel": "123456789/1"}}' \
  --bot-token "YOUR_BOT_TOKEN" \
  --ydb-endpoint "grpcs://ydb.serverless.yandexcloud.net:2135" \
  --ydb-database "/ru-central1/b1g8ejbrie0sfh5k0j2j/etn8l4e3hbti8k4n5g2e" \
  --dry-run
```
