# Telegram Integration Scripts

This directory contains scripts for integrating with Telegram for sending notifications and messages.

## Scripts

### 📨 `send_telegram_message.py`
General-purpose script for sending messages or file contents to Telegram channels.

**Features:**
- Send text messages or file contents
- Support for message threads
- Message chunking for large content
- Disable web page previews
- Configurable delays between messages

**Documentation:** [README_telegram.md](README_telegram.md)

### 👥 `parse_and_send_team_issues.py`
Specialized script for parsing GitHub issues and sending team-specific notifications.

**Features:**
- Parse team issues from formatted results
- Send separate messages for each team
- Support for multiple responsible users per team
- Team lead mentions in messages
- Support for message threads
- Dry run mode for testing

**Documentation:** [README_team_issues.md](README_team_issues.md)

## Configuration

### Example Team Responsible Mapping
See [team_leads_example.json](team_leads_example.json) for example configuration.

**Supported formats:**
- Single responsible: `"team": "@team-lead"`
- Multiple responsible: `"team": ["@lead1", "@lead2", "@lead3"]`

## GitHub Actions Setup

### 1. Secrets (Settings → Secrets and variables → Actions → Secrets)
- `TELEGRAM_BOT_TOKEN` - your bot token
- `TELEGRAM_YDBOT_TOKEN` - YDB bot token (used in workflow)

### 2. Variables (Settings → Secrets and variables → Actions → Variables)
- `TELEGRAM_MUTE_CHAT_ID` - channel ID in format "chat_id/thread_id" (e.g., "2018419243/1")
- `TEAM_TO_RESPONSIBLE_TG` - JSON string mapping teams to responsible users

### 3. Getting Bot Token
1. Find [@BotFather](https://t.me/BotFather) in Telegram
2. Send `/newbot`
3. Follow instructions to create a bot
4. Get token like `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`

### 4. Getting Channel ID
For channel `https://t.me/c/2018419243/1`:
- Channel ID: `-1002018419243`
- Or use [@userinfobot](https://t.me/userinfobot) to get the ID

## Usage in GitHub Actions

These scripts are used in the `create_issues_for_muted_tests.yml` workflow to send notifications about muted tests to team channels.

## Requirements

- Python 3.7+
- `requests` library
- Valid Telegram bot token
- Telegram chat/channel access