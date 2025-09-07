# Telegram Message Sender

Universal script for sending messages to Telegram channels.

## Quick Start

```bash
# Send file
python .github/scripts/telegram/send_telegram_message.py \
  --file "path/to/file.txt" \
  --bot-token "YOUR_BOT_TOKEN" \
  --chat-id "CHAT_ID"

# Send message
python .github/scripts/telegram/send_telegram_message.py \
  --message "Hello, World!" \
  --bot-token "YOUR_BOT_TOKEN" \
  --chat-id "CHAT_ID"
```

## Parameters

- `--bot-token` - Telegram bot token (required)
- `--chat-id` - Chat/channel ID (required)
- `--file` - Path to file to send
- `--message` - Direct message to send
- `--parse-mode` - Parse mode (Markdown, HTML, None) - default: Markdown
- `--delay` - Delay between messages in seconds - default: 1
- `--max-length` - Maximum message length - default: 4000
- `--message-thread-id` - Thread ID for group messages (optional)
- `--disable-web-page-preview` - Disable link previews (default: True)

## Features

- ✅ Automatic splitting of long messages into chunks
- ✅ Markdown formatting support
- ✅ Delay between messages to avoid API limits
- ✅ Message thread support
- ✅ Disable link previews
- ✅ Error handling with detailed diagnostics