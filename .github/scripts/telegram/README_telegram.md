# Telegram Message Sender

Universal script for sending messages to Telegram channels.

## Quick Start

```bash
# Send file (automatically detected)
python .github/scripts/telegram/send_telegram_message.py \
  --message "path/to/file.txt" \
  --bot-token "YOUR_BOT_TOKEN" \
  --chat-id "1234567890"

# Send direct message
python .github/scripts/telegram/send_telegram_message.py \
  --message "Hello, World!" \
  --bot-token "YOUR_BOT_TOKEN" \
  --chat-id "1234567890"

# Send photo with caption
python .github/scripts/telegram/send_telegram_message.py \
  --message "Check out this image!" \
  --photo "path/to/image.jpg" \
  --bot-token "YOUR_BOT_TOKEN" \
  --chat-id "1234567890"
```

## Parameters

- `--bot-token` - Telegram bot token (required)
- `--chat-id` - Chat/channel ID (required)
- `--message` - Message text or path to file to send (automatically detected)
- `--photo` - Path to photo file to send (optional)
- `--parse-mode` - Parse mode (Markdown, HTML, None) - default: Markdown
- `--delay` - Delay between messages in seconds - default: 1
- `--max-length` - Maximum message length - default: 4000
- `--message-thread-id` - Thread ID for group messages (optional)
- `--disable-web-page-preview` - Disable link previews (default: True)
- `--max-retries` - Maximum number of retry attempts for failed messages (default: 5)
- `--retry-delay` - Delay between retry attempts in seconds (default: 10)

## Features

- ✅ Automatic splitting of long messages into chunks
- ✅ Markdown formatting support
- ✅ Photo sending with caption support
- ✅ Delay between messages to avoid API limits
- ✅ Message thread support
- ✅ Disable link previews
- ✅ Automatic retry mechanism (5 retries with 10s delay by default)
- ✅ Detailed error logging with failed message content
- ✅ Error handling with detailed diagnostics