#!/usr/bin/env python3
"""
Script to send messages to Telegram channel.
Can be used to send file contents or custom messages.
"""

import os
import sys
import argparse
import requests
import time
import re
from pathlib import Path


def escape_markdown(text):
    """
    Escape special MarkdownV2 characters for Telegram, but preserve bold formatting and link structure.
    
    Args:
        text (str): Text to escape
        
    Returns:
        str: Escaped text
    """
    # For MarkdownV2, we need to escape these characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
    # But we want to preserve * for bold formatting, [ ] ( ) for links, and content inside backticks
    
    # First, protect inline code by temporarily replacing them
    code_pattern = r'`([^`]+)`'
    code_blocks = []
    
    def replace_code(match):
        code_blocks.append(match.group(1))
        return f"CODEPLACEHOLDER{len(code_blocks)-1}N"
    
    # Replace all inline code with placeholders
    text = re.sub(code_pattern, replace_code, text)
    
    # Then protect links by temporarily replacing them
    link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    links = []
    
    def replace_link(match):
        links.append((match.group(1), match.group(2)))
        return f"LINKPLACEHOLDER{len(links)-1}N"
    
    # Replace all links with placeholders
    text = re.sub(link_pattern, replace_link, text)
    
    # Also protect standalone URLs (http/https) by temporarily replacing them
    url_pattern = r'https?://[^\s\)]+'
    urls = []
    
    def replace_url(match):
        urls.append(match.group(0))
        return f"URLPLACEHOLDER{len(urls)-1}N"
    
    # Replace all standalone URLs with placeholders
    text = re.sub(url_pattern, replace_url, text)
    
    # Escape special characters for MarkdownV2 (single backslash)
    # Characters that need escaping: _ * [ ] ( ) ~ ` > # + - = | { } . !
    special_chars = ['_', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!', '(', ')']
    
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    
    # Restore standalone URLs (they should NOT be escaped)
    for i, url in enumerate(urls):
        text = text.replace(f"URLPLACEHOLDER{i}N", url)
    
    # Restore links, escaping special characters in link text but NOT in URLs
    for i, (link_text, link_url) in enumerate(links):
        # Escape special characters in link text only
        escaped_text = link_text
        for char in special_chars:
            escaped_text = escaped_text.replace(char, f'\\{char}')
        
        # URLs should NOT be escaped (Telegram handles them correctly)
        text = text.replace(f"LINKPLACEHOLDER{i}N", f"[{escaped_text}]({link_url})")
    
    # Restore inline code (content should NOT be escaped)
    for i, code_content in enumerate(code_blocks):
        text = text.replace(f"CODEPLACEHOLDER{i}N", f"`{code_content}`")
    return text


def _read_content(message_or_file):
    """
    Read content from message string or file.
    
    Args:
        message_or_file (str): Message text or path to file to send
        
    Returns:
        str: Content to send, or None if error
    """
    # Try to open as file first, if it fails, treat as message
    try:
        with open(message_or_file, 'r', encoding='utf-8') as f:
            return f.read()
    except (OSError, IOError, ValueError, UnicodeDecodeError):
        # It's a text message, not a file (or file can't be read as text)
        return message_or_file


def _send_chunked_messages(bot_token, chat_id, content, parse_mode, message_thread_id, disable_web_page_preview, max_retries, retry_delay, delay, max_length=4000):
    """
    Send chunked text messages.
    
    Args:
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        content (str): Content to send
        parse_mode (str): Parse mode for message formatting
        message_thread_id (int, optional): Thread ID for group messages
        disable_web_page_preview (bool): Disable web page preview for links
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Delay between retry attempts in seconds
        delay (int): Delay between message chunks in seconds
        max_length (int): Maximum length per chunk
        
    Returns:
        bool: True if all chunks sent successfully, False otherwise
    """
    if not content.strip():
        print(f"⚠️ Content is empty")
        return True
    
    # Split message into chunks if needed
    chunks = split_message(content, max_length)
    print(f"📤 Sending {len(chunks)} message(s)...")
    
    success_count = 0
    for i, chunk in enumerate(chunks, 1):
        print(f"📨 Sending chunk {i}/{len(chunks)}...")
        
        if _send_single_message(bot_token, chat_id, chunk, parse_mode, message_thread_id, disable_web_page_preview, max_retries, retry_delay):
            success_count += 1
        
        # Add delay between messages (except for the last one)
        if i < len(chunks):
            time.sleep(delay)
    
    if success_count == len(chunks):
        print(f"✅ All {success_count} message(s) sent successfully!")
        return True
    else:
        print(f"⚠️ Only {success_count}/{len(chunks)} message(s) sent successfully")
        return False


def _send_photo_with_chunked_caption(bot_token, chat_id, photo_path, content, parse_mode, message_thread_id, disable_web_page_preview, max_retries, retry_delay, delay):
    """
    Send photo with chunked caption.
    
    Args:
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        photo_path (str): Path to photo file
        content (str): Caption content
        parse_mode (str): Parse mode for message formatting
        message_thread_id (int, optional): Thread ID for group messages
        disable_web_page_preview (bool): Disable web page preview for links
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Delay between retry attempts in seconds
        delay (int): Delay between message chunks in seconds
        
    Returns:
        bool: True if all chunks sent successfully, False otherwise
    """
    photo_file = Path(photo_path)
    if not photo_file.exists() or not photo_file.is_file():
        print(f"❌ Photo file not found: {photo_path}")
        return False
    
    print(f"📷 Sending photo: {photo_path}")
    
    # Split message into chunks for photo caption
    chunks = split_message(content, max_length=1024)  # Telegram caption limit is 1024 characters
    print(f"📤 Sending photo with {len(chunks)} message chunk(s)...")
    
    success_count = 0
    for i, chunk in enumerate(chunks, 1):
        print(f"📨 Sending photo chunk {i}/{len(chunks)}...")
        
        if i == 1:
            # First chunk goes with photo
            if _send_photo(bot_token, chat_id, photo_path, chunk, parse_mode, message_thread_id, max_retries, retry_delay):
                success_count += 1
                print(f"✅ Photo with caption sent successfully!")
            else:
                print(f"❌ Failed to send photo with caption")
        else:
            # Subsequent chunks as regular messages
            if _send_single_message(bot_token, chat_id, chunk, parse_mode, message_thread_id, disable_web_page_preview, max_retries, retry_delay):
                success_count += 1
                print(f"✅ Text chunk {i} sent successfully!")
            else:
                print(f"❌ Failed to send text chunk {i}")
        
        # Add delay between messages (except for the last one)
        if i < len(chunks):
            time.sleep(delay)
    
    if success_count == len(chunks):
        print(f"✅ All {success_count} message(s) sent successfully!")
        return True
    else:
        print(f"⚠️ Only {success_count}/{len(chunks)} message(s) sent successfully")
        return False


def send_telegram_message(bot_token, chat_id, message_or_file, parse_mode="MarkdownV2", message_thread_id=None, disable_web_page_preview=True, max_retries=5, retry_delay=10, delay=1, photo_path=None):
    """
    Send a message to Telegram channel with retry mechanism.
    Can accept either text message or file path.
    
    Args:
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        message_or_file (str): Message text or path to file to send
        parse_mode (str): Parse mode for message formatting
        message_thread_id (int, optional): Thread ID for group messages
        disable_web_page_preview (bool): Disable web page preview for links
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Delay between retry attempts in seconds
        delay (int): Delay between message chunks in seconds
        photo_path (str, optional): Path to photo file to send
        
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"🔍 send_telegram_message called with photo_path: {photo_path}")
    print(f"🔍 message_or_file length: {len(message_or_file) if message_or_file else 'None'}")
    print(f"🔍 chat_id: {chat_id}, message_thread_id: {message_thread_id}")
    
    # Read content from message string or file
    content = _read_content(message_or_file)
    if content is None:
        return False
    
    # Escape content for MarkdownV2 if needed
    if parse_mode == "MarkdownV2":
        content = escape_markdown(content)
    
    # If photo is provided, send photo with chunked caption
    if photo_path:
        return _send_photo_with_chunked_caption(bot_token, chat_id, photo_path, content, parse_mode, message_thread_id, disable_web_page_preview, max_retries, retry_delay, delay)
    
    # If no photo, send text message
    return _send_chunked_messages(bot_token, chat_id, content, parse_mode, message_thread_id, disable_web_page_preview, max_retries, retry_delay, delay)


def _send_single_message(bot_token, chat_id, message, parse_mode, message_thread_id, disable_web_page_preview, max_retries, retry_delay):
    """
    Send a single message to Telegram channel with retry mechanism.
    
    Args:
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        message (str): Message to send
        parse_mode (str): Parse mode for message formatting
        message_thread_id (int, optional): Thread ID for group messages
        disable_web_page_preview (bool): Disable web page preview for links
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Delay between retry attempts in seconds
        
    Returns:
        bool: True if successful, False otherwise
    """
    for attempt in range(max_retries + 1):
        if attempt > 0:
            print(f"🔄 Retry attempt {attempt}/{max_retries}...")
            time.sleep(retry_delay)
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        data = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': parse_mode,
            'disable_web_page_preview': disable_web_page_preview
        }
        
        # Add thread ID if provided
        if message_thread_id is not None:
            data['message_thread_id'] = message_thread_id
        
        try:
            response = requests.post(url, data=data, timeout=30)
            
            # Always print response for debugging
            print(f"🔍 Telegram API Response: {response.status_code}")
            
            if response.status_code != 200:
                print(f"❌ HTTP Error {response.status_code}: {response.text}")
                if attempt < max_retries:
                    print(f"⏳ Waiting {retry_delay} seconds before retry...")
                    continue
                else:
                    return False
                
            result = response.json()
            print(f"🔍 Full Telegram API Response: {result}")
            
            if result.get('ok'):
                thread_info = f" (thread {message_thread_id})" if message_thread_id is not None else ""
                print(f"✅ Message sent successfully to chat {chat_id}{thread_info}")
                return True
            else:
                print(f"❌ Telegram API Error: {result.get('description', 'Unknown error')}")
                if attempt < max_retries:
                    print(f"⏳ Waiting {retry_delay} seconds before retry...")
                    continue
                else:
                    return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Network error: {e}")
            if attempt < max_retries:
                print(f"⏳ Waiting {retry_delay} seconds before retry...")
                continue
            else:
                return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            if attempt < max_retries:
                print(f"⏳ Waiting {retry_delay} seconds before retry...")
                continue
            else:
                return False
    
    # If all retries failed, print the message that couldn't be sent
    print(f"❌ Failed to send message after {max_retries} retries. Message content:")
    print("=" * 80)
    print(message)
    print("=" * 80)
    return False


def _send_photo(bot_token, chat_id, photo_path, caption, parse_mode, message_thread_id, max_retries, retry_delay):
    """
    Send a photo to Telegram channel with retry mechanism.
    
    Args:
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        photo_path (str): Path to photo file
        caption (str): Photo caption
        parse_mode (str): Parse mode for caption formatting
        message_thread_id (int, optional): Thread ID for group messages
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Delay between retry attempts in seconds
        
    Returns:
        bool: True if successful, False otherwise
    """
    for attempt in range(max_retries + 1):
        if attempt > 0:
            print(f"🔄 Retry attempt {attempt}/{max_retries}...")
            time.sleep(retry_delay)
        
        url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
        
        # Prepare files and data for multipart/form-data
        with open(photo_path, 'rb') as photo_file:
            files = {'photo': photo_file}
            
            data = {
                'chat_id': chat_id,
                'caption': caption,
                'parse_mode': parse_mode
            }
            
            # Add thread ID if provided
            if message_thread_id is not None:
                data['message_thread_id'] = message_thread_id
            
            try:
                response = requests.post(url, files=files, data=data, timeout=60)
                
                # Always print response for debugging
                print(f"🔍 Telegram API Response: {response.status_code}")
                
                if response.status_code != 200:
                    print(f"❌ HTTP Error {response.status_code}: {response.text}")
                    if attempt < max_retries:
                        print(f"⏳ Waiting {retry_delay} seconds before retry...")
                        continue
                    else:
                        return False
                    
                result = response.json()
                print(f"🔍 Full Telegram API Response: {result}")
                
                if result.get('ok'):
                    thread_info = f" (thread {message_thread_id})" if message_thread_id is not None else ""
                    print(f"✅ Photo sent successfully to chat {chat_id}{thread_info}")
                    return True
                else:
                    print(f"❌ Telegram API Error: {result.get('description', 'Unknown error')}")
                    if attempt < max_retries:
                        print(f"⏳ Waiting {retry_delay} seconds before retry...")
                        continue
                    else:
                        return False
                    
            except requests.exceptions.RequestException as e:
                print(f"❌ Network error: {e}")
                if attempt < max_retries:
                    print(f"⏳ Waiting {retry_delay} seconds before retry...")
                    continue
                else:
                    return False
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                if attempt < max_retries:
                    print(f"⏳ Waiting {retry_delay} seconds before retry...")
                    continue
                else:
                    return False
    
    # If all retries failed, print the caption that couldn't be sent
    print(f"❌ Failed to send photo after {max_retries} retries. Caption content:")
    print("=" * 80)
    print(caption)
    print("=" * 80)
    return False


def split_message(message, max_length=4000):
    """
    Split long message into chunks.
    
    Args:
        message (str): Message to split
        max_length (int): Maximum length per chunk
        
    Returns:
        list: List of message chunks
    """
    if len(message) <= max_length:
        return [message]
    
    # Split by lines first
    lines = message.split('\n')
    chunks = []
    current_chunk = ""
    
    for line in lines:
        # If adding this line would exceed max_length, start new chunk
        if len(current_chunk) + len(line) + 1 > max_length:
            if current_chunk:
                chunks.append(current_chunk.rstrip())
                current_chunk = ""
        
        # Add line to current chunk
        if current_chunk:
            current_chunk += "\n" + line
        else:
            current_chunk = line
    
    # Add the last chunk if it's not empty
    if current_chunk:
        chunks.append(current_chunk.rstrip())
    
    return chunks


def main():
    parser = argparse.ArgumentParser(description="Send messages to Telegram channel")
    
    # Required arguments
    parser.add_argument('--bot-token', required=True, help='Telegram bot token')
    parser.add_argument('--chat-id', required=True, help='Telegram chat ID')
    parser.add_argument('--message', required=True, help='Message text or path to file to send')
    
    # Optional arguments
    parser.add_argument('--parse-mode', default='MarkdownV2', choices=['Markdown', 'MarkdownV2', 'HTML', 'None'], 
                       help='Parse mode for message formatting (default: MarkdownV2)')
    parser.add_argument('--delay', type=int, default=1, 
                       help='Delay between message chunks in seconds (default: 1)')
    parser.add_argument('--max-length', type=int, default=4000, 
                       help='Maximum message length (default: 4000)')
    parser.add_argument('--message-thread-id', type=int, 
                       help='Thread ID for group messages (optional)')
    parser.add_argument('--disable-web-page-preview', action='store_true', default=True,
                       help='Disable web page preview for links (default: True)')
    parser.add_argument('--max-retries', type=int, default=5,
                       help='Maximum number of retry attempts for failed messages (default: 5)')
    parser.add_argument('--retry-delay', type=int, default=10,
                       help='Delay between retry attempts in seconds (default: 10)')
    parser.add_argument('--photo', 
                       help='Path to photo file to send (optional)')
    
    args = parser.parse_args()
    
    # Get bot token and chat ID from environment variables if not provided
    bot_token = args.bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = args.chat_id or os.getenv('TELEGRAM_CHAT_ID')
    
    if not bot_token:
        print("❌ Bot token not provided. Use --bot-token or set TELEGRAM_BOT_TOKEN environment variable")
        sys.exit(1)
    
    if not chat_id:
        print("❌ Chat ID not provided. Use --chat-id or set TELEGRAM_CHAT_ID environment variable")
        sys.exit(1)
    
    # Send message
    success = send_telegram_message(
        bot_token=bot_token,
        chat_id=chat_id,
        message_or_file=args.message,
        parse_mode=args.parse_mode,
        message_thread_id=args.message_thread_id,
        disable_web_page_preview=args.disable_web_page_preview,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay,
        delay=args.delay,
        photo_path=args.photo
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()