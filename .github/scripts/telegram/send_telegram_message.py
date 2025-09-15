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
import hashlib
from pathlib import Path

# Global cache for sent messages to avoid duplicates in the same session
_sent_messages_cache = set()


def send_telegram_message(bot_token, chat_id, message_or_file, parse_mode="Markdown", message_thread_id=None, disable_web_page_preview=True, max_retries=5, retry_delay=10, delay=1):
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
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Check if message_or_file is a file path
    file_path = Path(message_or_file)
    if file_path.exists() and file_path.is_file():
        # It's a file, read its content
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"❌ Error reading file {file_path}: {e}")
            return False
        
        if not content.strip():
            print(f"⚠️ File {file_path} is empty")
            return True
        
        # Split message into chunks if needed
        chunks = split_message(content)
        print(f"📤 Sending {len(chunks)} message(s) from file {file_path}...")
        
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
    else:
        # It's a text message, send directly
        return _send_single_message(bot_token, chat_id, message_or_file, parse_mode, message_thread_id, disable_web_page_preview, max_retries, retry_delay)


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
    # Create a unique key for this message
    message_key = f"{chat_id}:{message_thread_id}:{hashlib.md5(message.encode('utf-8')).hexdigest()}"
    
    # Check local cache first (for same session duplicates)
    if message_key in _sent_messages_cache:
        print(f"⚠️ Message already sent in this session, skipping duplicate")
        return True
    
    # Check for recent duplicate messages before sending
    if _check_for_recent_duplicate(bot_token, chat_id, message, message_thread_id):
        print(f"⚠️ Similar message found recently in chat, skipping to avoid duplicate")
        _sent_messages_cache.add(message_key)  # Add to cache even if skipped
        return True
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
            response = requests.post(url, data=data, timeout=60)  # Increased timeout
            
            # Always print response for debugging
            print(f"🔍 Telegram API Response: {response.status_code}")
            
            if response.status_code == 429:
                # Handle rate limiting specifically
                result = response.json()
                retry_after = result.get('parameters', {}).get('retry_after', retry_delay)
                print(f"❌ HTTP Error 429: Rate limited. Retry after {retry_after} seconds")
                if attempt < max_retries:
                    print(f"⏳ Waiting {retry_after} seconds before retry...")
                    time.sleep(retry_after)
                    continue
                else:
                    return False
            elif response.status_code != 200:
                print(f"❌ HTTP Error {response.status_code}: {response.text}")
                if attempt < max_retries:
                    # Use exponential backoff for server errors (5xx)
                    wait_time = retry_delay * (2 ** attempt) if response.status_code >= 500 else retry_delay
                    print(f"⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    return False
                
            result = response.json()
            if result.get('ok'):
                thread_info = f" (thread {message_thread_id})" if message_thread_id is not None else ""
                print(f"✅ Message sent successfully to chat {chat_id}{thread_info}")
                _sent_messages_cache.add(message_key)  # Add to cache on successful send
                return True
            else:
                print(f"❌ Telegram API Error: {result.get('description', 'Unknown error')}")
                print(f"❌ Full response: {result}")
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
    
    # If all retries failed, print the message details for logging
    print(f"❌ Failed to send message after {max_retries} retries.")
    print(f"📋 FAILED MESSAGE LOG:")
    print(f"   Chat ID: {chat_id}")
    if message_thread_id:
        print(f"   Thread ID: {message_thread_id}")
    print(f"   Message Hash: {hashlib.md5(message.encode('utf-8')).hexdigest()[:8]}")
    print("   Message content:")
    print("=" * 80)
    print(message)
    print("=" * 80)
    
    return False


def _check_for_recent_duplicate(bot_token, chat_id, message, message_thread_id=None, limit=10):
    """
    Check if a similar message was sent recently to avoid duplicates.
    
    Args:
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        message (str): Message to check
        message_thread_id (int, optional): Thread ID for group messages
        limit (int): Number of recent messages to check
        
    Returns:
        bool: True if duplicate found, False otherwise
    """
    try:
        # Get recent messages from the chat
        url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        params = {'limit': limit, 'timeout': 5}
        
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            # If we can't check, assume no duplicate to avoid blocking legitimate messages
            return False
            
        result = response.json()
        if not result.get('ok'):
            return False
            
        # Create a hash of the current message for comparison
        message_hash = hashlib.md5(message.encode('utf-8')).hexdigest()[:8]
        
        # Check recent messages for similar content
        for update in result.get('result', []):
            if 'message' in update:
                msg = update['message']
                
                # Check if it's from the same chat and thread
                if str(msg.get('chat', {}).get('id')) != str(chat_id):
                    continue
                    
                if message_thread_id is not None:
                    if msg.get('message_thread_id') != message_thread_id:
                        continue
                
                # Check if message content is similar
                msg_text = msg.get('text', '')
                if msg_text:
                    msg_hash = hashlib.md5(msg_text.encode('utf-8')).hexdigest()[:8]
                    if message_hash == msg_hash:
                        print(f"🔍 Found duplicate message hash: {message_hash}")
                        return True
                        
        return False
        
    except Exception as e:
        print(f"⚠️ Could not check for duplicates: {e}")
        # If we can't check, assume no duplicate to avoid blocking legitimate messages
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
    parser.add_argument('--parse-mode', default='Markdown', choices=['Markdown', 'HTML', 'None'], 
                       help='Parse mode for message formatting (default: Markdown)')
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
        delay=args.delay
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()