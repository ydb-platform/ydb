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
from pathlib import Path


def send_telegram_message(bot_token, chat_id, message, parse_mode="Markdown", message_thread_id=None, disable_web_page_preview=True):
    """
    Send a message to Telegram channel.
    
    Args:
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        message (str): Message to send
        parse_mode (str): Parse mode for message formatting
        message_thread_id (int, optional): Thread ID for group messages
        disable_web_page_preview (bool): Disable web page preview for links
        
    Returns:
        bool: True if successful, False otherwise
    """
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
            return False
            
        result = response.json()
        if result.get('ok'):
            thread_info = f" (thread {message_thread_id})" if message_thread_id is not None else ""
            print(f"✅ Message sent successfully to chat {chat_id}{thread_info}")
            return True
        else:
            print(f"❌ Telegram API Error: {result.get('description', 'Unknown error')}")
            print(f"❌ Full response: {result}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
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


def send_file_to_telegram(bot_token, chat_id, file_path, parse_mode="Markdown", delay=1, message_thread_id=None, disable_web_page_preview=True):
    """
    Send file contents to Telegram channel.
    
    Args:
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        file_path (str): Path to file to send
        parse_mode (str): Parse mode for message formatting
        delay (int): Delay between messages in seconds
        message_thread_id (int, optional): Thread ID for group messages
        disable_web_page_preview (bool): Disable web page preview for links
        
    Returns:
        bool: True if all messages sent successfully, False otherwise
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return False
    
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
    
    print(f"📤 Sending {len(chunks)} message(s) to Telegram...")
    
    success_count = 0
    for i, chunk in enumerate(chunks, 1):
        print(f"📨 Sending chunk {i}/{len(chunks)}...")
        
        if send_telegram_message(bot_token, chat_id, chunk, parse_mode, message_thread_id, disable_web_page_preview):
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


def main():
    parser = argparse.ArgumentParser(description="Send messages to Telegram channel")
    
    # Required arguments
    parser.add_argument('--bot-token', required=True, help='Telegram bot token')
    parser.add_argument('--chat-id', required=True, help='Telegram chat ID')
    
    # Message source (either file or direct message)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file', help='Path to file to send')
    group.add_argument('--message', help='Direct message to send')
    
    # Optional arguments
    parser.add_argument('--parse-mode', default='Markdown', choices=['Markdown', 'HTML', 'None'], 
                       help='Parse mode for message formatting (default: Markdown)')
    parser.add_argument('--delay', type=int, default=1, 
                       help='Delay between messages in seconds (default: 1)')
    parser.add_argument('--max-length', type=int, default=4000, 
                       help='Maximum message length (default: 4000)')
    parser.add_argument('--message-thread-id', type=int, 
                       help='Thread ID for group messages (optional)')
    parser.add_argument('--disable-web-page-preview', action='store_true', default=True,
                       help='Disable web page preview for links (default: True)')
    
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
    if args.file:
        success = send_file_to_telegram(
            bot_token=bot_token,
            chat_id=chat_id,
            file_path=args.file,
            parse_mode=args.parse_mode,
            delay=args.delay,
            message_thread_id=args.message_thread_id,
            disable_web_page_preview=args.disable_web_page_preview
        )
    else:
        success = send_telegram_message(
            bot_token=bot_token,
            chat_id=chat_id,
            message=args.message,
            parse_mode=args.parse_mode,
            message_thread_id=args.message_thread_id,
            disable_web_page_preview=args.disable_web_page_preview
        )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
