#!/usr/bin/env python3
"""
Script to parse GitHub issues results and send separate messages for each team.
"""

import os
import sys
import time
import requests
import argparse
import re
import json
from datetime import datetime
from pathlib import Path
from send_telegram_message import send_telegram_message


def parse_team_issues(content):
    """
    Parse team issues from the formatted results.
    
    Args:
        content (str): Formatted results content
        
    Returns:
        dict: Dictionary with team names as keys and their issues as values
    """
    teams = {}
    current_team = None
    current_issues = []
    
    lines = content.split('\n')
    
    for line in lines:
        line = line.strip()
        
        # Check for team header
        if line.startswith('👥 **TEAM** @ydb-platform/'):
            # Save previous team if exists
            if current_team and current_issues:
                teams[current_team] = current_issues.copy()
            
            # Start new team
            current_team = line.replace('👥 **TEAM** @ydb-platform/', '').strip()
            current_issues = []
            
        # Check for issue line
        elif line.startswith('🎯 ') and current_team:
            # Extract issue URL and title
            issue_match = re.match(r'🎯 (https://github\.com/[^\s]+) - `([^`]+)`', line)
            if issue_match:
                issue_url = issue_match.group(1)
                issue_title = issue_match.group(2)
                current_issues.append({
                    'url': issue_url,
                    'title': issue_title
                })
    
    # Save last team
    if current_team and current_issues:
        teams[current_team] = current_issues
    
    return teams


def escape_markdown(text):
    """
    Escape special Markdown characters for Telegram, but preserve link structure.
    
    Args:
        text (str): Text to escape
        
    Returns:
        str: Escaped text
    """
    special_chars = ['*', '~', '>', '#', '=', '|', '{', '}', '!']
    
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text


def format_team_message(team_name, issues, team_responsible=None):
    """
    Format message for a specific team.
    
    Args:
        team_name (str): Team name
        issues (list): List of issues for the team
        team_responsible (dict): Dictionary mapping team names to responsible usernames
        
    Returns:
        str: Formatted message
    """
    if not issues:
        return ""
    
    # Get current date in DD-MM-YY format
    current_date = datetime.now().strftime("%d-%m-%y")
    
    # Start with title and team tag (replace - with _ in tag)
    team_tag = team_name.replace('-', '')
    message = f"🔇 **{current_date} new muted tests for [{team_name}](https://github.com/orgs/ydb-platform/teams/{team_name})** #{team_tag}\n\n"
    
    # Add responsible users on new line with "fyi:" prefix
    if team_responsible and team_name in team_responsible:
        responsible = team_responsible[team_name]
        # Handle both single responsible and list of responsibles
        if isinstance(responsible, list):
            responsible_str = " ".join(f"@{r.replace('_', '\\_')}" if not r.startswith('@') else r.replace('_', '\\_') for r in responsible)
        else:
            responsible_str = f"@{responsible.replace('_', '\\_')}" if not responsible.startswith('@') else responsible.replace('_', '\\_')
        message += f"fyi: {responsible_str}\n\n"
    
    for issue in issues:
        # Escape the title for Markdown and wrap in backticks
        escaped_title = escape_markdown(issue['title'])
        message += f" - 🎯 [{issue['url']}]({issue['url']}) - `{escaped_title}`\n"
    
    # Add empty line at the end for better readability
    message += "\n"
    
    return message


def send_team_messages(teams, bot_token, chat_id, delay=2, message_thread_id=None, max_retries=5, retry_delay=10, team_channels=None):
    """
    Send separate messages for each team.
    
    Args:
        teams (dict): Dictionary with team names and their issues
        bot_token (str): Telegram bot token
        chat_id (str): Default Telegram chat ID
        delay (int): Delay between messages in seconds
        message_thread_id (int, optional): Default thread ID for group messages
        max_retries (int): Maximum number of retry attempts for failed messages
        retry_delay (int): Delay between retry attempts in seconds
        team_channels (dict): Dictionary mapping team names to their specific channel configs
    """
    
    total_teams = len(teams)
    sent_count = 0
    
    print(f"📤 Sending messages for {total_teams} teams...")
    
    for team_name, issues in teams.items():
        if not issues:
            continue
            
        # Get responsible users and channel from team_channels
        team_responsible = None
        team_chat_id = chat_id
        team_thread_id = message_thread_id
        
        if team_channels and 'teams' in team_channels and team_name in team_channels['teams']:
            team_config = team_channels['teams'][team_name]
            
            # Get responsible users
            if 'responsible' in team_config:
                team_responsible = {team_name: team_config['responsible']}
            
            # Get channel
            if 'channel' in team_config:
                channel_name = team_config['channel']
                if 'channels' in team_channels and channel_name in team_channels['channels']:
                    team_chat_id, team_thread_id = parse_chat_and_thread_id(team_channels['channels'][channel_name])
                    print(f"📨 Using channel '{channel_name}' for team {team_name}: {team_chat_id}" + (f" (thread {team_thread_id})" if team_thread_id else ""))
                else:
                    print(f"⚠️ Channel '{channel_name}' not found in channels config, using fallback")
            else:
                print(f"📨 No channel specified for team {team_name}, using fallback")
        elif team_channels and 'default_channel' in team_channels:
            # Use default channel from configuration
            default_channel_name = team_channels['default_channel']
            if 'channels' in team_channels and default_channel_name in team_channels['channels']:
                team_chat_id, team_thread_id = parse_chat_and_thread_id(team_channels['channels'][default_channel_name])
                print(f"📨 Using default channel '{default_channel_name}' for team {team_name}: {team_chat_id}" + (f" (thread {team_thread_id})" if team_thread_id else ""))
            else:
                print(f"⚠️ Default channel '{default_channel_name}' not found, using fallback")
        else:
            print(f"📨 Using fallback channel for team {team_name}")
        
        message = format_team_message(team_name, issues, team_responsible)
        
        if not message.strip():
            continue
        
        print(f"📨 Sending message for team: {team_name} ({len(issues)} issues)")
        
        if send_telegram_message(bot_token, team_chat_id, message, "Markdown", team_thread_id, True, max_retries, retry_delay):
            sent_count += 1
            print(f"✅ Message sent for team: {team_name}")
        else:
            print(f"❌ Failed to send message for team: {team_name} after {max_retries} retries")
        
        # Add delay between messages
        if sent_count < total_teams:
            time.sleep(delay)
    
    print(f"🎉 Sent {sent_count}/{total_teams} team messages successfully!")


def parse_chat_and_thread_id(chat_id_str):
    """
    Parse chat ID and thread ID from string format like "2018419243/1".
    
    Args:
        chat_id_str (str): String in format "chat_id/thread_id" or just "chat_id"
        
    Returns:
        tuple: (chat_id, thread_id) where thread_id is None if not provided or if thread_id is 1 (main thread)
    """
    if '/' in chat_id_str:
        chat_id_part, thread_id_part = chat_id_str.split('/', 1)
        # Add 100 prefix to chat_id for supergroup
        chat_id = f"-100{chat_id_part}"
        thread_id = int(thread_id_part)
        
        # If thread_id is 1, it means main thread (no thread)
        if thread_id == 1:
            return chat_id, None
        else:
            return chat_id, thread_id
    else:
        # Add 100 prefix to chat_id for supergroup
        chat_id = f"-100{chat_id_str}"
        return chat_id, None


def test_telegram_connection(bot_token, chat_id, message_thread_id=None):
    """
    Test Telegram connection without sending messages.
    
    Args:
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        message_thread_id (int, optional): Thread ID to test
        
    Returns:
        bool: True if connection is valid
    """
    
    print(f"🔍 Testing Telegram connection to chat {chat_id}...")
    if message_thread_id:
        print(f"🔍 Testing thread {message_thread_id}...")
    
    # Test with getChat method instead of sending a message
    url = f"https://api.telegram.org/bot{bot_token}/getChat"
    data = {'chat_id': chat_id}
    
    if message_thread_id:
        data['message_thread_id'] = message_thread_id
    
    try:
        response = requests.post(url, data=data, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        if result.get('ok'):
            print("✅ Telegram connection successful!")
            return True
        else:
            print(f"❌ Telegram connection failed: {result.get('description', 'Unknown error')}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Telegram connection failed: {e}")
        return False


def load_team_channels(team_channels_json):
    """
    Load team channels configuration from JSON string or file.
    
    Args:
        team_channels_json (str): JSON string or path to JSON file
        
    Returns:
        dict: Dictionary mapping team names to their channel configurations
    """
    if not team_channels_json:
        return None
    
    try:
        # Try to parse as JSON string first
        if team_channels_json.strip().startswith('{'):
            return json.loads(team_channels_json)
        else:
            # Try to read as file
            file_path = Path(team_channels_json)
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                print(f"⚠️ Team channels file not found: {file_path}")
                return None
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing team channels JSON: {e}")
        return None
    except Exception as e:
        print(f"❌ Error loading team channels: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Parse team issues and send separate messages for each team")
    
    # Required arguments
    parser.add_argument('--file', required=True, help='Path to file with formatted results')
    parser.add_argument('--bot-token', help='Telegram bot token (or use TELEGRAM_BOT_TOKEN env var)')
    parser.add_argument('--chat-id', help='Telegram chat ID (or use TELEGRAM_CHAT_ID env var)')
    
    # Optional arguments
    parser.add_argument('--delay', type=int, default=2, help='Delay between messages in seconds (default: 2)')
    parser.add_argument('--dry-run', action='store_true', help='Parse and show teams without sending messages')
    parser.add_argument('--test-connection', action='store_true', help='Test Telegram connection only')
    parser.add_argument('--team-channels', help='JSON string or path to JSON file with team channel configurations (or use TEAM_CHANNELS env var)')
    parser.add_argument('--message-thread-id', type=int, help='Thread ID for group messages (optional)')
    parser.add_argument('--max-retries', type=int, default=5, help='Maximum number of retry attempts for failed messages (default: 5)')
    parser.add_argument('--retry-delay', type=int, default=10, help='Delay between retry attempts in seconds (default: 10)')
    
    args = parser.parse_args()
    
    # Get bot token and chat ID
    bot_token = args.bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id_str = args.chat_id or os.getenv('TELEGRAM_CHAT_ID')
    
    # Parse chat ID and thread ID
    if chat_id_str:
        chat_id, thread_id = parse_chat_and_thread_id(chat_id_str)
        # Override thread_id if explicitly provided via argument
        if args.message_thread_id is not None:
            thread_id = args.message_thread_id
    else:
        chat_id = None
        thread_id = None
    
    # Get team channels
    team_channels_json = getattr(args, 'team_channels', None) or os.getenv('TEAM_CHANNELS')
    team_channels = load_team_channels(team_channels_json)
    
    # Show team channels status
    if not team_channels:
        print("⚠️ No team channels loaded, using default channel for all teams")
    else:
        print(f"📋 Loaded channel configurations for {len(team_channels)} teams")
    
    # Check if we need Telegram connection (not for dry run)
    if not args.dry_run or args.test_connection:
        if not bot_token:
            print("❌ Bot token not provided. Use --bot-token or set TELEGRAM_BOT_TOKEN environment variable")
            sys.exit(1)
        
        if not chat_id:
            print("❌ Chat ID not provided. Use --chat-id or set TELEGRAM_CHAT_ID environment variable")
            sys.exit(1)
        
        # Test connection and thread
        if not test_telegram_connection(bot_token, chat_id, thread_id):
            if thread_id is not None:
                print(f"⚠️ Thread {thread_id} not found, trying without thread...")
                if test_telegram_connection(bot_token, chat_id, None):
                    print("✅ Connection successful without thread, continuing...")
                    thread_id = None
                else:
                    print("❌ Connection failed even without thread")
                    sys.exit(1)
            else:
                print("❌ Connection failed")
                sys.exit(1)
        
        # If this was just a connection test, exit here
        if args.test_connection:
            print("✅ Connection test successful!")
            sys.exit(0)
    
    # Read file
    file_path = Path(args.file)
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        sys.exit(1)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"❌ Error reading file {file_path}: {e}")
        sys.exit(1)
    
    # Parse teams
    teams = parse_team_issues(content)
    
    if not teams:
        print("⚠️ No teams found in the file")
        sys.exit(0)
    
    print(f"📋 Found {len(teams)} teams:")
    for team_name, issues in teams.items():
        responsible_info = ""
        channel_info = ""
        
        if team_channels and 'teams' in team_channels and team_name in team_channels['teams']:
            team_config = team_channels['teams'][team_name]
            
            # Get responsible info
            if 'responsible' in team_config:
                responsible = team_config['responsible']
                if isinstance(responsible, list):
                    responsible_str = ", ".join(responsible)
                else:
                    responsible_str = responsible
                responsible_info = f" (Responsible: {responsible_str})"
            
            # Get channel info
            if 'channel' in team_config:
                channel_name = team_config['channel']
                if 'channels' in team_channels and channel_name in team_channels['channels']:
                    channel_info = f" (Channel: {channel_name} -> {team_channels['channels'][channel_name]})"
                else:
                    channel_info = f" (Channel: {channel_name} - not found)"
            else:
                channel_info = " (Channel: default)"
        elif team_channels and 'default_channel' in team_channels:
            channel_info = f" (Channel: {team_channels['default_channel']} - default)"
        else:
            channel_info = " (Channel: fallback)"
        
        print(f"  - {team_name}: {len(issues)} issues{responsible_info}{channel_info}")
    
    if args.dry_run:
        print("\n🔍 Dry run - showing formatted messages:")
        for team_name, issues in teams.items():
            if issues:
                # Get responsible users from team_channels
                team_responsible = None
                if team_channels and 'teams' in team_channels and team_name in team_channels['teams']:
                    team_config = team_channels['teams'][team_name]
                    if 'responsible' in team_config:
                        team_responsible = {team_name: team_config['responsible']}
                
                message = format_team_message(team_name, issues, team_responsible)
                print(f"\n--- Team: {team_name} ---")
                print(message)
        return
    
    # Send messages
    send_team_messages(teams, bot_token, chat_id, args.delay, thread_id, args.max_retries, args.retry_delay, team_channels)


if __name__ == "__main__":
    main()
