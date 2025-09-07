#!/usr/bin/env python3
"""
Script to parse GitHub issues results and send separate messages for each team.
"""

import os
import sys
import argparse
import re
import json
from pathlib import Path


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
    # Only escape characters that can break Markdown parsing outside of code blocks
    # Don't escape characters that are essential for Markdown links: [ ] ( )
    # Don't escape common filename characters: _ . - + 
    # Don't escape characters inside backticks as they are already protected
    
    # Escape only the most problematic characters that can break parsing
    # Be very conservative - only escape what absolutely breaks parsing
    special_chars = ['*', '~', '>', '#', '=', '|', '{', '}', '!', '+', '[', ']']
    
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
    
    # Add team responsible mention in the same line as title if available
    if team_responsible and team_name in team_responsible:
        responsible = team_responsible[team_name]
        # Handle both single responsible and list of responsibles
        if isinstance(responsible, list):
            responsible_str = " ".join(f"@{r}" if not r.startswith('@') else r for r in responsible)
        else:
            responsible_str = f"@{responsible}" if not responsible.startswith('@') else responsible
        message = f"🆕 **New muted tests for [{team_name}](https://github.com/orgs/ydb-platform/teams/{team_name})** {responsible_str}\n\n"
    else:
        message = f"🆕 **New muted tests for [{team_name}](https://github.com/orgs/ydb-platform/teams/{team_name})**\n\n"
    
    for issue in issues:
        # Escape the title for Markdown and wrap in backticks
        escaped_title = escape_markdown(issue['title'])
        message += f" - 🎯 [{issue['url']}]({issue['url']}) - `{escaped_title}`\n"
    
    # Add empty line at the end for better readability
    message += "\n"
    
    return message


def send_team_messages(teams, bot_token, chat_id, delay=2, team_responsible=None, message_thread_id=None):
    """
    Send separate messages for each team.
    
    Args:
        teams (dict): Dictionary with team names and their issues
        bot_token (str): Telegram bot token
        chat_id (str): Telegram chat ID
        delay (int): Delay between messages in seconds
        team_responsible (dict): Dictionary mapping team names to responsible usernames
        message_thread_id (int, optional): Thread ID for group messages
    """
    from .send_telegram_message import send_telegram_message
    import time
    
    total_teams = len(teams)
    sent_count = 0
    
    print(f"📤 Sending messages for {total_teams} teams...")
    
    for team_name, issues in teams.items():
        if not issues:
            continue
            
        message = format_team_message(team_name, issues, team_responsible)
        
        if not message.strip():
            continue
        
        print(f"📨 Sending message for team: {team_name} ({len(issues)} issues)")
        
        if send_telegram_message(bot_token, chat_id, message, "Markdown", message_thread_id):
            sent_count += 1
            print(f"✅ Message sent for team: {team_name}")
        else:
            print(f"❌ Failed to send message for team: {team_name}")
        
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
    import requests
    
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


def load_team_responsible(team_responsible_json):
    """
    Load team responsible mapping from JSON string or file.
    
    Args:
        team_responsible_json (str): JSON string or path to JSON file
        
    Returns:
        dict: Dictionary mapping team names to responsible usernames
    """
    if not team_responsible_json:
        return None
    
    try:
        # Try to parse as JSON string first
        if team_responsible_json.strip().startswith('{'):
            return json.loads(team_responsible_json)
        else:
            # Try to read as file
            file_path = Path(team_responsible_json)
            if file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                print(f"⚠️ Team responsible file not found: {file_path}")
                return None
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing team responsible JSON: {e}")
        return None
    except Exception as e:
        print(f"❌ Error loading team responsible: {e}")
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
    parser.add_argument('--team-responsible', help='JSON string or path to JSON file with team responsible mapping (or use TEAM_RESPONSIBLE env var)')
    parser.add_argument('--message-thread-id', type=int, help='Thread ID for group messages (optional)')
    
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
    
    # Get team responsible
    team_responsible_json = getattr(args, 'team_responsible', None) or os.getenv('TEAM_RESPONSIBLE')
    team_responsible = load_team_responsible(team_responsible_json)
    
    # Show team responsible status
    if not team_responsible:
        print("⚠️ No team responsible loaded")
    
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
        if team_responsible and team_name in team_responsible:
            responsible = team_responsible[team_name]
            if isinstance(responsible, list):
                responsible_str = ", ".join(responsible)
            else:
                responsible_str = responsible
            responsible_info = f" (Responsible: {responsible_str})"
        print(f"  - {team_name}: {len(issues)} issues{responsible_info}")
    
    if args.dry_run:
        print("\n🔍 Dry run - showing formatted messages:")
        for team_name, issues in teams.items():
            if issues:
                message = format_team_message(team_name, issues, team_responsible)
                print(f"\n--- Team: {team_name} ---")
                print(message)
        return
    
    # Send messages
    send_team_messages(teams, bot_token, chat_id, args.delay, team_responsible, thread_id)


if __name__ == "__main__":
    main()
