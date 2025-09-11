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


def get_team_config(team_name, team_channels):
    """
    Get configuration for a team (responsible users and channel).
    
    Args:
        team_name (str): Team name
        team_channels (dict): Team channels configuration
        
    Returns:
        tuple: (team_responsible, team_chat_id, team_thread_id) or (None, None, None) if not found
    """
    if not team_channels:
        return None, None, None
    
    # Get default channel first
    default_channel_name = team_channels.get('default_channel')
    default_chat_id, default_thread_id = None, None
    if default_channel_name and 'channels' in team_channels:
        if default_channel_name in team_channels['channels']:
            default_chat_id, default_thread_id = parse_chat_and_thread_id(team_channels['channels'][default_channel_name])
    
    # Try to find team in teams config
    if 'teams' in team_channels and team_name in team_channels['teams']:
        team_config = team_channels['teams'][team_name]
        
        # Get responsible users
        team_responsible = None
        if 'responsible' in team_config:
            team_responsible = {team_name: team_config['responsible']}
        
        # Get channel (team-specific or default)
        team_chat_id, team_thread_id = default_chat_id, default_thread_id
        if 'channel' in team_config:
            channel_name = team_config['channel']
            if 'channels' in team_channels and channel_name in team_channels['channels']:
                team_chat_id, team_thread_id = parse_chat_and_thread_id(team_channels['channels'][channel_name])
                print(f"📨 Using channel '{channel_name}' for team {team_name}: {team_chat_id}" + (f" (thread {team_thread_id})" if team_thread_id else ""))
            else:
                print(f"❌ Channel '{channel_name}' not found in channels config")
                return None, None, None
        else:
            if default_chat_id:
                print(f"📨 Using default channel '{default_channel_name}' for team {team_name}: {default_chat_id}" + (f" (thread {default_thread_id})" if default_thread_id else ""))
            else:
                print(f"❌ No channel specified for team {team_name} and no default channel")
                return None, None, None
        
        return team_responsible, team_chat_id, team_thread_id
    
    # Try Unknown team as fallback
    elif 'teams' in team_channels and 'Unknown' in team_channels['teams']:
        unknown_config = team_channels['teams']['Unknown']
        
        # Get responsible users from Unknown team
        team_responsible = None
        if 'responsible' in unknown_config:
            team_responsible = {team_name: unknown_config['responsible']}
        
        # Use default channel or Unknown team's channel
        if default_chat_id:
            print(f"📨 Using default channel '{default_channel_name}' for unknown team {team_name}: {default_chat_id}" + (f" (thread {default_thread_id})" if default_thread_id else ""))
            return team_responsible, default_chat_id, default_thread_id
        elif 'channel' in unknown_config:
            # Try Unknown team's specific channel
            channel_name = unknown_config['channel']
            if 'channels' in team_channels and channel_name in team_channels['channels']:
                team_chat_id, team_thread_id = parse_chat_and_thread_id(team_channels['channels'][channel_name])
                print(f"📨 Using Unknown team channel '{channel_name}' for team {team_name}: {team_chat_id}" + (f" (thread {team_thread_id})" if team_thread_id else ""))
                return team_responsible, team_chat_id, team_thread_id
            else:
                print(f"❌ Unknown team channel '{channel_name}' not found")
                return None, None, None
        else:
            print(f"❌ No channel configuration found for unknown team {team_name}")
            return None, None, None
    
    # No configuration found
    else:
        print(f"❌ No channel configuration found for team {team_name}")
        return None, None, None


def send_team_messages(teams, bot_token, delay=2, max_retries=5, retry_delay=10, team_channels=None, dry_run=False):
    """
    Send separate messages for each team.
    
    Args:
        teams (dict): Dictionary with team names and their issues
        bot_token (str): Telegram bot token
        delay (int): Delay between messages in seconds
        max_retries (int): Maximum number of retry attempts for failed messages
        retry_delay (int): Delay between retry attempts in seconds
        team_channels (dict): Dictionary mapping team names to their specific channel configs
        dry_run (bool): If True, only print messages without sending to Telegram
    """
    
    total_teams = len(teams)
    sent_count = 0
    
    if dry_run:
        print(f"🔍 Dry run - showing formatted messages for {total_teams} teams...")
    else:
        print(f"📤 Sending messages for {total_teams} teams...")
    
    for team_name, issues in teams.items():
        if not issues:
            continue
        
        # Get team configuration
        team_responsible, team_chat_id, team_thread_id = get_team_config(team_name, team_channels)
        
        if not team_chat_id:
            if dry_run:
                print(f"\n--- Team: {team_name} ---")
                print("❌ No channel configuration found - skipping")
            continue
        
        # Format message
        message = format_team_message(team_name, issues, team_responsible)
        
        if not message.strip():
            continue
        
        if dry_run:
            print(f"\n--- Team: {team_name} ---")
            print(f"📨 Channel: {team_chat_id}" + (f" (thread {team_thread_id})" if team_thread_id else ""))
            print(message)
            sent_count += 1
        else:
            print(f"📨 Sending message for team: {team_name} ({len(issues)} issues)")
            
            if send_telegram_message(bot_token, team_chat_id, message, "Markdown", team_thread_id, True, max_retries, retry_delay):
                sent_count += 1
                print(f"✅ Message sent for team: {team_name}")
            else:
                print(f"❌ Failed to send message for team: {team_name} after {max_retries} retries")
            
            # Add delay between messages
            if sent_count < total_teams:
                time.sleep(delay)
    
    if dry_run:
        print(f"🎉 Dry run completed: {sent_count}/{total_teams} team messages formatted!")
    else:
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
    parser.add_argument('--team-channels', required=True, help='JSON string mapping teams to their channel configurations (or use TEAM_CHANNELS env var)')
    
    # Optional arguments
    parser.add_argument('--delay', type=int, default=2, help='Delay between messages in seconds (default: 2)')
    parser.add_argument('--dry-run', action='store_true', help='Parse and show teams without sending messages')
    parser.add_argument('--test-connection', action='store_true', help='Test Telegram connection only')
    parser.add_argument('--message-thread-id', type=int, help='Thread ID for group messages (optional)')
    parser.add_argument('--max-retries', type=int, default=5, help='Maximum number of retry attempts for failed messages (default: 5)')
    parser.add_argument('--retry-delay', type=int, default=10, help='Delay between retry attempts in seconds (default: 10)')
    
    args = parser.parse_args()
    
    # Get bot token
    bot_token = args.bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
    
    # Get team channels
    team_channels_json = args.team_channels or os.getenv('TEAM_CHANNELS')
    team_channels = load_team_channels(team_channels_json)
    
    # Validate configuration
    if not team_channels:
        print("❌ Team channels configuration is required")
        print("   Use --team-channels parameter or set TEAM_CHANNELS environment variable")
        sys.exit(1)
    
    print(f"📋 Loaded channel configurations for {len(team_channels.get('teams', {}))} teams")
    
    # Check if we need Telegram connection (not for dry run)
    if not args.dry_run or args.test_connection:
        if not bot_token:
            print("❌ Bot token not provided. Use --bot-token or set TELEGRAM_BOT_TOKEN environment variable")
            sys.exit(1)
        
        # Test connection for each team's channel
        if args.test_connection:
            print("🔍 Testing connections for all team channels...")
            for team_name, team_config in team_channels.get('teams', {}).items():
                if 'channel' in team_config:
                    channel_name = team_config['channel']
                    if 'channels' in team_channels and channel_name in team_channels['channels']:
                        chat_id, thread_id = parse_chat_and_thread_id(team_channels['channels'][channel_name])
                        if test_telegram_connection(bot_token, chat_id, thread_id):
                            print(f"✅ Connection successful for team {team_name} (channel {channel_name})")
                        else:
                            print(f"❌ Connection failed for team {team_name} (channel {channel_name})")
            print("✅ Connection test completed!")
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
    
    # Send messages (or show in dry run)
    send_team_messages(teams, bot_token, args.delay, args.max_retries, args.retry_delay, team_channels, args.dry_run)


if __name__ == "__main__":
    main()
