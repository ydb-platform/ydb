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
import configparser
import tempfile
import base64
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from send_telegram_message import send_telegram_message

# Add analytics directory to path for ydb_wrapper import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'analytics'))
try:
    from ydb_wrapper import YDBWrapper
    YDB_AVAILABLE = True
except ImportError as e:
    YDB_AVAILABLE = False
    print(f"⚠️ YDBWrapper not available: {e}")

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("⚠️ Matplotlib not available. Install with: pip install matplotlib")

# Configuration constants
MUTE_UPDATE_SHOW_DIFF = False  # Set to True to show +/- statistics in mute update messages

# Teams blacklisted from weekly/monthly updates
PERIOD_UPDATE_BLACKLIST = {
    'storage'  # Add team names that should not receive periodic updates
}

# URL constants (only for duplicated URLs)
GITHUB_ORG_TEAMS_URL = "https://github.com/orgs/ydb-platform/teams"
DATALENS_DASHBOARD_URL = "https://datalens.yandex/4un3zdm0zcnyr?owner_team={team_name}"


def _execute_ydb_query(query, description):
    """
    Execute a YDB query using YDBWrapper and return results.
    
    Args:
        query (str): SQL query to execute
        description (str): Description for logging
        
    Returns:
        list: Query results or None if error
    """
    if not YDB_AVAILABLE:
        print("❌ YDBWrapper not available")
        return None
    
    try:
        print(f"🔍 {description}")
        
        with YDBWrapper() as ydb_wrapper:
            if not ydb_wrapper.check_credentials():
                print("❌ YDB credentials check failed")
                return None
            
            print("✅ Successfully connected to YDB")
            results = ydb_wrapper.execute_scan_query(query, query_name=description)
            
            print(f"📊 Query returned {len(results)} rows")
            return results
        
    except Exception as e:
        print(f"❌ Error executing YDB query: {e}")
        print(f"❌ Error type: {type(e).__name__}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        return None


def get_all_team_data(use_yesterday=False):
    """
    Get all team data (stats + trends) from YDB in one optimized query.
    
    Args:
        use_yesterday (bool): If True, use yesterday's data for development convenience
        
    Returns:
        dict: Dictionary with team names as keys and their data, or None if error
    """
    if not YDB_AVAILABLE:
        print("❌ YDBWrapper not available")
        return None
    
    # Calculate target date
    if use_yesterday:
        target_date = datetime.now() - timedelta(days=1)
    else:
        target_date = datetime.now()
    
    yesterday_date = target_date - timedelta(days=1)
    start_date = target_date - timedelta(days=30)
    
    print(f"🔍 Date calculation:")
    print(f"   Current datetime.now(): {datetime.now()}")
    print(f"   use_yesterday: {use_yesterday}")
    print(f"   target_date: {target_date}")
    print(f"   start_date: {start_date}")
    print(f"   yesterday_date: {yesterday_date}")
    
    # Get table path from config
    with YDBWrapper() as ydb_wrapper:
        test_muted_monitor_mart_table = ydb_wrapper.get_table_path("test_muted_monitor_mart")
    
    # Single optimized query for all data
    all_data_query = f"""
    SELECT 
        owner,
        date_window,
        COUNT(*) as daily_count,
        SUM(CASE WHEN mute_state_change_date = Date('{target_date.strftime('%Y-%m-%d')}') THEN 1 ELSE 0 END) as today_count
    FROM `{test_muted_monitor_mart_table}`
    WHERE date_window >= Date('{start_date.strftime('%Y-%m-%d')}')
    AND date_window <= Date('{target_date.strftime('%Y-%m-%d')}')
    AND is_muted = 1
    AND branch = 'main'
    AND build_type = 'relwithdebinfo'
    AND is_test_chunk = 0
    AND resolution != 'Skipped'
    GROUP BY owner, date_window
    ORDER BY owner, date_window
    """
    
    # Execute query
    print(f"🔍 Query details:")
    print(f"   Start date: {start_date.strftime('%Y-%m-%d')}")
    print(f"   Target date: {target_date.strftime('%Y-%m-%d')}")
    
    results = _execute_ydb_query(all_data_query, f"Getting all team data from {start_date.strftime('%Y-%m-%d')} to {target_date.strftime('%Y-%m-%d')}")
    if results is None:
        return None
    
    # Process results
    team_data = {}
    base_date = datetime(1970, 1, 1)
    
    for row in results:
        owner = row.get('owner') if isinstance(row, dict) else row.owner
        if not owner:
            continue
            
        # Handle both "TEAM:@ydb-platform/teamname" and "Unknown" formats
        if owner.startswith('TEAM:@ydb-platform/'):
            team_name = owner.split('/')[-1]
        elif owner == 'Unknown':
            team_name = 'Unknown'
        else:
            # Skip other formats
            continue
        
        if team_name not in team_data:
            team_data[team_name] = {
                'stats': {'total': 0, 'today': 0, 'minus_today': 0},
                'trend': {}
            }
        
        # Convert days since epoch to date
        date_window = row.get('date_window') if isinstance(row, dict) else row.date_window
        date_obj = base_date + timedelta(days=date_window)
        date_str = date_obj.strftime('%Y-%m-%d')
        
        # Add to trend data
        daily_count = row.get('daily_count') if isinstance(row, dict) else row.daily_count
        team_data[team_name]['trend'][date_str] = daily_count
        
        # Update stats for target date
        if date_str == target_date.strftime('%Y-%m-%d'):
            team_data[team_name]['stats']['total'] = daily_count
            today_count = row.get('today_count') if isinstance(row, dict) else row.today_count
            team_data[team_name]['stats']['today'] = today_count
    
    # Calculate "minus today" for each team and fix total if needed
    for team_name, data in team_data.items():
        trend = data['trend']
        yesterday_str = yesterday_date.strftime('%Y-%m-%d')
        
        # If total is 0 but we have trend data, use the latest available value
        if data['stats']['total'] == 0 and trend:
            latest_date = max(trend.keys())
            data['stats']['total'] = trend[latest_date]
            print(f"🔍 Fixed total for team {team_name}: using {latest_date} value {trend[latest_date]}")
        
        if yesterday_str in trend:
            yesterday_total = trend[yesterday_str]
            today_total = data['stats']['total']
            today_new = data['stats']['today']
            
            # minus_today = yesterday_total - (today_total - today_new)
            data['stats']['minus_today'] = max(0, yesterday_total - (today_total - today_new))
    
    print(f"📊 Processed data for {len(team_data)} teams")
    return team_data


def get_muted_tests_stats(use_yesterday=False):
    """
    Get statistics about muted tests from YDB by team.
    
    Args:
        use_yesterday (bool): If True, use yesterday's data for development convenience
        
    Returns:
        dict: Dictionary with team names as keys and {'total': count, 'today': count} as values, or None if error
    """
    
    # Use the optimized function to get all data
    all_data = get_all_team_data(use_yesterday)
    if all_data is None:
        return None
    
    # Extract just the stats part
    team_stats = {}
    for team_name, data in all_data.items():
        team_stats[team_name] = data['stats']
    
    print(f"📊 Found statistics for {len(team_stats)} teams")
    return team_stats


def get_monthly_trend_data(team_name=None, use_yesterday=False):
    """
    Get monthly trend data for a specific team.
    
    Args:
        team_name (str): Team name to get data for
        use_yesterday (bool): If True, use yesterday as end date for development convenience
        
    Returns:
        dict: Dictionary with dates as keys and counts as values, or None if error
    """
    # Use the optimized function to get all data
    all_data = get_all_team_data(use_yesterday)
    if all_data is None:
        return None
    
    # Extract trend data for the specific team
    if team_name in all_data:
        trend_data = all_data[team_name]['trend']
        print(f"📊 Found trend data for {len(trend_data)} days for team '{team_name}'")
        return trend_data
    else:
        print(f"⚠️ No data found for team '{team_name}'")
        return None


def get_interval_dates(trend_data, period):
    """
    Get interval start and end dates for period visualization.
    
    Args:
        trend_data (dict): Dictionary with dates as keys and counts as values
        period (str): Period type ('week' or 'month')
        
    Returns:
        tuple: (interval_start_date, interval_end_date, interval_start_count, interval_end_count)
    """
    if not trend_data:
        return None, None, None, None
    
    available_dates = sorted(trend_data.keys())
    if not available_dates:
        return None, None, None, None
    
    # Calculate interval days
    interval_days = 7 if period == 'week' else 30
    
    # Use the last available date as end
    interval_end_date = available_dates[-1]
    interval_end_count = trend_data[interval_end_date]
    
    # Calculate interval start (period_days-1 days before end)
    end_date_obj = datetime.strptime(interval_end_date, '%Y-%m-%d')
    interval_start = end_date_obj - timedelta(days=interval_days-1)
    interval_start_str = interval_start.strftime('%Y-%m-%d')
    
    # Find the closest available date to interval start
    interval_start_count = None
    actual_interval_start_str = None
    for date_str in available_dates:
        if date_str >= interval_start_str:
            interval_start_count = trend_data[date_str]
            actual_interval_start_str = date_str
            break
    
    return actual_interval_start_str, interval_end_date, interval_start_count, interval_end_count


def create_trend_plot(team_name, trend_data, debug_dir=None, period=None):
    """
    Create a trend plot for muted tests.
    
    Args:
        team_name (str): Team name
        trend_data (dict): Dictionary with dates as keys and counts as values
        debug_dir (str): Directory to save debug plot files (if None, debug mode is disabled)
        period (str): Period type ('week' or 'month') for interval visualization
        
    Returns:
        str: Base64 encoded image data, or None if error
    """
    if not MATPLOTLIB_AVAILABLE:
        print("❌ Matplotlib not available for plotting")
        return None
    
    if not trend_data:
        print("⚠️ No trend data available for plotting")
        return None
    
    try:
        # Prepare data
        dates = []
        counts = []
        for date_str in sorted(trend_data.keys()):
            dates.append(datetime.strptime(date_str, '%Y-%m-%d'))
            counts.append(trend_data[date_str])
        
        # Create plot
        plt.figure(figsize=(10, 6))
        plt.plot(dates, counts, marker='o', linewidth=2, markersize=4)
        plt.title(f'Muted Tests Trend - {team_name}', fontsize=14, fontweight='bold')
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('Number of Muted Tests', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.ylim(bottom=0)  # Start y-axis from 0
        
        # Format x-axis
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=3))
        plt.xticks(rotation=45)
        
        # Add trend line
        if len(dates) > 1:
            x_numeric = np.arange(len(dates))
            z = np.polyfit(x_numeric, counts, 1)
            p = np.poly1d(z)
            plt.plot(dates, p(x_numeric), "r--", alpha=0.8, linewidth=2, label='Trend')
            plt.legend()
        
        # Add interval visualization for period reports
        if period and dates and counts:
            # Get interval dates using the shared function
            interval_start_str, interval_end_str, interval_start_count, interval_end_count = get_interval_dates(trend_data, period)
            
            if interval_start_str and interval_end_str:
                # Convert strings to datetime objects for plotting
                interval_start_date = datetime.strptime(interval_start_str, '%Y-%m-%d')
                interval_end_date = datetime.strptime(interval_end_str, '%Y-%m-%d')
                
                # Add vertical dotted lines for interval boundaries
                plt.axvline(x=interval_start_date, color='gray', linestyle='--', alpha=0.7, linewidth=1)
                plt.axvline(x=interval_end_date, color='gray', linestyle='--', alpha=0.7, linewidth=1)
                
                # Add annotation for the interval start point
                if interval_start_count is not None:
                    plt.annotate(f'{interval_start_count}', 
                                xy=(interval_start_date, interval_start_count), 
                                xytext=(10, 10), 
                                textcoords='offset points',
                                bbox=dict(boxstyle='round,pad=0.3', facecolor='lightblue', alpha=0.7),
                                arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
                                fontsize=10, fontweight='bold')
        
        # Add annotation for the last point
        if dates and counts:
            last_date = dates[-1]
            last_count = counts[-1]
            plt.annotate(f'{last_count}', 
                        xy=(last_date, last_count), 
                        xytext=(10, 10), 
                        textcoords='offset points',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                        arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'),
                        fontsize=10, fontweight='bold')
        
        plt.tight_layout()
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
            plt.savefig(tmp_file.name, dpi=150, bbox_inches='tight')
            tmp_path = tmp_file.name
        
        print(f"📈 Created trend plot for {team_name}: {tmp_path}")
        print(f"📁 File size: {os.path.getsize(tmp_path)} bytes")
        
        # Save to debug directory if requested
        if debug_dir:
            os.makedirs(debug_dir, exist_ok=True)
            debug_path = os.path.join(debug_dir, f"trend_{team_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            
            # Copy to debug directory
            shutil.copy2(tmp_path, debug_path)
            print(f"🔍 Debug plot saved to: {debug_path}")
        
        # Read and encode as base64
        with open(tmp_path, 'rb') as f:
            image_data = f.read()
        
        print(f"📊 Base64 data length: {len(base64.b64encode(image_data).decode('utf-8'))} characters")
        
        # Clean up temporary file (but keep debug file)
        os.unlink(tmp_path)
        plt.close()
        
        # Encode as base64
        base64_data = base64.b64encode(image_data).decode('utf-8')
        return base64_data
        
    except Exception as e:
        print(f"❌ Error creating trend plot: {e}")
        return None


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




def format_team_message(team_name, issues, team_responsible=None, muted_stats=None, show_diff=False):
    """
    Format message for a specific team.
    
    Args:
        team_name (str): Team name
        issues (list): List of issues for the team
        team_responsible (dict): Dictionary mapping team names to responsible usernames
        muted_stats (dict): Dictionary with team names as keys and {'total': count, 'today': count} as values
        show_diff (bool): Whether to show +/- statistics
        
    Returns:
        str: Formatted message
    """
    if not issues:
        return ""
    
    # Get current date in DD-MM-YY format
    current_date = datetime.now().strftime("%d-%m-%y")
    
    # Start with title and team tag (replace - with _ in tag)
    team_tag = team_name.replace('-', '')
    # Create team URL and escape it
    team_url = f"{GITHUB_ORG_TEAMS_URL}/{team_name}"
    message = f"🔇 *{current_date} new muted tests in `main` for [{team_name}]({team_url})* #{team_tag}\n\n"
    
    for issue in issues:
        # Extract issue number from URL for compact display
        issue_number = issue['url'].split('/')[-1] if '/' in issue['url'] else issue['url']
        
        # Remove "in main" from title if present and "Mute " prefix
        title = issue['title']
        if title.endswith(' in main'):
            title = title[:-8]  # Remove " in main"
        if title.startswith('Mute '):
            title = title[5:]  # Remove "Mute " prefix
        
        # Wrap title in backticks (will be escaped later with the whole message)
        message += f" 🎯 `{title}` [#{issue_number}]({issue['url']})\n"
    
    # Add muted tests statistics for this specific team if available (moved to end)
    if muted_stats and team_name in muted_stats:
        team_stats = muted_stats[team_name]
        total = team_stats['total']
        today = team_stats['today']
        minus_today = team_stats.get('minus_today', 0)
        
        # Create dashboard URL for the team
        dashboard_url = DATALENS_DASHBOARD_URL.format(team_name=team_name)
        
        # Format statistics with color coding and emojis
        if show_diff:
            if today > 0 and minus_today > 0:
                message += f"\n📊 *[Total muted tests: {total}]({dashboard_url}) 🔴+{today} muted /🟢-{minus_today} unmuted*"
            elif today > 0:
                message += f"\n📊 *[Total muted tests: {total}]({dashboard_url}) 🔴+{today} muted*"
            elif minus_today > 0:
                message += f"\n📊 *[Total muted tests: {total}]({dashboard_url}) 🟢-{minus_today} unmuted*"
            else:
                message += f"\n📊 *[Total muted tests: {total}]({dashboard_url})*"
        else:
            message += f"\n📊 *[Total muted tests: {total}]({dashboard_url})*"
    
    # Add responsible users on new line with "fyi:" prefix (moved after statistics)
    if team_responsible and team_name in team_responsible:
        responsible = team_responsible[team_name]
        # Handle both single responsible and list of responsibles
        if isinstance(responsible, list):
            responsible_str = " ".join(f"@{r}" if not r.startswith('@') else r for r in responsible)
        else:
            responsible_str = f"@{responsible}" if not responsible.startswith('@') else responsible
        message += f"\n\nfyi: {responsible_str}"
    
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


def send_team_messages(teams, bot_token, delay=2, max_retries=5, retry_delay=10, team_channels=None, dry_run=False, muted_stats=None, include_plots=False, ydb_config=None, debug_plots_dir=None, all_team_data=None, show_diff=False):
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
        muted_stats (dict): Dictionary with team names as keys and {'total': count, 'today': count} as values
        include_plots (bool): If True, include trend plots in messages
        ydb_config (dict): YDB configuration for trend data
        debug_plots_dir (str): Directory to save debug plot files (if None, debug mode is disabled)
        all_team_data (dict): Pre-fetched team data to avoid repeated queries
        show_diff (bool): Whether to show +/- statistics in messages
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
        message = format_team_message(team_name, issues, team_responsible, muted_stats, show_diff)
        
        if not message.strip():
            continue
        
        # Message will be automatically escaped by send_telegram_message for MarkdownV2
        
        # Print final message before sending
        print(f"🔍 Final message for {team_name}:")
        print("-" * 80)
        print(message)
        print("-" * 80)
        
        if dry_run:
            print(f"\n--- Team: {team_name} ---")
            print(f"📨 Channel: {team_chat_id}" + (f" (thread {team_thread_id})" if team_thread_id else ""))
            print(message)
            sent_count += 1
        else:
            print(f"📨 Sending message for team: {team_name} ({len(issues)} issues)")
            
            # Get trend plot if requested
            plot_data = None
            print(f"🔍 Plot settings: include_plots={include_plots}, ydb_config={ydb_config is not None}, MATPLOTLIB_AVAILABLE={MATPLOTLIB_AVAILABLE}")
            
            if include_plots and MATPLOTLIB_AVAILABLE:
                # Use pre-fetched data if available, otherwise fetch on demand
                if all_team_data and team_name in all_team_data:
                    trend_data = all_team_data[team_name]['trend']
                    print(f"📊 Using cached trend data for {team_name}: {len(trend_data)} days")
                elif ydb_config:
                    print(f"📊 Getting trend data for team: {team_name}")
                    trend_data = get_monthly_trend_data(
                        team_name=team_name,
                        use_yesterday=ydb_config.get('use_yesterday', False)
                    )
                else:
                    trend_data = None
                
                if trend_data:
                    plot_data = create_trend_plot(team_name, trend_data, debug_plots_dir, period=None)
                    if plot_data:
                        print(f"📈 Created trend plot for team: {team_name} (data length: {len(plot_data)})")
                    else:
                        print(f"⚠️ Could not create trend plot for team: {team_name}")
                else:
                    print(f"⚠️ No trend data available for team: {team_name}")
            elif include_plots and not MATPLOTLIB_AVAILABLE:
                print(f"⚠️ Matplotlib not available, skipping plot for team: {team_name}")
            elif include_plots and not ydb_config:
                print(f"⚠️ YDB config not available, skipping plot for team: {team_name}")
            else:
                print(f"ℹ️ Plots disabled for team: {team_name}")
            
            # Send message with or without plot
            if plot_data:
                # Save plot to temporary file and send as photo
                with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                    tmp_file.write(base64.b64decode(plot_data))
                    tmp_path = tmp_file.name
                
                print(f"📁 Saved plot to temporary file: {tmp_path}")
                print(f"📁 File size: {os.path.getsize(tmp_path)} bytes")
                
                # Also save to debug directory for final file if requested
                if debug_plots_dir:
                    os.makedirs(debug_plots_dir, exist_ok=True)
                    final_debug_path = os.path.join(debug_plots_dir, f"final_{team_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                    shutil.copy2(tmp_path, final_debug_path)
                    print(f"🔍 Final debug plot saved to: {final_debug_path}")
                
                try:
                    print(f"📤 Sending message with plot for team: {team_name}")
                    print(f"📤 Chat ID: {team_chat_id}, Thread ID: {team_thread_id}")
                    print(f"📤 Photo path: {tmp_path}")
                    print(f"📤 Message length: {len(message)} characters")
                    
                    if send_telegram_message(bot_token, team_chat_id, message, "MarkdownV2", team_thread_id, True, max_retries, retry_delay, photo_path=tmp_path):
                        sent_count += 1
                        print(f"✅ Message with plot sent for team: {team_name}")
                    else:
                        print(f"❌ Failed to send message with plot for team: {team_name} after {max_retries} retries")
                finally:
                    # Clean up temporary file
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                        print(f"🗑️ Cleaned up temporary file: {tmp_path}")
            else:
                # Send regular message
                print(f"📤 Sending regular message for team: {team_name}")
                print(f"📤 Chat ID: {team_chat_id}, Thread ID: {team_thread_id}")
                print(f"📤 Message length: {len(message)} characters")
                
                if send_telegram_message(bot_token, team_chat_id, message, "MarkdownV2", team_thread_id, True, max_retries, retry_delay):
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


def send_period_updates(period, bot_token, team_channels, ydb_config, delay=2, max_retries=5, retry_delay=10, dry_run=False, debug_plots_dir=None):
    """
    Send periodic trend updates for all teams.
    
    Args:
        period (str): Period type ('week' or 'month')
        bot_token (str): Telegram bot token
        team_channels (dict): Dictionary mapping team names to their channel configurations
        ydb_config (dict): YDB configuration
        delay (int): Delay between messages in seconds
        max_retries (int): Maximum number of retry attempts
        retry_delay (int): Delay between retry attempts in seconds
        dry_run (bool): If True, only print messages without sending to Telegram
        debug_plots_dir (str): Directory to save debug plot files
        
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"📊 Starting {period}ly trend updates...")
    
    # Get all team data for trends
    all_team_data = get_all_team_data(
        use_yesterday=ydb_config.get('use_yesterday', False)
    )
    
    if not all_team_data:
        print("❌ Could not fetch team data for trend updates")
        return False
    
    # Get teams from data (all teams that have data)
    teams_from_data = list(all_team_data.keys())
    if not teams_from_data:
        print("❌ No teams found in data")
        return False
    
    # Filter out blacklisted teams
    teams_to_process = [team for team in teams_from_data if team not in PERIOD_UPDATE_BLACKLIST]
    blacklisted_count = len(teams_from_data) - len(teams_to_process)
    
    if blacklisted_count > 0:
        print(f"⏭️ Skipping {blacklisted_count} blacklisted teams: {', '.join(team for team in teams_from_data if team in PERIOD_UPDATE_BLACKLIST)}")
    
    print(f"📤 Sending {period}ly updates for {len(teams_to_process)} teams from data...")
    
    success_count = 0
    total_teams = len(teams_to_process)
    
    for team_name in teams_to_process:
        
        # Get team channel configuration
        team_responsible, team_chat_id, team_thread_id = get_team_config(team_name, team_channels)
        
        # If team not found in config, use default channel
        if not team_chat_id and team_channels:
            default_channel_name = team_channels.get('default_channel')
            if default_channel_name and 'channels' in team_channels:
                if default_channel_name in team_channels['channels']:
                    team_chat_id, team_thread_id = parse_chat_and_thread_id(team_channels['channels'][default_channel_name])
                    print(f"📨 Using default channel '{default_channel_name}' for team {team_name}: {team_chat_id}")
        
        # Determine channel name for logging
        if team_channels and 'teams' in team_channels and team_name in team_channels['teams']:
            team_config = team_channels['teams'][team_name]
            team_channel_name = team_config.get('channel', team_channels.get('default_channel', 'default'))
        else:
            team_channel_name = team_channels.get('default_channel', 'default') if team_channels else 'default'
        
        if not team_chat_id:
            print(f"⚠️ No channel configuration for team: {team_name} (skipping)")
            continue
        
        # Create trend message
        period_title = "Weekly Muted Tests Report" if period == "week" else "Monthly Muted Tests Report"
        team_tag = team_name.replace('-', '')
        team_url = f"{GITHUB_ORG_TEAMS_URL}/{team_name}"
        message = f"📈 *{period_title}* for team [{team_name}]({team_url}) #{team_tag}\n\n"
        
        # Add trend statistics if available
        if team_name in all_team_data:
            team_stats = all_team_data[team_name]['stats']
            trend_data = all_team_data[team_name]['trend']
            total = team_stats['total']
            dashboard_url = DATALENS_DASHBOARD_URL.format(team_name=team_name)
            
            # Calculate period change using the same logic as the plot
            interval_start_str, current_date_str, previous_count, current_count = get_interval_dates(trend_data, period)
            period_days = 7 if period == "week" else 30
            
            if current_date_str:
                previous_date_str = interval_start_str
            else:
                # No data available, skip change calculation
                current_date_str = None
                previous_date_str = None
                current_count = 0
                previous_count = 0
            
            # Check if we have data for both dates to calculate meaningful change
            has_current_data = current_date_str in trend_data
            has_previous_data = previous_date_str in trend_data
            
            # Debug information
            print(f"🔍 Debug for team {team_name}:")
            print(f"   Current date: {current_date_str}, count: {current_count}, has_data: {has_current_data}")
            print(f"   Previous date: {previous_date_str}, count: {previous_count}, has_data: {has_previous_data}")
            print(f"   Available dates in trend_data: {sorted(trend_data.keys())[-5:]}")  # Last 5 dates
            
            message += f"📊 *[Total muted tests: {total}]({dashboard_url})*\n\n"
            
            # Add change information only if we have data for both dates
            if has_current_data and has_previous_data:
                change = current_count - previous_count
                print(f"   Change: {change} (current - previous)")
                if change > 0:
                    message += f"🔴 +{change} muted tests in last {period_days} days\n\n"
                elif change < 0:
                    message += f"🟢 {change} muted tests in last {period_days} days\n\n"
                else:
                    message += f"⚪ No change in last {period_days} days\n\n"
            else:
                print(f"   Cannot calculate change: missing data for current={has_current_data}, previous={has_previous_data}")
                
                # Try to find the earliest available data for comparison
                if trend_data and has_current_data:
                    available_dates = sorted(trend_data.keys())
                    if len(available_dates) > 1:
                        earliest_date = available_dates[0]
                        earliest_count = trend_data[earliest_date]
                        days_span = (datetime.strptime(current_date_str, '%Y-%m-%d') - datetime.strptime(earliest_date, '%Y-%m-%d')).days
                        change = current_count - earliest_count
                        print(f"   Using earliest available data: {earliest_date} ({earliest_count}) vs {current_date_str} ({current_count}) = {change} over {days_span} days")
                        
                        if change > 0:
                            message += f"🔴 +{change} muted tests since {earliest_date} ({days_span} days ago)\n\n"
                        elif change < 0:
                            message += f"🟢 {change} muted tests since {earliest_date} ({days_span} days ago)\n\n"
                        else:
                            message += f"⚪ No change since {earliest_date} ({days_span} days ago)\n\n"
                    else:
                        message += f"ℹ️ No historical data available for comparison\n\n"
                else:
                    if not has_previous_data:
                        message += f"ℹ️ No data available for comparison ({period_days} days ago)\n\n"
                    else:
                        message += f"ℹ️ Insufficient data for trend analysis\n\n"
        
        # Add responsible users if available
        if team_responsible and team_name in team_responsible:
            responsible = team_responsible[team_name]
            # Handle both single responsible and list of responsibles
            if isinstance(responsible, list):
                responsible_str = " ".join(f"@{r}" if not r.startswith('@') else r for r in responsible)
            else:
                responsible_str = f"@{responsible}" if not responsible.startswith('@') else responsible
            message += f"fyi: {responsible_str}\n\n"
        
        # Message will be automatically escaped by send_telegram_message for MarkdownV2
        
        # Print final message before sending
        print(f"🔍 Final {period}ly message for {team_name}:")
        print("-" * 80)
        print(message)
        print("-" * 80)
        
        if dry_run:
            print(f"📋 [DRY RUN] Team: {team_name}")
            print(f"📋 Channel: {team_channel_name} ({team_chat_id})")
            print(f"📋 Thread: {team_thread_id}")
            print("📋 Message:")
            print("-" * 80)
            print(message)
            print("-" * 80)
            print()
            success_count += 1
        else:
            print(f"📨 Sending {period}ly update for team: {team_name}")
            
            # Get trend data for this team
            trend_data = all_team_data.get(team_name, {}).get('trend', {})
            
            if trend_data and MATPLOTLIB_AVAILABLE:
                # Create trend plot
                plot_data = create_trend_plot(team_name, trend_data, debug_plots_dir, period=period)
                
                if plot_data:
                    # Send message with plot
                    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                        tmp_file.write(base64.b64decode(plot_data))
                        tmp_path = tmp_file.name
                    
                    try:
                        print(f"📤 Sending trend plot for team: {team_name}")
                        if send_telegram_message(bot_token, team_chat_id, message, "MarkdownV2", team_thread_id, True, max_retries, retry_delay, photo_path=tmp_path):
                            success_count += 1
                            print(f"✅ Trend update sent for team: {team_name}")
                        else:
                            print(f"❌ Failed to send trend update for team: {team_name}")
                    finally:
                        # Clean up temporary file
                        if os.path.exists(tmp_path):
                            os.unlink(tmp_path)
                else:
                    # Fallback to text message
                    print(f"⚠️ Could not create plot, sending text message for team: {team_name}")
                    if send_telegram_message(bot_token, team_chat_id, message, "MarkdownV2", team_thread_id, True, max_retries, retry_delay):
                        success_count += 1
                        print(f"✅ Text update sent for team: {team_name}")
                    else:
                        print(f"❌ Failed to send text update for team: {team_name}")
            else:
                # Send text message only
                print(f"📤 Sending text update for team: {team_name}")
                if send_telegram_message(bot_token, team_chat_id, message, "MarkdownV2", team_thread_id, True, max_retries, retry_delay):
                    success_count += 1
                    print(f"✅ Text update sent for team: {team_name}")
                else:
                    print(f"❌ Failed to send text update for team: {team_name}")
        
        # Add delay between messages
        if success_count < total_teams:
            time.sleep(delay)
    
    if dry_run:
        print(f"🎉 Dry run completed: {success_count}/{total_teams} {period}ly updates formatted!")
        if blacklisted_count > 0:
            print(f"📊 Summary: {len(teams_from_data)} total teams, {blacklisted_count} blacklisted, {total_teams} processed")
    else:
        print(f"🎉 Sent {success_count}/{total_teams} {period}ly updates successfully!")
        if blacklisted_count > 0:
            print(f"📊 Summary: {len(teams_from_data)} total teams, {blacklisted_count} blacklisted, {total_teams} processed, {success_count} sent")
    
    return success_count == total_teams


def main():
    parser = argparse.ArgumentParser(description="Parse team issues and send separate messages for each team")
    
    # Required arguments
    parser.add_argument('--file', help='Path to file with formatted results (required for --on-mute-change-update mode)')
    parser.add_argument('--bot-token', help='Telegram bot token (or use TELEGRAM_BOT_TOKEN env var)')
    parser.add_argument('--team-channels', required=True, help='JSON string mapping teams to their channel configurations (or use TEAM_CHANNELS env var)')
    
    # Optional arguments
    parser.add_argument('--delay', type=int, default=2, help='Delay between messages in seconds (default: 2)')
    parser.add_argument('--dry-run', action='store_true', help='Parse and show teams without sending messages')
    parser.add_argument('--test-connection', action='store_true', help='Test Telegram connection only')
    parser.add_argument('--message-thread-id', type=int, help='Thread ID for group messages (optional)')
    parser.add_argument('--max-retries', type=int, default=5, help='Maximum number of retry attempts for failed messages (default: 5)')
    parser.add_argument('--retry-delay', type=int, default=10, help='Delay between retry attempts in seconds (default: 10)')
    
    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--on-mute-change-update', action='store_true', help='Default mode: send updates about new muted tests')
    mode_group.add_argument('--period-update', choices=['week', 'month'], help='Send periodic trend updates (week or month)')
    
    # YDB arguments for muted tests statistics
    parser.add_argument('--no-stats', action='store_true', help='Skip fetching muted tests statistics from YDB')
    parser.add_argument('--use-yesterday', action='store_true', help='Use yesterday\'s data for development convenience')
    parser.add_argument('--include-plots', action='store_true', help='Include trend plots in messages (requires matplotlib)')
    parser.add_argument('--debug-plots-dir', help='Directory to save debug plot files (enables debug mode)')
    
    args = parser.parse_args()
    
    # Validate mode-specific requirements
    if args.on_mute_change_update and not args.file:
        print("❌ --file is required for --on-mute-change-update mode")
        sys.exit(1)
    
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
    
    # Handle period update mode
    if args.period_update:
        # Prepare YDB config for period updates
        ydb_config = {
            'use_yesterday': args.use_yesterday
        }
        
        # Check if we need Telegram connection (not for dry run)
        if not args.dry_run:
            if not bot_token:
                print("❌ Bot token not provided. Use --bot-token or set TELEGRAM_BOT_TOKEN environment variable")
                sys.exit(1)
        
        # Send period updates
        success = send_period_updates(
            period=args.period_update,
            bot_token=bot_token,
            team_channels=team_channels,
            ydb_config=ydb_config,
            delay=args.delay,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
            dry_run=args.dry_run,
            debug_plots_dir=args.debug_plots_dir
        )
        
        if success:
            print("✅ Period updates completed successfully")
            sys.exit(0)
        else:
            print("❌ Period updates failed")
            sys.exit(1)
    
    # Handle on-mute-change-update mode (default mode)
    # Get muted tests statistics if not disabled
    muted_stats = None
    if not args.no_stats:
        print("📊 Fetching muted tests statistics from YDB...")
        muted_stats = get_muted_tests_stats(
            use_yesterday=args.use_yesterday
        )
        if muted_stats:
            print(f"✅ Statistics loaded for {len(muted_stats)} teams")
        else:
            print("⚠️ Could not load statistics, continuing without stats")
    else:
        print("⏭️ Skipping statistics fetch (--no-stats flag)")
    
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
    
    # Prepare YDB config and get all team data if needed
    ydb_config = None
    all_team_data = None
    
    if args.include_plots and not args.no_stats:
        ydb_config = {
            'use_yesterday': args.use_yesterday
        }
        
        # Get all team data in one optimized query
        print("📊 Fetching all team data in one optimized query...")
        all_team_data = get_all_team_data(
            use_yesterday=args.use_yesterday
        )
        
        if all_team_data:
            print(f"✅ Successfully fetched data for {len(all_team_data)} teams")
        else:
            print("⚠️ Could not fetch team data, will use individual queries if needed")
    
    # Send messages (or show in dry run)
    send_team_messages(
        teams, 
        bot_token, 
        args.delay, 
        args.max_retries, 
        args.retry_delay, 
        team_channels, 
        args.dry_run,
        muted_stats,
        args.include_plots,
        ydb_config,
        args.debug_plots_dir,
        all_team_data,
        MUTE_UPDATE_SHOW_DIFF
    )


if __name__ == "__main__":
    main()
