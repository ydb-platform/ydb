#!/usr/bin/env python3
"""
Script for monitoring workflow runs queued in GitHub Actions.
Sends notifications to Telegram when stuck jobs are detected.
"""

import requests
import os
import sys
import argparse
import subprocess
from collections import defaultdict
from typing import Dict, List, Any
from datetime import datetime, timezone
import time

# -----------------------------------------------------------------------------
# Config & constants
# -----------------------------------------------------------------------------
GITHUB_API_RUNS_URL = "https://api.github.com/repos/ydb-platform/ydb/actions/runs"
GITHUB_API_TIMEOUT_SEC = 30
GITHUB_API_PER_PAGE = 1000
TELEGRAM_REQUEST_TIMEOUT_SEC = 10
TELEGRAM_SEND_TIMEOUT_SEC = 60
MAX_STUCK_JOBS_IN_MESSAGE = 15
DEFAULT_GITHUB_REPO = "ydb-platform/ydb"

DASHBOARD_LINK = "📊 [Dashboard details](https://datalens.yandex/wkptiaeyxz7qj?tab=ka)"

# (pattern, display_name, threshold_spec). threshold_spec: float or [(start_utc, end_utc, hours), ...] (overnight: start > end).
WORKFLOW_THRESHOLDS = [
    ("PR-check", "PR-check", [(8, 20, 1), (20, 8, 3.0)]),   # 8–20 UTC: 1 h, 20–8 UTC: 3 h
    ("Postcommit", "Postcommit", 6),
]

EMPTY_QUEUE_MESSAGE = (
    "✅ *GITHUB ACTIONS MONITORING*\n\nQueue is empty - all jobs are working normally! 🎉"
)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def get_alert_call() -> str:
    """
    Value for alert messages from GH_ALERTS_TG_LOGINS (repo variable).
    Can be: Telegram usernames or a bot command (e.g. /duty ydb-ci). Message must start with this for the bot to react.
    """
    value = os.getenv('GH_ALERTS_TG_LOGINS')
    return value.strip() if value else ""


def get_blacklisted_run_ids(blacklist_param: str = None) -> set:
    """
    Gets the set of blacklisted run IDs from command line parameter.
    
    Args:
        blacklist_param: Space-separated list of run IDs from command line (optional)
    
    Returns:
        Set of run IDs to exclude from monitoring
    """
    if not blacklist_param:
        return set()
    
    # Split by any whitespace and convert to set of strings
    run_ids = blacklist_param.split()
    return set(run_ids)

def threshold_for_time(utc_dt: datetime, spec) -> float:
    """Resolve threshold in hours: spec is float or list of (start_hour, end_hour, hours)."""
    if isinstance(spec, (int, float)):
        return float(spec)
    hour = utc_dt.hour
    for start, end, h in spec:
        if start <= end:
            if start <= hour < end:
                return h
        else:
            if hour >= start or hour < end:
                return h
    raise ValueError(f"No interval matches hour={hour} in spec={spec}")

def fetch_workflow_runs(status: str = "queued", per_page: int = GITHUB_API_PER_PAGE, page: int = 1) -> tuple[Dict[str, Any], str]:
    """
    Fetches workflow runs data from GitHub API.
    
    Args:
        status: Status of workflow runs (queued, in_progress, completed, etc.)
        per_page: Number of records per page
        page: Page number
    
    Returns:
        Tuple (data, error). If successful - (data, ""), if error - ({}, error_message)
    """
    url = GITHUB_API_RUNS_URL
    params = {
        "per_page": per_page,
        "page": page,
        "status": status
    }
    
    try:
        response = requests.get(url, params=params, timeout=GITHUB_API_TIMEOUT_SEC)
        
        if response.status_code == 200:
            return response.json(), ""
        else:
            # Return error as is from API
            try:
                error_data = response.json()
                error_message = error_data.get("message", f"HTTP {response.status_code}")
            except Exception:
                error_message = f"HTTP {response.status_code}: {response.text}"
            
            return {}, error_message
            
    except requests.exceptions.Timeout:
        return {}, f"Timeout: Request exceeded timeout ({GITHUB_API_TIMEOUT_SEC} sec)"
    except requests.exceptions.ConnectionError:
        return {}, "Connection error: Failed to connect to API"
    except requests.exceptions.RequestException as e:
        return {}, f"Request error: {e}"
    except Exception as e:
        return {}, f"Unexpected error: {e}"

def get_effective_start_time(run: Dict[str, Any]) -> datetime:
    """
    Gets the effective start time for run (accounts for retry).
    
    Args:
        run: Workflow run object
    
    Returns:
        datetime: Effective start time
    """
    run_attempt = run.get('run_attempt', 1)
    updated_at_str = run.get('updated_at')
    created_at_str = run.get('created_at')
    
    # For retry jobs use updated_at (time of last status change)
    # since API with status=queued always returns only queued jobs
    if run_attempt > 1 and updated_at_str:
        try:
            return datetime.fromisoformat(updated_at_str.replace('Z', '+00:00'))
        except ValueError:
            pass
    
    # For first attempt use created_at
    if created_at_str:
        try:
            return datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        except ValueError:
            pass
    
    # Fallback - current time
    return datetime.now(timezone.utc)


def is_retry_job(run: Dict[str, Any]) -> bool:
    """
    True if this run is a retry (user clicked "Re-run" in GitHub Actions).
    Retries re-enter the queue and can trigger false alerts; use this when we
    want to exclude or treat retries differently (e.g. skip from stuck count).
    """
    return run.get('run_attempt', 1) > 1


def analyze_queued_workflows(workflow_runs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Analyzes workflow runs in queue and finds the oldest job for each type.
    
    Args:
        workflow_runs: List of workflow runs from API
    
    Returns:
        Dictionary with information about each workflow type
    """
    workflow_info = defaultdict(lambda: {
        'count': 0,
        'oldest_created_at': None,
        'oldest_run_id': None,
        'runs': []
    })
    
    for run in workflow_runs:
        workflow_name = run.get('name', 'Unknown')
        run_id = run.get('id')
        
        workflow_info[workflow_name]['count'] += 1
        workflow_info[workflow_name]['runs'].append(run)
        
        # Get effective start time (accounts for retry)
        effective_start_time = get_effective_start_time(run)
        
        # Check if this run is the oldest
        if (workflow_info[workflow_name]['oldest_created_at'] is None or 
            effective_start_time < workflow_info[workflow_name]['oldest_created_at']):
            workflow_info[workflow_name]['oldest_created_at'] = effective_start_time
            workflow_info[workflow_name]['oldest_run_id'] = run_id
    
    return dict(workflow_info)

def format_time_ago(created_at: datetime) -> str:
    """
    Formats time into a human-readable format "X minutes/hours/days ago".
    
    Args:
        created_at: Creation time
    
    Returns:
        String with time description
    """
    if created_at is None:
        return "Unknown"
    
    current_time = datetime.now(timezone.utc)
    time_diff = current_time - created_at
    
    total_seconds = time_diff.total_seconds()
    
    if total_seconds < 60:
        return f"{int(total_seconds)}s"
    elif total_seconds < 3600:
        minutes = total_seconds / 60
        return f"{minutes:.1f}m"
    elif total_seconds < 86400:
        hours = total_seconds / 3600
        return f"{hours:.1f}h"
    else:
        days = total_seconds / 86400
        return f"{days:.1f}d"

def filter_blacklisted_jobs(workflow_runs: List[Dict[str, Any]], blacklisted_ids: set) -> List[Dict[str, Any]]:
    """
    Filters jobs that are in the blacklist.
    
    Args:
        workflow_runs: List of workflow runs in queue
        blacklisted_ids: Set of run IDs to exclude from monitoring
    
    Returns:
        Filtered list of workflow runs
    """
    if not blacklisted_ids:
        return workflow_runs
    
    filtered_runs = []
    
    for run in workflow_runs:
        run_id = str(run.get('id', ''))
        if run_id not in blacklisted_ids:
            filtered_runs.append(run)
    
    excluded_count = len(workflow_runs) - len(filtered_runs)
    if excluded_count > 0:
        print(f"⚠️ Excluded {excluded_count} jobs from blacklist: {', '.join(sorted(blacklisted_ids))}")
    
    return filtered_runs

def is_job_stuck_by_criteria(run, waiting_hours):
    """
    Checks if job is stuck by our criteria.
    
    Args:
        run: Workflow run object
        waiting_hours: Waiting time in hours
    
    Returns:
        bool: True if job is considered stuck
    """
    workflow_name = run.get('name', '')
    current_time = datetime.now(timezone.utc)
    for pattern, display_name, spec in WORKFLOW_THRESHOLDS:
        if pattern in workflow_name and waiting_hours > threshold_for_time(current_time, spec):
            return True
    return False

def generate_stuck_jobs_summary(stuck_jobs: List[Dict[str, Any]]) -> List[str]:
    """
    Generates brief description of stuck jobs as a list of strings, each starting with ⚠️
    
    Args:
        stuck_jobs: List of stuck jobs
    
    Returns:
        List of strings with description of stuck jobs
    """
    if not stuck_jobs:
        return []
    
    stuck_counts = count_stuck_jobs_by_type(stuck_jobs)
    current_time = datetime.now(timezone.utc)
    descriptions = []
    for pattern, display_name, spec in WORKFLOW_THRESHOLDS:
        count = stuck_counts.get(display_name, 0)
        if count > 0:
            th = threshold_for_time(current_time, spec)
            descriptions.append(f"⚠️ {display_name} job(s) have been in the queue for more than {th} hour(s)! Total: {count} job(s).")
    
    # Add Other if any
    other_count = stuck_counts.get('Other', 0)
    if other_count > 0:
        descriptions.append(f"⚠️ Other job(s) are stuck! Total: {other_count} job(s).")
    
    return descriptions

def count_stuck_jobs_by_type(stuck_jobs: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Counts the number of stuck jobs by types.
    
    Args:
        stuck_jobs: List of stuck jobs
    
    Returns:
        Dictionary with count of stuck jobs by types
    """
    counts = {}
    for pattern, display_name, spec in WORKFLOW_THRESHOLDS:
        counts[display_name] = 0
    counts['Other'] = 0
    
    for stuck_job in stuck_jobs:
        workflow_name = stuck_job['run'].get('name', '')
        found_type = False
        for pattern, display_name, spec in WORKFLOW_THRESHOLDS:
            if pattern in workflow_name:
                counts[display_name] += 1
                found_type = True
                break
        
        # If no type found, add to Other
        if not found_type:
            counts['Other'] += 1
    
    return counts

def check_for_stuck_jobs(workflow_runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Finds "stuck" jobs by our criteria from WORKFLOW_THRESHOLDS.

    Returns:
        List of stuck jobs
    """
    stuck_jobs = []
    current_time = datetime.now(timezone.utc)
    
    for run in workflow_runs:
        # Get effective start time (accounts for retry)
        effective_start_time = get_effective_start_time(run)
        time_diff = current_time - effective_start_time
        waiting_hours = time_diff.total_seconds() / 3600
        
        # Use our criteria to determine stuck jobs
        if is_job_stuck_by_criteria(run, waiting_hours):
            stuck_jobs.append({
                'run': run,
                'waiting_hours': waiting_hours
            })
    
    return stuck_jobs

def format_telegram_messages(workflow_info: Dict[str, Dict[str, Any]], stuck_jobs: List[Dict[str, Any]], total_queued: int, excluded_count: int = 0) -> List[str]:
    """
    Formats messages for sending to Telegram (splits into 2 parts).
    
    Args:
        workflow_info: Information about workflows
        stuck_jobs: List of stuck jobs
        total_queued: Total number of jobs in queue
        excluded_count: Number of excluded jobs (from blacklist)
    
    Returns:
        List of 2 messages for Telegram
    """
    messages = []
    
    # First message - general statistics
    message1_parts = []
    
    # Header
    if stuck_jobs:
        message1_parts.append("🚨 *GITHUB ACTIONS MONITORING*")
        
        # Replace "Stuck jobs detected!" with descriptive messages about stuck jobs (one line per type)
        stuck_summary_lines = generate_stuck_jobs_summary(stuck_jobs)
        if stuck_summary_lines:
            message1_parts.extend(stuck_summary_lines)
        else:
            message1_parts.append("⚠️ *Stuck jobs detected!*")
    else:
        message1_parts.append("✅ *GITHUB ACTIONS MONITORING*")
        message1_parts.append("All jobs in the queue are working normally")
    
    message1_parts.append("")
    
    # General statistics
    message1_parts.append(f"📊 *Statistics:*")
    message1_parts.append(f"• Total in queue: {total_queued} jobs")
    
    # Statistics of stuck jobs by types
    stuck_counts = count_stuck_jobs_by_type(stuck_jobs)
    total_stuck = sum(stuck_counts.values())
    message1_parts.append(f"• Stuck: {total_stuck} jobs")
    
    if excluded_count > 0:
        message1_parts.append(f"• Excluded (blacklist): {excluded_count} jobs")
    message1_parts.append("")
    
    # Summary by workflow types
    if workflow_info:
        message1_parts.append("📋 *Workflows in queue:*")
        
        # First show types from WORKFLOW_THRESHOLDS
        threshold_workflows = []
        other_workflows = []
        
        for workflow_name, info in workflow_info.items():
            is_threshold_type = False
            for pattern, display_name, spec in WORKFLOW_THRESHOLDS:
                if pattern in workflow_name:
                    threshold_workflows.append((workflow_name, info))
                    is_threshold_type = True
                    break
            if not is_threshold_type:
                other_workflows.append((workflow_name, info))
        
        # Sort each group by number of jobs
        threshold_workflows.sort(key=lambda x: x[1]['count'], reverse=True)
        other_workflows.sort(key=lambda x: x[1]['count'], reverse=True)
        
        # Combine lists: first threshold types, then others
        all_workflows = threshold_workflows + other_workflows
        
        for workflow_name, info in all_workflows:  # Show all types
            count = info['count']
            oldest_time = info['oldest_created_at']
            time_ago = format_time_ago(oldest_time)
            message1_parts.append(f"• `{workflow_name}`: {count} jobs (oldest: {time_ago})")
    
    message1_parts.append("")
    message1_parts.append(f"🕐 *Check time:* {datetime.now().strftime('%H:%M:%S UTC')}")
    
    messages.append("\n".join(message1_parts))
    
    # Second message - details about stuck jobs (only if any). Must start with the call (e.g. /duty ydb-ci) so the bot reacts.
    if stuck_jobs:
        message2_parts = []
        message2_parts.append("🚨 *Stuck jobs:*")
        message2_parts.append("")
        
        # Sort by waiting time (oldest first)
        stuck_jobs_sorted = sorted(stuck_jobs, key=lambda x: x['waiting_hours'], reverse=True)
        
        for i, stuck_job in enumerate(stuck_jobs_sorted[:MAX_STUCK_JOBS_IN_MESSAGE], 1):
            run = stuck_job['run']
            waiting_hours = stuck_job['waiting_hours']
            workflow_name = run.get('name', 'Unknown')
            run_id = run.get('id')
            run_attempt = run.get('run_attempt', 1)
            
            if waiting_hours > 24:
                waiting_str = f"{waiting_hours/24:.1f}d"
            elif waiting_hours > 1:
                waiting_str = f"{waiting_hours:.1f}h"
            else:
                waiting_str = f"{waiting_hours*60:.0f}m"
            
            # Add retry information
            retry_info = f" (retry #{run_attempt})" if run_attempt > 1 else ""
            
            github_url = f"https://github.com/{DEFAULT_GITHUB_REPO}/actions/runs/{run_id}" if run_id else "N/A"
            message2_parts.append(f"{i}. `{workflow_name}`{retry_info} - {waiting_str}")
            if run_id:
                message2_parts.append(f"   [Run {run_id}]({github_url})")
            message2_parts.append("")
        
        if len(stuck_jobs) > MAX_STUCK_JOBS_IN_MESSAGE:
            message2_parts.append(f"• ... and {len(stuck_jobs) - MAX_STUCK_JOBS_IN_MESSAGE} more jobs")
        
        message2_parts.append("")
        message2_parts.append(DASHBOARD_LINK)
        
        call = get_alert_call()
        body = "\n".join(message2_parts)
        messages.append((call + "\n\n" + body) if call else body)
    
    return messages

def test_telegram_connection(bot_token: str, chat_id: str, thread_id: int = None) -> bool:
    """
    Tests connection to Telegram without sending messages.
    
    Args:
        bot_token: Telegram bot token
        chat_id: Chat ID
        thread_id: Thread ID for group messages
    
    Returns:
        True if connection is successful, False otherwise
    """
    print(f"🔍 Testing connection to Telegram for chat {chat_id}...")
    if thread_id:
        print(f"🔍 Testing thread {thread_id}...")
    
    # Debug information
    print(f"🔍 Bot token: {bot_token[:10]}...{bot_token[-10:] if len(bot_token) > 20 else 'SHORT'}")
    print(f"🔍 Chat ID: {chat_id}")
    
    # Use getChat method instead of sending message
    url = f"https://api.telegram.org/bot{bot_token}/getChat"
    data = {'chat_id': chat_id}
    
    if thread_id:
        data['message_thread_id'] = thread_id
    
    try:
        response = requests.post(url, data=data, timeout=TELEGRAM_REQUEST_TIMEOUT_SEC)
        response.raise_for_status()
        
        result = response.json()
        if result.get('ok'):
            print("✅ Connection to Telegram successful!")
            return True
        else:
            print(f"❌ Connection error to Telegram: {result.get('description', 'Unknown error')}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Connection error to Telegram: {e}")
        return False

def get_current_workflow_url() -> str:
    """
    Gets current workflow run URL from GitHub Actions environment variables.
    
    Returns:
        Current workflow run URL or empty string if variables are unavailable
    """
    github_repository = os.getenv('GITHUB_REPOSITORY', DEFAULT_GITHUB_REPO)
    github_run_id = os.getenv('GITHUB_RUN_ID')
    
    if github_run_id:
        return f"https://github.com/{github_repository}/actions/runs/{github_run_id}"
    return ""

def send_api_error_notification(bot_token: str, chat_id: str, error_message: str, thread_id: int = None) -> bool:
    """
    Sends API error notification to Telegram.
    
    Args:
        bot_token: Telegram bot token
        chat_id: Chat ID
        error_message: Error message
        thread_id: Thread ID for group messages
    
    Returns:
        True if successful, False otherwise
    """
    # Get link to current workflow run
    workflow_url = get_current_workflow_url()
    workflow_link = f"\n\n🔗 [Workflow Run]({workflow_url})" if workflow_url else ""

    call = get_alert_call()
    body = f"⚠️ *GITHUB ACTIONS MONITORING ERROR*\n\n{error_message}\n\n🕐 *Time:* {datetime.now().strftime('%H:%M:%S UTC')}{workflow_link}\n\n{DASHBOARD_LINK}"
    message = (call + "\n\n" + body) if call else body
    return send_telegram_message(bot_token, chat_id, message, thread_id, "MarkdownV2")

def send_telegram_message(bot_token: str, chat_id: str, message: str, thread_id: int = None, parse_mode: str = "MarkdownV2") -> bool:
    """
    Sends message to Telegram using external script.
    
    Args:
        bot_token: Telegram bot token
        chat_id: Chat ID
        message: Message text
        thread_id: Thread ID for group messages
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get path to send_telegram_message.py script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        send_script = os.path.join(script_dir, 'send_telegram_message.py')
        
        # Call external script with message
        print(f"Chat ID: {chat_id}")
        cmd = [
            'python3', send_script,
            '--bot-token', bot_token,
            '--chat-id', chat_id,
            '--message', message,
            '--parse-mode', parse_mode
        ]
        
        # Add thread_id if specified
        if thread_id:
            cmd.extend(['--message-thread-id', str(thread_id)])
        
        result = subprocess.run(cmd, text=True, timeout=TELEGRAM_SEND_TIMEOUT_SEC)
        
        if result.returncode == 0:
            print("✅ Message sent to Telegram")
            return True
        else:
            print(f"❌ Error sending to Telegram (code {result.returncode})")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout when sending to Telegram")
        return False
    except Exception as e:
        print(f"❌ Error when calling send script: {e}")
        return False

def _send_or_skip_empty_queue_message(bot_token, chat_id, thread_id, dry_run, send_when_all_good):
    """Send or skip the 'queue is empty' notification depending on flags."""
    if dry_run:
        print(f"\n📤 DRY-RUN: Message for Telegram:{chat_id}:{thread_id}")
        print("-" * 50)
        print(EMPTY_QUEUE_MESSAGE)
        print("-" * 50)
    elif send_when_all_good:
        print("📤 Sending empty queue message to Telegram")
        if send_telegram_message(bot_token, chat_id, EMPTY_QUEUE_MESSAGE, thread_id, "MarkdownV2"):
            print("✅ Empty queue message sent successfully")
        else:
            print("❌ Error sending empty queue message")
    else:
        print("📤 Queue is empty - sending nothing")


def main():
    """Main script function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Monitor workflow runs in GitHub Actions queue")
    parser.add_argument('--dry-run', action='store_true', 
                       help='Debug mode without sending to Telegram')
    parser.add_argument('--bot-token', 
                       help='Telegram bot token (or use TELEGRAM_BOT_TOKEN env var)')
    parser.add_argument('--chat-id', 
                       help='Telegram chat ID')
    parser.add_argument('--channel', 
                       help='Telegram channel ID (alternative to --chat-id)')
    parser.add_argument('--thread-id', type=int,
                       help='Telegram thread ID for group messages')
    parser.add_argument('--test-connection', action='store_true',
                       help='Only test connection to Telegram')
    parser.add_argument('--send-when-all-good', action='store_true',
                       help='Send message even when all jobs are working normally')
    parser.add_argument('--notify-on-api-errors', action='store_true',
                       help='Send notifications to Telegram on API errors')
    parser.add_argument('--blacklist', 
                       help='Space-separated list of run IDs to exclude from monitoring')
    
    args = parser.parse_args()
    
    print("🔍 Monitoring workflow runs in GitHub Actions queue")
    print("=" * 60)
    
    # Get parameters from arguments or environment variables
    bot_token = args.bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = args.channel or args.chat_id or os.getenv('TELEGRAM_CHAT_ID')
    thread_id = args.thread_id or os.getenv('TELEGRAM_THREAD_ID')
    dry_run = args.dry_run or os.getenv('DRY_RUN', 'false').lower() == 'true'
    send_when_all_good = args.send_when_all_good or os.getenv('SEND_WHEN_ALL_GOOD', 'false').lower() == 'true'
    notify_on_api_errors = args.notify_on_api_errors or os.getenv('NOTIFY_ON_API_ERRORS', 'false').lower() == 'true'
    
    # Fix chat_id format for channels (as in parse_and_send_team_issues.py)
    if chat_id and not chat_id.startswith('-') and len(chat_id) >= 10:
        # Add -100 prefix for supergroup
        chat_id = f"-100{chat_id}"
    
    # Check connection testing mode
    if args.test_connection:
        if not bot_token:
            print("❌ TELEGRAM_BOT_TOKEN not set")
            print("   Use --bot-token or set TELEGRAM_BOT_TOKEN environment variable")
            sys.exit(1)
        
        print("🔍 Testing connection to Telegram...")
        if test_telegram_connection(bot_token, chat_id, thread_id):
            print("✅ Connection successful!")
            sys.exit(0)
        else:
            print("❌ Connection failed!")
            sys.exit(1)
    
    if dry_run:
        print("🧪 DRY-RUN MODE: Tokens not required, messages not sent")
        print("=" * 60)
    elif not bot_token:
        print("❌ TELEGRAM_BOT_TOKEN not set")
        print("💡 For local debugging use --dry-run")
        sys.exit(1)
    
    # Get data for status "queued"
    print("📡 Loading data for status: queued")
    data, error = fetch_workflow_runs(status="queued")
    
    if error:
        print(f"❌ GitHub API error: {error}")
        
        # Send API error notification if enabled
        if notify_on_api_errors:
            if dry_run:
                # Get link to current workflow run for dry-run
                workflow_url = get_current_workflow_url()
                workflow_link = f"\n\n🔗 [Workflow Run]({workflow_url})" if workflow_url else ""
                
                print(f"\n📤 DRY-RUN: API error notification for Telegram {chat_id}:{thread_id}")
                print("-" * 50)
                call = get_alert_call()
                body = f"⚠️ *GITHUB ACTIONS MONITORING ERROR*\n\n{error}\n\n🕐 *Time:* {datetime.now().strftime('%H:%M:%S UTC')}{workflow_link}\n\n{DASHBOARD_LINK}"
                print((call + "\n\n" + body) if call else body)
                print("-" * 50)
            else:
                print("📤 Sending API error notification to Telegram...")
                if send_api_error_notification(bot_token, chat_id, error, thread_id):
                    print("✅ Error notification sent")
                else:
                    print("❌ Error sending error notification")
        
        sys.exit(1)
    
    queued_runs = data['workflow_runs']
    print(f"📊 Found {len(queued_runs)} workflow runs in queue")
    
    # Check that we got data
    if not queued_runs:
        print("✅ No workflow runs in queue")
        _send_or_skip_empty_queue_message(bot_token, chat_id, thread_id, dry_run, send_when_all_good)
        return
    
    # Get blacklisted run IDs from command line parameter
    blacklisted_ids = get_blacklisted_run_ids(args.blacklist)
    
    # Filter blacklisted jobs
    filtered_runs = filter_blacklisted_jobs(queued_runs, blacklisted_ids)
    excluded_count = len(queued_runs) - len(filtered_runs)
    print(f"📊 After filtering: {len(filtered_runs)} workflow runs (excluded {excluded_count} blacklisted jobs)")
    
    if not filtered_runs:
        print("✅ No current workflow runs in queue after filtering")
        _send_or_skip_empty_queue_message(bot_token, chat_id, thread_id, dry_run, send_when_all_good)
        return
    
    # Analyze data
    workflow_info = analyze_queued_workflows(filtered_runs)
    total_queued = sum(info['count'] for info in workflow_info.values())
    
    # Check for stuck jobs by our criteria
    stuck_jobs = check_for_stuck_jobs(filtered_runs)
    
    # Format messages for Telegram (even if not sending)
    telegram_messages = format_telegram_messages(workflow_info, stuck_jobs, total_queued, excluded_count)
    
    # If no stuck jobs, check if we need to send message
    if not stuck_jobs:
        if send_when_all_good:
            print(f"✅ No stuck jobs by our criteria - sending good status report")
        else:
            print(f"✅ No stuck jobs by our criteria - sending nothing")
        
        current_time = datetime.now(timezone.utc)
        criteria_parts = [f"{display_name} >{threshold_for_time(current_time, spec)}h" for _, display_name, spec in WORKFLOW_THRESHOLDS]
        criteria_str = ", ".join(criteria_parts)
        print(f"   ({criteria_str})")
        print("\n📊 CURRENT STATISTICS:")
        print("=" * 50)
        for i, message in enumerate(telegram_messages, 1):
            print(f"\n--- Report {i} ---")
            print(message)
        print("=" * 50)
        
        # If don't need to send when all good, exit
        if not send_when_all_good:
            return
    
    print(f"🚨 Found {len(stuck_jobs)} stuck jobs by our criteria")
    
    # Send to Telegram or show in dry-run mode
    if dry_run:
        print(f"\n📤 DRY-RUN: {len(telegram_messages)} message(s) for Telegram:")
        for i, message in enumerate(telegram_messages, 1):
            print(f"\n--- Message {i} ---")
            print("=" * 60)
            print(message)
            print("=" * 60)
        print("\n✅ Monitoring completed (dry-run mode)")
        sys.exit(0)
    else:
        print(f"📤 Sending {len(telegram_messages)} message(s) to Telegram {chat_id}:{thread_id}")
        
        success_count = 0
        for i, message in enumerate(telegram_messages, 1):
            print(f"📨 Sending message {i}/{len(telegram_messages)}...")
            if send_telegram_message(bot_token, chat_id, message, thread_id, "MarkdownV2"):
                success_count += 1
            else:
                print(f"❌ Error sending message {i}")
            
            # Add delay between messages (except last)
            if i < len(telegram_messages):
                print("⏳ Waiting 2 seconds before next message...")
                time.sleep(2)
        
        if success_count == len(telegram_messages):
            print("✅ Monitoring completed successfully")
            sys.exit(0)
        else:
            print(f"⚠️ Sent {success_count}/{len(telegram_messages)} messages")
            sys.exit(1)

if __name__ == "__main__":
    main()
