#!/usr/bin/env python3
"""
Script for monitoring workflow runs queued in GitHub Actions.
Sends notifications to Telegram when stuck jobs are detected.
"""

import requests
import json
import os
import sys
import argparse
import subprocess
from collections import Counter, defaultdict
from typing import Dict, List, Any
from datetime import datetime, timezone
import time

def get_alert_logins() -> str:
    """
    Gets the list of logins for notifications from GH_ALERTS_TG_LOGINS environment variable.
    
    Returns:
        String with logins separated by spaces, or default login
    """
    logins = os.getenv('GH_ALERTS_TG_LOGINS')
    return logins.strip() if logins else "@KirLynx"

def get_tail_message() -> str:
    """
    Generates TAIL_MESSAGE with dynamic logins.
    
    Returns:
        String with tail message
    """
    logins = get_alert_logins()
    return f"📊 [Dashboard details](https://datalens.yandex/wkptiaeyxz7qj?tab=ka)\n\nFYI: {logins}"

# Constants
TAIL_MESSAGE = get_tail_message()

# Filtering settings
MAX_AGE_DAYS = 3  # Maximum job age in days (excludes GitHub bugs)

# Message sending settings
SEND_WHEN_ALL_GOOD = False  # Whether to send a message when all jobs are working fine

# Criteria for determining stuck jobs
# Each element: [pattern, threshold_hours, display_name]
WORKFLOW_THRESHOLDS = [
    ["PR-check", 0.5, "PR-check"],
    ["Postcommit", 3, "Postcommit"],
    # Example of adding a new type:
    # ["Nightly", 12, "Nightly-Build"]
]

def fetch_workflow_runs(status: str = "queued", per_page: int = 1000, page: int = 1) -> tuple[Dict[str, Any], str]:
    """
    Fetches workflow runs data from GitHub API.
    
    Args:
        status: Status of workflow runs (queued, in_progress, completed, etc.)
        per_page: Number of records per page
        page: Page number
    
    Returns:
        Tuple (data, error). If successful - (data, ""), if error - ({}, error_message)
    """
    url = "https://api.github.com/repos/ydb-platform/ydb/actions/runs"
    params = {
        "per_page": per_page,
        "page": page,
        "status": status
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json(), ""
        else:
            # Возвращаем ошибку как есть от API
            try:
                error_data = response.json()
                error_message = error_data.get("message", f"HTTP {response.status_code}")
            except:
                error_message = f"HTTP {response.status_code}: {response.text}"
            
            return {}, error_message
            
    except requests.exceptions.Timeout:
        return {}, "Timeout: Запрос превысил время ожидания (30 сек)"
    except requests.exceptions.ConnectionError:
        return {}, "Connection error: Не удалось подключиться к API"
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
    run_started_at_str = run.get('run_started_at')
    updated_at_str = run.get('updated_at')
    created_at_str = run.get('created_at')
    
    # Для retry jobs используем updated_at (время последнего изменения статуса)
    # так как API с status=queued всегда возвращает только queued jobs
    if run_attempt > 1 and updated_at_str:
        try:
            return datetime.fromisoformat(updated_at_str.replace('Z', '+00:00'))
        except ValueError:
            pass
    
    # Для первой попытки используем created_at
    if created_at_str:
        try:
            return datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        except ValueError:
            pass
    
    # Fallback - текущее время
    return datetime.now(timezone.utc)

def is_retry_job(run: Dict[str, Any]) -> bool:
    """
    Determines if the job is a retry.
    
    Args:
        run: Workflow run object
    
    Returns:
        bool: True if this is a retry job
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
    
    current_time = datetime.now(timezone.utc)
    
    for run in workflow_runs:
        workflow_name = run.get('name', 'Unknown')
        run_id = run.get('id')
        
        workflow_info[workflow_name]['count'] += 1
        workflow_info[workflow_name]['runs'].append(run)
        
        # Получаем эффективное время начала (учитывает retry)
        effective_start_time = get_effective_start_time(run)
        
        # Проверяем, является ли этот run самым старым
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

def filter_old_jobs(workflow_runs: List[Dict[str, Any]], max_age_days: int = None) -> List[Dict[str, Any]]:
    """
    Filters jobs older than max_age_days (excludes GitHub bugs).
    
    Args:
        workflow_runs: List of workflow runs in queue
        max_age_days: Maximum age in days (defaults to MAX_AGE_DAYS)
    
    Returns:
        Filtered list of workflow runs
    """
    if max_age_days is None:
        max_age_days = MAX_AGE_DAYS
    current_time = datetime.now(timezone.utc)
    max_age_seconds = max_age_days * 24 * 3600
    filtered_runs = []
    excluded_count = 0
    
    for run in workflow_runs:
        created_at_str = run.get('created_at')
        if created_at_str:
            try:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                time_diff = current_time - created_at
                
                if time_diff.total_seconds() <= max_age_seconds:
                    filtered_runs.append(run)
                else:
                    excluded_count += 1
            except ValueError:
                # Если не можем распарсить время, включаем в отчет
                filtered_runs.append(run)
        else:
            # Если нет времени создания, включаем в отчет
            filtered_runs.append(run)
    
    if excluded_count > 0:
        print(f"⚠️ Исключено {excluded_count} jobs старше {max_age_days} дней (вероятно баги GitHub)")
    
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
    
    # Проверяем каждый тип workflow из конфигурации
    for pattern, threshold_hours, display_name in WORKFLOW_THRESHOLDS:
        if pattern in workflow_name and waiting_hours > threshold_hours:
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
    
    # Собираем описания по типам
    descriptions = []
    for pattern, threshold_hours, display_name in WORKFLOW_THRESHOLDS:
        count = stuck_counts.get(display_name, 0)
        if count > 0:
            job_word = "job" if count == 1 else "jobs"
            have_word = "has" if count == 1 else "have"
            hour_word = "hour" if threshold_hours == 1 else "hours"
            descriptions.append(f"⚠️ {display_name} {job_word} {have_word} been in the queue for more than {threshold_hours} {hour_word}! Total: {count} {job_word}.")
    
    # Добавляем Other если есть
    other_count = stuck_counts.get('Other', 0)
    if other_count > 0:
        job_word = "job" if other_count == 1 else "jobs"
        are_word = "is" if other_count == 1 else "are"
        descriptions.append(f"⚠️ Other {job_word} {are_word} stuck! Total: {other_count} {job_word}.")
    
    return descriptions

def count_stuck_jobs_by_type(stuck_jobs: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Counts the number of stuck jobs by types.
    
    Args:
        stuck_jobs: List of stuck jobs
    
    Returns:
        Dictionary with count of stuck jobs by types
    """
    # Инициализируем счетчики для всех типов из конфигурации
    counts = {}
    for pattern, threshold_hours, display_name in WORKFLOW_THRESHOLDS:
        counts[display_name] = 0
    counts['Other'] = 0
    
    for stuck_job in stuck_jobs:
        workflow_name = stuck_job['run'].get('name', '')
        found_type = False
        
        # Проверяем каждый тип из конфигурации
        for pattern, threshold_hours, display_name in WORKFLOW_THRESHOLDS:
            if pattern in workflow_name:
                counts[display_name] += 1
                found_type = True
                break
        
        # Если не найден ни один тип, добавляем в Other
        if not found_type:
            counts['Other'] += 1
    
    return counts

def check_for_stuck_jobs(workflow_runs: List[Dict[str, Any]], threshold_hours: int = 1) -> List[Dict[str, Any]]:
    """
    Finds "stuck" jobs by our criteria from WORKFLOW_THRESHOLDS.
    
    Args:
        workflow_runs: List of workflow runs in queue
        threshold_hours: Not used, kept for compatibility
    
    Returns:
        List of stuck jobs
    """
    stuck_jobs = []
    current_time = datetime.now(timezone.utc)
    
    for run in workflow_runs:
        # Получаем эффективное время начала (учитывает retry)
        effective_start_time = get_effective_start_time(run)
        time_diff = current_time - effective_start_time
        waiting_hours = time_diff.total_seconds() / 3600
        
        # Используем наши критерии для определения застрявших jobs
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
        excluded_count: Number of excluded jobs (older than MAX_AGE_DAYS days)
    
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
    
    if total_stuck > 0:
        # Show detailed statistics by types
        for pattern, threshold_hours, display_name in WORKFLOW_THRESHOLDS:
            if stuck_counts[display_name] > 0:
                message1_parts.append(f"• Stuck {display_name} (>{threshold_hours}h): {stuck_counts[display_name]}")
        if stuck_counts['Other'] > 0:
            message1_parts.append(f"• Stuck Other: {stuck_counts['Other']}")
    else:
        message1_parts.append(f"• Stuck: 0")
    
    if excluded_count > 0:
        message1_parts.append(f"• Excluded (>{MAX_AGE_DAYS}d): {excluded_count} jobs")
    message1_parts.append("")
    
    # Summary by workflow types
    if workflow_info:
        message1_parts.append("📋 *Workflows in queue:*")
        
        # Сначала показываем типы из WORKFLOW_THRESHOLDS
        threshold_workflows = []
        other_workflows = []
        
        for workflow_name, info in workflow_info.items():
            is_threshold_type = False
            for pattern, threshold_hours, display_name in WORKFLOW_THRESHOLDS:
                if pattern in workflow_name:
                    threshold_workflows.append((workflow_name, info))
                    is_threshold_type = True
                    break
            if not is_threshold_type:
                other_workflows.append((workflow_name, info))
        
        # Сортируем каждую группу по количеству jobs
        threshold_workflows.sort(key=lambda x: x[1]['count'], reverse=True)
        other_workflows.sort(key=lambda x: x[1]['count'], reverse=True)
        
        # Объединяем списки: сначала threshold типы, потом остальные
        all_workflows = threshold_workflows + other_workflows
        
        for workflow_name, info in all_workflows:  # Показываем все типы
            count = info['count']
            oldest_time = info['oldest_created_at']
            time_ago = format_time_ago(oldest_time)
            message1_parts.append(f"• `{workflow_name}`: {count} jobs (oldest: {time_ago})")
    
    message1_parts.append("")
    message1_parts.append(f"🕐 *Check time:* {datetime.now().strftime('%H:%M:%S UTC')}")
    
    messages.append("\n".join(message1_parts))
    
    # Second message - details about stuck jobs (only if any)
    if stuck_jobs:
        message2_parts = []
        message2_parts.append("🚨 *Stuck jobs:*")
        message2_parts.append("")
        
        # Sort by waiting time (oldest first)
        stuck_jobs_sorted = sorted(stuck_jobs, key=lambda x: x['waiting_hours'], reverse=True)
        
        for i, stuck_job in enumerate(stuck_jobs_sorted[:15], 1):  # Show up to 15 jobs
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
            
            github_url = f"https://github.com/ydb-platform/ydb/actions/runs/{run_id}" if run_id else "N/A"
            message2_parts.append(f"{i}. `{workflow_name}`{retry_info} - {waiting_str}")
            if run_id:
                message2_parts.append(f"   [Run {run_id}]({github_url})")
            message2_parts.append("")
        
        if len(stuck_jobs) > 15:
            message2_parts.append(f"• ... and {len(stuck_jobs) - 15} more jobs")
        
        # Add dashboard link
        message2_parts.append("")
        message2_parts.append(TAIL_MESSAGE)
        
        messages.append("\n".join(message2_parts))
    
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
    print(f"🔍 Тестируем соединение с Telegram для чата {chat_id}...")
    if thread_id:
        print(f"🔍 Тестируем thread {thread_id}...")
    
    # Отладочная информация
    print(f"🔍 Bot token: {bot_token[:10]}...{bot_token[-10:] if len(bot_token) > 20 else 'SHORT'}")
    print(f"🔍 Chat ID: {chat_id}")
    
    # Используем getChat метод вместо отправки сообщения
    url = f"https://api.telegram.org/bot{bot_token}/getChat"
    data = {'chat_id': chat_id}
    
    if thread_id:
        data['message_thread_id'] = thread_id
    
    try:
        response = requests.post(url, data=data, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        if result.get('ok'):
            print("✅ Соединение с Telegram успешно!")
            return True
        else:
            print(f"❌ Ошибка соединения с Telegram: {result.get('description', 'Неизвестная ошибка')}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка соединения с Telegram: {e}")
        return False

def get_current_workflow_url() -> str:
    """
    Gets current workflow run URL from GitHub Actions environment variables.
    
    Returns:
        Current workflow run URL or empty string if variables are unavailable
    """
    github_repository = os.getenv('GITHUB_REPOSITORY', 'ydb-platform/ydb')
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
    # Получаем ссылку на текущий workflow run
    workflow_url = get_current_workflow_url()
    workflow_link = f"\n\n🔗 [Workflow Run]({workflow_url})" if workflow_url else ""
    
    message = f"⚠️ *GITHUB ACTIONS MONITORING ERROR*\n\n{error_message}\n\n🕐 *Time:* {datetime.now().strftime('%H:%M:%S UTC')}{workflow_link}\n\n{TAIL_MESSAGE}"
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
        # Получаем путь к скрипту send_telegram_message.py
        script_dir = os.path.dirname(os.path.abspath(__file__))
        send_script = os.path.join(script_dir, 'send_telegram_message.py')
        
        # Вызываем внешний скрипт с сообщением
        print(f"Chat ID: {chat_id}")
        cmd = [
            'python3', send_script,
            '--bot-token', bot_token,
            '--chat-id', chat_id,
            '--message', message,
            '--parse-mode', parse_mode
        ]
        
        # Добавляем thread_id если указан
        if thread_id:
            cmd.extend(['--message-thread-id', str(thread_id)])
        
        result = subprocess.run(cmd, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✅ Сообщение отправлено в Telegram")
            return True
        else:
            print(f"❌ Ошибка отправки в Telegram (код {result.returncode})")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Таймаут при отправке в Telegram")
        return False
    except Exception as e:
        print(f"❌ Ошибка при вызове скрипта отправки: {e}")
        return False

def main():
    """Main script function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Мониторинг workflow runs в очереди GitHub Actions")
    parser.add_argument('--dry-run', action='store_true', 
                       help='Режим отладки без отправки в Telegram')
    parser.add_argument('--bot-token', 
                       help='Telegram bot token (или используйте TELEGRAM_BOT_TOKEN env var)')
    parser.add_argument('--chat-id', 
                       help='Telegram chat ID')
    parser.add_argument('--channel', 
                       help='Telegram channel ID (альтернатива для --chat-id)')
    parser.add_argument('--thread-id', type=int,
                       help='Telegram thread ID для групповых сообщений')
    parser.add_argument('--test-connection', action='store_true',
                       help='Только тестировать соединение с Telegram')
    parser.add_argument('--send-when-all-good', action='store_true',
                       help='Отправлять сообщение даже когда все jobs работают нормально')
    parser.add_argument('--notify-on-api-errors', action='store_true',
                       help='Отправлять уведомления в Telegram при ошибках API')
    
    args = parser.parse_args()
    
    print("🔍 Мониторинг workflow runs в очереди GitHub Actions")
    print("=" * 60)
    
    # Get parameters from arguments or environment variables
    bot_token = args.bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = args.channel or args.chat_id or os.getenv('TELEGRAM_CHAT_ID')
    thread_id = args.thread_id or os.getenv('TELEGRAM_THREAD_ID')
    dry_run = args.dry_run or os.getenv('DRY_RUN', 'false').lower() == 'true'
    send_when_all_good = args.send_when_all_good or os.getenv('SEND_WHEN_ALL_GOOD', 'false').lower() == 'true'
    notify_on_api_errors = args.notify_on_api_errors or os.getenv('NOTIFY_ON_API_ERRORS', 'false').lower() == 'true'
    
    # Исправляем формат chat_id для каналов (как в parse_and_send_team_issues.py)
    if chat_id and not chat_id.startswith('-') and len(chat_id) >= 10:
        # Добавляем -100 префикс для supergroup
        chat_id = f"-100{chat_id}"
    
    # Проверяем режим тестирования соединения
    if args.test_connection:
        if not bot_token:
            print("❌ TELEGRAM_BOT_TOKEN не установлен")
            print("   Используйте --bot-token или установите переменную окружения TELEGRAM_BOT_TOKEN")
            sys.exit(1)
        
        print("🔍 Тестируем соединение с Telegram...")
        if test_telegram_connection(bot_token, chat_id, thread_id):
            print("✅ Соединение успешно!")
            sys.exit(0)
        else:
            print("❌ Соединение не удалось!")
            sys.exit(1)
    
    if dry_run:
        print("🧪 РЕЖИМ DRY-RUN: Токены не требуются, сообщения не отправляются")
        print("=" * 60)
    elif not bot_token:
        print("❌ TELEGRAM_BOT_TOKEN не установлен")
        print("💡 Для локальной отладки используйте --dry-run")
        sys.exit(1)
    
    # Получаем данные для статуса "queued"
    print("📡 Загружаем данные для статуса: queued")
    data, error = fetch_workflow_runs(status="queued")
    
    if error:
        print(f"❌ GitHub API error: {error}")
        
        # Отправляем уведомление об ошибке API если включено
        if notify_on_api_errors:
            if dry_run:
                # Получаем ссылку на текущий workflow run для dry-run
                workflow_url = get_current_workflow_url()
                workflow_link = f"\n\n🔗 [Workflow Run]({workflow_url})" if workflow_url else ""
                
                print(f"\n📤 DRY-RUN: Уведомление об ошибке API для Telegram {chat_id}:{thread_id}")
                print("-" * 50)
                print(f"⚠️ *GITHUB ACTIONS MONITORING ERROR*\n\n{error}\n\n🕐 *Time:* {datetime.now().strftime('%H:%M:%S UTC')}{workflow_link}\n\n{TAIL_MESSAGE}")
                print("-" * 50)
            else:
                print("📤 Отправляем уведомление об ошибке API в Telegram...")
                if send_api_error_notification(bot_token, chat_id, error, thread_id):
                    print("✅ Уведомление об ошибке отправлено")
                else:
                    print("❌ Ошибка отправки уведомления об ошибке")
        
        sys.exit(1)
    
    queued_runs = data['workflow_runs']
    print(f"📊 Найдено {len(queued_runs)} workflow runs в очереди")
    
    # Проверяем, что получили данные
    if not queued_runs:
        print("✅ Нет workflow runs в очереди")
        # Отправляем сообщение о том, что очередь пуста
        message = "✅ *GITHUB ACTIONS MONITORING*\n\nQueue is empty - all jobs are working normally! 🎉"
        
        if dry_run:
            print(f"\n📤 DRY-RUN: Сообщение для Telegram:{chat_id}:{thread_id}")
            print("-" * 50)
            print(message)
            print("-" * 50)
        elif send_when_all_good:
            print(f"📤 Отправляем сообщение о пустой очереди в Telegram")
            if send_telegram_message(bot_token, chat_id, message, thread_id, "MarkdownV2"):
                print("✅ Сообщение о пустой очереди отправлено успешно")
            else:
                print("❌ Ошибка отправки сообщения о пустой очереди")
        else:
            print(f"📤 Очередь пуста - ничего не отправляем")
        return
    
    # Фильтруем старые jobs (старше MAX_AGE_DAYS дней)
    filtered_runs = filter_old_jobs(queued_runs)
    excluded_count = len(queued_runs) - len(filtered_runs)
    print(f"📊 После фильтрации: {len(filtered_runs)} workflow runs (исключены jobs старше {MAX_AGE_DAYS} дней)")
    
    if not filtered_runs:
        print("✅ Нет актуальных workflow runs в очереди")
        # Отправляем сообщение о том, что очередь пуста
        message = "✅ *GITHUB ACTIONS MONITORING*\n\nQueue is empty - all jobs are working normally! 🎉"
        
        if dry_run:
            print(f"\n📤 DRY-RUN: Сообщение для Telegram:{chat_id}:{thread_id}")
            print("-" * 50)
            print(message)
            print("-" * 50)
        elif send_when_all_good:
            print(f"📤 Отправляем сообщение о пустой очереди в Telegram")
            if send_telegram_message(bot_token, chat_id, message, thread_id, "MarkdownV2"):
                print("✅ Сообщение о пустой очереди отправлено успешно")
            else:
                print("❌ Ошибка отправки сообщения о пустой очереди")
        else:
            print(f"📤 Очередь пуста - ничего не отправляем")
        return
    
    # Анализируем данные
    workflow_info = analyze_queued_workflows(filtered_runs)
    total_queued = sum(info['count'] for info in workflow_info.values())
    
    # Проверяем на застрявшие jobs по нашим критериям
    stuck_jobs = check_for_stuck_jobs(filtered_runs, threshold_hours=1)
    
    # Формируем сообщения для Telegram (даже если не отправляем)
    telegram_messages = format_telegram_messages(workflow_info, stuck_jobs, total_queued, excluded_count)
    
    # Если нет застрявших jobs, проверяем нужно ли отправлять сообщение
    if not stuck_jobs:
        if send_when_all_good:
            print(f"✅ Нет застрявших jobs по нашим критериям - отправляем отчет о хорошем состоянии")
        else:
            print(f"✅ Нет застрявших jobs по нашим критериям - ничего не отправляем")
        
        # Формируем строку с критериями из конфигурации
        criteria_parts = []
        for pattern, threshold_hours, display_name in WORKFLOW_THRESHOLDS:
            criteria_parts.append(f"{display_name} >{threshold_hours}ч")
        criteria_str = ", ".join(criteria_parts)
        print(f"   ({criteria_str})")
        print("\n📊 ТЕКУЩАЯ СТАТИСТИКА:")
        print("=" * 50)
        for i, message in enumerate(telegram_messages, 1):
            print(f"\n--- Отчет {i} ---")
            print(message)
        print("=" * 50)
        
        # Если не нужно отправлять когда все хорошо, выходим
        if not send_when_all_good:
            return
    
    print(f"🚨 Найдено {len(stuck_jobs)} застрявших jobs по нашим критериям")
    
        # Отправляем в Telegram или показываем в dry-run режиме
    if dry_run:
        print(f"\n📤 DRY-RUN: {len(telegram_messages)} сообщение(й) для Telegram:")
        for i, message in enumerate(telegram_messages, 1):
            print(f"\n--- Сообщение {i} ---")
            print("=" * 60)
            print(message)
            print("=" * 60)
        print("\n✅ Мониторинг завершен (dry-run режим)")
        sys.exit(0)
    else:
        print(f"📤 Отправляем {len(telegram_messages)} сообщение(й) в Telegram {chat_id}:{thread_id}")
        
        success_count = 0
        for i, message in enumerate(telegram_messages, 1):
            print(f"📨 Отправляем сообщение {i}/{len(telegram_messages)}...")
            if send_telegram_message(bot_token, chat_id, message, thread_id, "MarkdownV2"):
                success_count += 1
            else:
                print(f"❌ Ошибка отправки сообщения {i}")
            
            # Добавляем задержку между сообщениями (кроме последнего)
            if i < len(telegram_messages):
                print("⏳ Ожидание 2 секунды перед следующим сообщением...")
                time.sleep(2)
        
        if success_count == len(telegram_messages):
            print("✅ Мониторинг завершен успешно")
            sys.exit(0)
        else:
            print(f"⚠️ Отправлено {success_count}/{len(telegram_messages)} сообщений")
            sys.exit(1)

if __name__ == "__main__":
    main()
