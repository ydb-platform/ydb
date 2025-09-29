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

# Константы
TAIL_MESSAGE = "📊 [Подробности на дашборде](https://datalens.yandex/wkptiaeyxz7qj?tab=ka)\n\nFYI: @KirLynx"

# Настройки фильтрации
MAX_AGE_DAYS = 3  # Максимальный возраст jobs в днях (исключаем баги GitHub)

# Настройки отправки сообщений
SEND_WHEN_ALL_GOOD = False  # Whether to send a message when all jobs are working fine

# Критерии для определения застрявших jobs
# Каждый элемент: [pattern, threshold_hours, display_name]
WORKFLOW_THRESHOLDS = [
    ["PR-check", 1, "PR-check"],
    ["Postcommit", 6, "Postcommit"],
    # Пример добавления нового типа:
    # ["Nightly", 12, "Nightly-Build"]
]

def fetch_workflow_runs(status: str = "queued", per_page: int = 1000, page: int = 1) -> Dict[str, Any]:
    """
    Получает данные о workflow runs из GitHub API.
    
    Args:
        status: Статус workflow runs (queued, in_progress, completed, etc.)
        per_page: Количество записей на страницу
        page: Номер страницы
    
    Returns:
        Словарь с данными API ответа
    """
    url = "https://api.github.com/repos/ydb-platform/ydb/actions/runs"
    params = {
        "per_page": per_page,
        "page": page,
        "status": status
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error when requesting API: {e}")
        return {}

def analyze_queued_workflows(workflow_runs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Анализирует workflow runs в очереди и находит самый старый job для каждого типа.
    
    Args:
        workflow_runs: Список workflow runs из API
    
    Returns:
        Словарь с информацией о каждом типе workflow
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
        created_at_str = run.get('created_at')
        run_id = run.get('id')
        
        workflow_info[workflow_name]['count'] += 1
        workflow_info[workflow_name]['runs'].append(run)
        
        if created_at_str:
            try:
                # Парсим ISO 8601 формат времени
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                
                # Проверяем, является ли этот run самым старым
                if (workflow_info[workflow_name]['oldest_created_at'] is None or 
                    created_at < workflow_info[workflow_name]['oldest_created_at']):
                    workflow_info[workflow_name]['oldest_created_at'] = created_at
                    workflow_info[workflow_name]['oldest_run_id'] = run_id
                    
            except ValueError as e:
                print(f"Ошибка парсинга времени для run {run_id}: {e}")
    
    return dict(workflow_info)

def format_time_ago(created_at: datetime) -> str:
    """
    Форматирует время в удобочитаемый вид "X минут/часов/дней назад".
    
    Args:
        created_at: Время создания
    
    Returns:
        Строка с описанием времени
    """
    if created_at is None:
        return "Неизвестно"
    
    current_time = datetime.now(timezone.utc)
    time_diff = current_time - created_at
    
    total_seconds = time_diff.total_seconds()
    
    if total_seconds < 60:
        return f"{int(total_seconds)} сек"
    elif total_seconds < 3600:
        minutes = total_seconds / 60
        return f"{minutes:.1f} мин"
    elif total_seconds < 86400:
        hours = total_seconds / 3600
        return f"{hours:.1f} ч"
    else:
        days = total_seconds / 86400
        return f"{days:.1f} дн"

def filter_old_jobs(workflow_runs: List[Dict[str, Any]], max_age_days: int = None) -> List[Dict[str, Any]]:
    """
    Фильтрует jobs старше max_age_days (исключает баги GitHub).
    
    Args:
        workflow_runs: Список workflow runs в очереди
        max_age_days: Максимальный возраст в днях (по умолчанию MAX_AGE_DAYS)
    
    Returns:
        Отфильтрованный список workflow runs
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
    Проверяет, является ли job застрявшим по нашим критериям.
    
    Args:
        run: Workflow run объект
        waiting_hours: Время ожидания в часах
    
    Returns:
        bool: True если job считается застрявшим
    """
    workflow_name = run.get('name', '')
    
    # Проверяем каждый тип workflow из конфигурации
    for pattern, threshold_hours, display_name in WORKFLOW_THRESHOLDS:
        if pattern in workflow_name and waiting_hours > threshold_hours:
            return True
    
    return False

def count_stuck_jobs_by_type(stuck_jobs: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Подсчитывает количество застрявших jobs по типам.
    
    Args:
        stuck_jobs: Список застрявших jobs
    
    Returns:
        Словарь с количеством застрявших jobs по типам
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
    Находит "застрявшие" jobs по нашим критериям из WORKFLOW_THRESHOLDS.
    
    Args:
        workflow_runs: Список workflow runs в очереди
        threshold_hours: Не используется, оставлен для совместимости
    
    Returns:
        Список застрявших jobs
    """
    stuck_jobs = []
    current_time = datetime.now(timezone.utc)
    
    for run in workflow_runs:
        created_at_str = run.get('created_at')
        if created_at_str:
            try:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                time_diff = current_time - created_at
                waiting_hours = time_diff.total_seconds() / 3600
                
                # Используем наши критерии для определения застрявших jobs
                if is_job_stuck_by_criteria(run, waiting_hours):
                    stuck_jobs.append({
                        'run': run,
                        'waiting_hours': waiting_hours
                    })
            except ValueError:
                pass
    
    return stuck_jobs

def format_telegram_messages(workflow_info: Dict[str, Dict[str, Any]], stuck_jobs: List[Dict[str, Any]], total_queued: int, excluded_count: int = 0) -> List[str]:
    """
    Форматирует сообщения для отправки в Telegram (разбивает на 2 части).
    
    Args:
        workflow_info: Информация о workflow
        stuck_jobs: Список застрявших jobs
        total_queued: Общее количество jobs в очереди
        excluded_count: Количество исключенных jobs (старше 3 дней)
    
    Returns:
        Список из 2 сообщений для Telegram
    """
    messages = []
    
    # Первое сообщение - общая статистика
    message1_parts = []
    
    # Заголовок
    if stuck_jobs:
        message1_parts.append("🚨 *МОНИТОРИНГ GITHUB ACTIONS*")
        message1_parts.append("⚠️ *Обнаружены застрявшие jobs!*")
    else:
        message1_parts.append("✅ *МОНИТОРИНГ GITHUB ACTIONS*")
        message1_parts.append("Все jobs в очереди работают нормально")
    
    message1_parts.append("")
    
    # Общая статистика
    message1_parts.append(f"📊 *Статистика:*")
    message1_parts.append(f"• Всего в очереди: {total_queued} jobs")
    
    # Статистика застрявших jobs по типам
    stuck_counts = count_stuck_jobs_by_type(stuck_jobs)
    total_stuck = sum(stuck_counts.values())
    
    if total_stuck > 0:
        # Показываем детальную статистику по типам
        for pattern, threshold_hours, display_name in WORKFLOW_THRESHOLDS:
            if stuck_counts[display_name] > 0:
                message1_parts.append(f"• Застрявших {display_name} (>{threshold_hours}ч): {stuck_counts[display_name]}")
        if stuck_counts['Other'] > 0:
            message1_parts.append(f"• Застрявших Other: {stuck_counts['Other']}")
    else:
        message1_parts.append(f"• Застрявших: 0")
    
    if excluded_count > 0:
        message1_parts.append(f"• Исключено (>3дн): {excluded_count} jobs")
    message1_parts.append("")
    
    # Сводка по типам workflow
    if workflow_info:
        message1_parts.append("📋 *По типам workflow:*")
        
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
            message1_parts.append(f"• `{workflow_name}`: {count} jobs (старейший: {time_ago})")
    
    message1_parts.append("")
    message1_parts.append(f"🕐 *Время проверки:* {datetime.now().strftime('%H:%M:%S UTC')}")
    
    messages.append("\n".join(message1_parts))
    
    # Второе сообщение - детали по застрявшим jobs (только если есть)
    if stuck_jobs:
        message2_parts = []
        message2_parts.append("🚨 *Застрявшие jobs:*")
        message2_parts.append("")
        
        # Сортируем по времени ожидания (самые старые сначала)
        stuck_jobs_sorted = sorted(stuck_jobs, key=lambda x: x['waiting_hours'], reverse=True)
        
        for i, stuck_job in enumerate(stuck_jobs_sorted[:15], 1):  # Показываем до 15 jobs
            run = stuck_job['run']
            waiting_hours = stuck_job['waiting_hours']
            workflow_name = run.get('name', 'Unknown')
            run_id = run.get('id')
            
            if waiting_hours > 24:
                waiting_str = f"{waiting_hours/24:.1f} дн"
            elif waiting_hours > 1:
                waiting_str = f"{waiting_hours:.1f} ч"
            else:
                waiting_str = f"{waiting_hours*60:.0f} мин"
            
            github_url = f"https://github.com/ydb-platform/ydb/actions/runs/{run_id}" if run_id else "N/A"
            message2_parts.append(f"{i}. `{workflow_name}` - {waiting_str}")
            if run_id:
                message2_parts.append(f"   [Run {run_id}]({github_url})")
            message2_parts.append("")
        
        if len(stuck_jobs) > 15:
            message2_parts.append(f"• ... и еще {len(stuck_jobs) - 15} jobs")
        
        # Добавляем ссылку на дашборд
        message2_parts.append("")
        message2_parts.append(TAIL_MESSAGE)
        
        messages.append("\n".join(message2_parts))
    
    return messages

def test_telegram_connection(bot_token: str, chat_id: str, thread_id: int = None) -> bool:
    """
    Тестирует соединение с Telegram без отправки сообщений.
    
    Args:
        bot_token: Токен Telegram бота
        chat_id: ID чата
        thread_id: ID thread для групповых сообщений
    
    Returns:
        True если соединение успешно, False иначе
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

def send_telegram_message(bot_token: str, chat_id: str, message: str, thread_id: int = None, parse_mode: str = "MarkdownV2") -> bool:
    """
    Отправляет сообщение в Telegram используя внешний скрипт.
    
    Args:
        bot_token: Токен Telegram бота
        chat_id: ID чата
        message: Текст сообщения
        thread_id: ID thread для групповых сообщений
    
    Returns:
        True если успешно, False иначе
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
    """Основная функция скрипта."""
    # Парсим аргументы командной строки
    parser = argparse.ArgumentParser(description="Мониторинг workflow runs в очереди GitHub Actions")
    parser.add_argument('--dry-run', action='store_true', 
                       help='Режим отладки без отправки в Telegram')
    parser.add_argument('--bot-token', 
                       help='Telegram bot token (или используйте TELEGRAM_BOT_TOKEN env var)')
    parser.add_argument('--chat-id', 
                       help='Telegram chat ID (по умолчанию: 1003017506311)')
    parser.add_argument('--channel', 
                       help='Telegram channel ID (альтернатива для --chat-id)')
    parser.add_argument('--thread-id', type=int,
                       help='Telegram thread ID для групповых сообщений')
    parser.add_argument('--test-connection', action='store_true',
                       help='Только тестировать соединение с Telegram')
    parser.add_argument('--send-when-all-good', action='store_true',
                       help='Отправлять сообщение даже когда все jobs работают нормально')
    
    args = parser.parse_args()
    
    print("🔍 Мониторинг workflow runs в очереди GitHub Actions")
    print("=" * 60)
    
    # Получаем параметры из аргументов или переменных окружения
    bot_token = args.bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
    chat_id = args.channel or args.chat_id or os.getenv('TELEGRAM_CHAT_ID', '1003017506311')
    thread_id = args.thread_id or os.getenv('TELEGRAM_THREAD_ID')
    dry_run = args.dry_run or os.getenv('DRY_RUN', 'false').lower() == 'true'
    send_when_all_good = args.send_when_all_good or os.getenv('SEND_WHEN_ALL_GOOD', 'false').lower() == 'true'
    
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
    data = fetch_workflow_runs(status="queued")
    
    if 'workflow_runs' not in data:
        print("❌ Не удалось получить данные из API")
        sys.exit(1)
    
    queued_runs = data['workflow_runs']
    print(f"📊 Найдено {len(queued_runs)} workflow runs в очереди")
    
    # Фильтруем старые jobs (старше MAX_AGE_DAYS дней)
    filtered_runs = filter_old_jobs(queued_runs)
    excluded_count = len(queued_runs) - len(filtered_runs)
    print(f"📊 После фильтрации: {len(filtered_runs)} workflow runs (исключены jobs старше {MAX_AGE_DAYS} дней)")
    
    if not filtered_runs:
        print("✅ Нет актуальных workflow runs в очереди")
        # Отправляем сообщение о том, что очередь пуста
        message = "✅ *МОНИТОРИНГ GITHUB ACTIONS*\n\nОчередь пуста - все jobs работают нормально! 🎉"
        
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
