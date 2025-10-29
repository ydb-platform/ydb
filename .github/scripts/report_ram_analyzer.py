#!/usr/bin/env python3

import argparse
import json
from collections import defaultdict
import os
import subprocess
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timezone
from telegram.send_telegram_message import send_telegram_message


def get_alert_logins() -> str:
    logins = os.getenv('GH_ALERTS_TG_LOGINS')
    return logins.strip() if logins else "@empEfarinov"


def get_current_workflow_url() -> str:
    github_repository = os.getenv('GITHUB_REPOSITORY', 'ydb-platform/ydb')
    github_run_id = os.getenv('GITHUB_RUN_ID')

    if github_run_id:
        return f"https://github.com/{github_repository}/actions/runs/{github_run_id}"
    return ""


def timstamp_to_time(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def get_total_runner_memory_in_gb():
    cmd = [
        'awk', '/MemTotal/ { printf "%.3f \\n", $2/1024/1024 }', '/proc/meminfo'
    ]
    return float(subprocess.run(cmd, text=True, capture_output=True, timeout=60).stdout)


def calculate_total_memory_consumption(processes):
    """
    Вычисляет суммарное потребление памяти для каждого момента времени.
    Корректно работает с несортированными данными.

    Args:
        processes: список кортежей (rss_consumption, path, start_time, end_time)

    Returns:
        timeline: отсортированный список временных меток
        memory_usage: список суммарного потребления памяти для каждой метки
    """
    processes = sorted(processes, key=lambda x: x[2])
    events = defaultdict(float)

    for rss, path, start, end in processes:
        events[start] += rss   # При старте добавляем память
        events[end] -= rss     # При завершении убираем память

    sorted_events = sorted(events.items(), key=lambda x: x[0])
    timeline = []
    memory_usage = []
    current_memory = 0

    for timestamp, delta in sorted_events:
        current_memory += delta  # Применяем изменение
        timeline.append(timestamp)
        memory_usage.append(round(current_memory, 2))
    return timeline, memory_usage


def get_active_processes_at_time(processes, target_time):
    """
    Возвращает список процессов, активных в указанный момент времени
    """
    active = []
    for rss, path, start, end in processes:
        if start <= target_time < end:
            active.append((rss, path, start, round(end)))
    return active


def create_simple_interactive_plot(processes, output_file):
    """Упрощённая версия с hover-информацией"""
    timeline, memory_usage = calculate_total_memory_consumption(processes)

    # Создаём subplot с дополнительной информацией
    fig = make_subplots(
        rows=1, cols=1,
        row_heights=[1,],
        subplot_titles=('Memory Consumption',),
        vertical_spacing=0.12
    )

    # Готовим hover-текст с информацией об активных процессах
    hover_texts = []
    process_counts = []
    timeline_in_time = list(map(timstamp_to_time, timeline))
    for t, mem in zip(timeline, memory_usage):
        active = get_active_processes_at_time(processes, t)
        process_counts.append(len(active))
        test_suites = defaultdict(float)
        for rss, path, _, _ in active:
            test_suites[path.split(' ')[0]] += rss
        test_suites = sorted(test_suites.items(),
                             key=lambda x: x[1], reverse=True)

        hover_text = f"<b>Time:</b> {timstamp_to_time(t)}<br>"
        hover_text += f"<b>Memory:</b> {mem} GB<br>"
        hover_text += f"<b>Processes:</b> {len(active)}<br><br>"

        if active:
            hover_text += "<b>Top 5 Test Suites:</b><br>"
            for suite, rss in test_suites[:5]:
                hover_text += f"  • {suite}: {round(rss, 2)} GB<br>"

        hover_texts.append(hover_text)

    # График памяти
    fig.add_trace(
        go.Scatter(
            x=timeline_in_time,
            y=memory_usage,
            mode='lines',
            name='Total RSS',
            line=dict(shape='hv', width=1, color='rgb(46, 134, 171)'),
            fill='tozeroy',
            fillcolor='rgba(46, 134, 171, 0.3)',
            hovertext=hover_texts,
            hoverinfo='text'
        ),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(
            x=timeline_in_time,
            y=process_counts,
            mode='lines',
            name='Active processes',
            line=dict(shape='hv', width=1, color='rgb(171, 134, 46)'),
        ),
        row=1, col=1
    )

    # Отмечаем пик
    max_memory = max(memory_usage)
    max_idx = memory_usage.index(max_memory)
    max_time = timeline[max_idx]

    if not output_file:
        print(hover_texts[max_idx].replace('<br>', '\n'))

    fig.add_trace(
        go.Scatter(
            x=[timstamp_to_time(max_time)],
            y=[max_memory],
            mode='markers+text',
            marker=dict(size=15, color='red', symbol='star'),
            text=[f'Peak: {max_memory} GB'],
            textposition='top center',
            name='Peak',
            showlegend=False
        ),
        row=1, col=1
    )

    fig.update_yaxes(title_text="Memory (GB)", row=1, col=1)

    fig.update_layout(
        height=800,
        hovermode='x unified',
        template='plotly_white',
        title_text="Interactive Memory Consumption Monitor"
    )
    if output_file:
        fig.write_html(output_file)
    else:
        fig.show()
    return max_memory


def parse_report_file(report_json):
    all = []
    for result in report_json["results"]:
        type_ = result["type"]
        if type_ == "test" and result.get("chunk"):
            rss_consumtion = result["metrics"].get(
                "suite_max_proc_tree_memory_consumption_kb", 0) / 1024 / 1024
            start_time = result["metrics"].get('suite_start_timestamp', 0)
            end_time = start_time + result["metrics"].get("wall_time", 0)
            path = result["path"] + " " + result.get("subtest_name", "")
            all.append((rss_consumtion, path, start_time, end_time))
    return all


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--report-file",
        help="path to file received via 'ya make ... --build-results-report <file>'",
        type=argparse.FileType("r"),
    )
    parser.add_argument(
        "--output-file",
        help="path to graph file"
    )
    parser.add_argument(
        "--output-file-url",
        help="Path to graph file in run artifacts"
    )
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
    parser.add_argument('--memory-threshold', type=float,
                        help='Threshold for used memory in percent. Default = 90',
                        default=90)
    args = parser.parse_args()

    report_file = args.report_file
    obj = json.load(report_file)
    all = parse_report_file(obj)
    output_file = args.output_file

    # Draw or export fig with RAM usage
    max_used_ram = create_simple_interactive_plot(all, output_file)

    max_agent_ram = get_total_runner_memory_in_gb()
    max_agent_ram_with_threshold = max_agent_ram * (args.memory_threshold / 100)
    if max_used_ram > max_agent_ram_with_threshold:
        print(f"Max used RAM {max_used_ram} is greater than max agent RAM {max_agent_ram}")

        bot_token = args.bot_token or os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = args.channel or args.chat_id or os.getenv('TELEGRAM_CHAT_ID')
        thread_id = args.thread_id or os.getenv('TELEGRAM_THREAD_ID')
        dry_run = args.dry_run or os.getenv('DRY_RUN', 'false').lower() == 'true'

        if not bot_token or not chat_id:
            print('No bot-token or chat-id was set. Forcing dry-run mode')
            dry_run = True

        message = f"""🚨 *Possible OOM*
During [RUN]({get_current_workflow_url()}) max used RAM *{round(max_used_ram, 1)}GB* is greater than agent RAM *{round(max_agent_ram_with_threshold, 1)}GB*
{max_agent_ram}GB total
Threshold is {args.memory_threshold}%

[Ram usage graph]({args.output_file_url})
CC {get_alert_logins()}"""
        if dry_run:
            print(message)
        else:
            if chat_id and not chat_id.startswith('-') and len(chat_id) >= 10:
                # Add -100 prefix for supergroup
                chat_id = f"-100{chat_id}"
            send_telegram_message(
                bot_token,
                chat_id,
                message,
                thread_id,
                "MarkdownV2")
