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


def timestamp_to_time(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def get_total_runner_memory_in_gb():
    cmd = [
        'awk', '/MemTotal/ { printf "%.3f \\n", $2/1024/1024 }', '/proc/meminfo'
    ]
    return float(subprocess.run(cmd, text=True, capture_output=True, timeout=60).stdout)


def calculate_total_memory_consumption(processes):
    """
    Calculates the total memory consumption of running suites for each moment in time.

    Args:
        processes: list of tuples (rss_consumption, path, start_time, end_time)

    Returns:
        timeline: sorted list of timestamps
        memory_usage: list of total memory consumption for each timestamp
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
    Returns a list of tests that were active at the given time.
    """
    active = []
    for rss, path, start, end in processes:
        if start <= target_time < end:
            active.append((rss, path, start, round(end)))
    return active


def create_simple_interactive_plot(processes, ram_usage_with_ts, output_file):
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
    timeline_in_time = list(map(timestamp_to_time, timeline))
    for t, mem in zip(timeline, memory_usage):
        active = get_active_processes_at_time(processes, t)
        process_counts.append(len(active))
        test_suites = defaultdict(float)
        for rss, path, _, _ in active:
            test_suites[path.split(' ')[0]] += rss
        test_suites = sorted(test_suites.items(),
                             key=lambda x: x[1], reverse=True)

        hover_text = f"<b>Time:</b> {timestamp_to_time(t)}<br>"
        hover_text += f"<b>Sum of suites:</b> {mem} GB<br>"
        hover_text += f"<b>Processes:</b> {len(active)}<br><br>"

        if active:
            hover_text += "<b>Top 5 Test Suites:</b><br>"
            for suite, rss in test_suites[:5]:
                hover_text += f"  • {suite}: {round(rss, 2)} GB<br>"

        hover_texts.append(hover_text)

    min_ts = timeline[0]
    max_ts = timeline[-1]
    ram_usage_with_ts = list(filter(lambda x: min_ts <= x[0] <= max_ts, ram_usage_with_ts))
    ram_usage = list(map(lambda x: x[1], ram_usage_with_ts))
    ts_of_ram_usage = list(map(lambda x: timestamp_to_time(x[0]), ram_usage_with_ts))
    fig.add_trace(
        go.Scatter(
            x=ts_of_ram_usage,
            y=ram_usage,
            mode='lines',
            name='RSS, meminfo',
            line=dict(shape='hv', width=1, color='rgb(115, 187, 142)'),
            fill='tozeroy',
            fillcolor='rgba(115, 187, 142, 0.3)',
        ),
        row=1, col=1
    )

    fig.add_trace(
        go.Scatter(
            x=timeline_in_time,
            y=memory_usage,
            mode='lines',
            name='RSS, ya make',
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
            line=dict(shape='hv', width=1, color='rgb(248, 157, 33)'),
        ),
        row=1, col=1
    )

    # Отмечаем пик
    max_memory = max(ram_usage)
    max_idx = ram_usage.index(max_memory)
    max_time = ts_of_ram_usage[max_idx]

    if not output_file:
        print(hover_texts[max_idx].replace('<br>', '\n'))

    fig.add_trace(
        go.Scatter(
            x=[max_time],
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


def parse_ram_usage_file(ram_usage_file):
    ram_usage = []
    for line in ram_usage_file:
        timestamp, ram = line.strip().split()
        ram_usage.append((int(timestamp), float(ram) / 1024 / 1024))
    return ram_usage


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--report-file",
        help="path to file received via 'ya make ... --build-results-report <file>'",
        type=argparse.FileType("r"),
        required=True
    )
    parser.add_argument(
        "--ram-usage-file",
        help="path to file with timestamped RAM consumption",
        type=argparse.FileType("r"),
        required=True
    )
    parser.add_argument(
        "--output-file",
        help="path to graph file",
        required=True
    )
    parser.add_argument(
        "--output-file-url",
        help="Path to graph file in run artifacts",
        required=True
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

    ram_usage = parse_ram_usage_file(args.ram_usage_file)
    output_file = args.output_file
    # Draw or export fig with RAM usage
    create_simple_interactive_plot(all, ram_usage, output_file)

    max_used_ram = max(ram_usage, key=lambda x: x[1])[1]
    max_agent_ram = get_total_runner_memory_in_gb()
    max_agent_ram_with_threshold = max_agent_ram * (args.memory_threshold / 100)
    print(f"Max used RAM {max_used_ram}, max agent RAM {max_agent_ram}")

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
During [RUN]({get_current_workflow_url()}) max used RAM *{round(max_used_ram, 1)}GB* is greater than agent RAM under threshold - *{round(max_agent_ram_with_threshold, 1)}GB*
{max_agent_ram}GB total available
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
                parse_mode='MarkdownV2',
                message_thread_id=thread_id)
