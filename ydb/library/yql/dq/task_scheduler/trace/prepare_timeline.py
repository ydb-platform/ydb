#!/usr/bin/env python3
"""Merge DQ per-node captures into a Perfetto JSON timeline (stdlib only)."""

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def prepare(captures):
    """Return a trace and task summaries; retain raw events and their arguments."""
    events = []
    summaries = []
    processes = {}
    threads = {}
    states = defaultdict(list)
    task_info = {}
    totals = {}
    seen_captures = set()

    for capture in captures:
        if "dqTrace" not in capture or "traceEvents" not in capture:
            raise ValueError("Expected a query capture, not the query list or an HTTP error")
        summary = capture["dqTrace"]
        if summary.get("schema_version") != 1:
            raise ValueError("Unsupported DQ trace schema")
        identity = (summary["query"], summary["node_id"], summary["started_unix_us"])
        if identity in seen_captures:
            raise ValueError("Duplicate capture: download one snapshot per query and node")
        seen_captures.add(identity)
        summaries.append(summary)
        for source in capture["traceEvents"]:
            event = dict(source)
            args = dict(source.get("args", {}))
            event["args"] = args
            query = args.get("query", summary["query"])
            node = summary["node_id"]
            task = str(args.get("task_id", "0"))
            actor = args.get("actor", "")
            process_key = (query, node)
            pid = processes.setdefault(process_key, len(processes) + 1)
            key = (query, node, task, actor)
            tid = threads.setdefault(key, len(threads) + 1)
            event.update(pid=pid, tid=tid)
            args["node_id"] = node
            events.append(event)
            if event["name"] == "state":
                states[key].append(event)
            elif event["name"] == "task":
                task_info[key] = args
            elif event["name"] == "runner":
                progress = args.get("progress", {})
                total = totals.setdefault(key, dict(runs=0, cpu_us=0, elapsed_us=0,
                                                    input_rows=0, output_rows=0,
                                                    no_observed_progress_runs=0))
                total["runs"] += 1
                for metric in ("cpu_us", "elapsed_us", "input_rows", "output_rows"):
                    total[metric] += progress.get(metric, 0)
                total["no_observed_progress_runs"] += not progress.get("observed_progress", False)

    if not events:
        return {"traceEvents": [], "dqCaptures": summaries}, []

    first = min(event["ts"] for event in events)
    last = max(event["ts"] + event.get("dur", 0) for event in events)
    for key, changes in states.items():
        changes.sort(key=lambda event: (event["ts"], event["args"].get("revision", 0)))
        for index, change in enumerate(changes):
            args = change["args"]
            state = args["state"]
            if state not in ("blocked", "ready"):
                continue
            end = changes[index + 1]["ts"] if index + 1 < len(changes) else last
            if end <= change["ts"]:
                continue
            events.append(dict(name="queue_wait" if state == "ready" else "dependency_wait",
                               cat="dq.wait", ph="X", pid=change["pid"], tid=change["tid"],
                               ts=change["ts"], dur=end - change["ts"],
                               args=dict(args, derived=True, open_interval=index + 1 == len(changes))))

    for event in events:
        event["ts"] -= first
    for (query, node), pid in processes.items():
        events.append(dict(name="process_name", ph="M", pid=pid, tid=0,
                           args=dict(name=f"node {node} / query {query}")))
    rows = []
    for key, tid in threads.items():
        query, node, task, actor = key
        stage = task_info.get(key, {}).get("stage_id", "?")
        label = "query graph" if task == "0" else f"task {task} / stage {stage}"
        events.append(dict(name="thread_name", ph="M", pid=processes[(query, node)], tid=tid,
                           args=dict(name=label)))
        if key in totals:
            total = totals[key]
            rows.append(dict(query=query, node=node, task=task, actor=actor, **total,
                             input_rows_per_run=total["input_rows"] / total["runs"],
                             output_rows_per_run=total["output_rows"] / total["runs"]))
    rows.sort(key=lambda row: (row["query"], row["node"], row["task"], row["actor"]))
    return dict(traceEvents=events, displayTimeUnit="ms", dqCaptures=summaries,
                dqTimeline=dict(origin_unix_us=first,
                                clock_note="Cross-node alignment depends on wall-clock synchronization; "
                                           "local durations use a monotonic clock.")), rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("captures", type=Path, nargs="+", help="One JSON capture per query and node")
    parser.add_argument("-o", "--output", type=Path, required=True, help="Perfetto JSON output")
    parser.add_argument("--summary", type=Path, help="Optional per-task CSV of captured runner calls")
    options = parser.parse_args()
    try:
        captures = [json.loads(path.read_text()) for path in options.captures]
        result, rows = prepare(captures)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    options.output.write_text(json.dumps(result, separators=(",", ":")))
    if options.summary:
        fields = ["query", "node", "task", "actor", "runs", "cpu_us", "elapsed_us", "input_rows", "output_rows",
                  "no_observed_progress_runs", "input_rows_per_run", "output_rows_per_run"]
        with options.summary.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
    for summary in result.get("dqCaptures", []):
        if summary.get("truncated"):
            print(f"WARNING: node {summary['node_id']}, query {summary['query']}: "
                  f"{summary['dropped_events']} events dropped; timeline and totals are incomplete")
        if not summary.get("completed"):
            print(f"WARNING: node {summary['node_id']}, query {summary['query']}: capture still active")


if __name__ == "__main__":
    main()
