#!/usr/bin/env python3
"""Generate LaTeX tables and matplotlib charts from WikiSQL benchmark statistics.

The script scans a ``statistics`` directory for JSON files produced by
``WikiSqlBenchmark.collect_statistics`` and, for a given split argument
(e.g. ``validation0.01`` or ``test0.1``), produces:

1. Model comparison artifacts (tables + bar charts) for the subsubsection
   "Comparison of the performance of different models".
2. Impact-of-table-count artifacts (tables + line charts) for the subsubsection
   "Study of the impact of the number of tables on performance". This section is emitted
   only when statistics files for multiple ``tablesN`` values are available.

Run with::

    python wiki_sql_report.py validation0.01

By default reads from ``../statistics`` and writes to ``./report_output/<split>``.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


# Filename pattern: <model>-tables<N>-<split_with_rate>-run<R>wiki-sql.json
FILENAME_RE = re.compile(
    r"^(?P<model>.+)-tables(?P<tables>\d+)-(?P<split>.+?)-run(?P<run>\d+)wiki-sql\.json$"
)


@dataclass
class StatEntry:
    model: str
    tables: int
    split: str
    run: int
    path: Path
    data: Dict[str, Any]

    @property
    def key(self) -> Tuple[str, int]:
        return (self.model, self.tables)


# ---------------------------------------------------------------------------
# loading & parsing
# ---------------------------------------------------------------------------


def parse_filename(name: str) -> Optional[Dict[str, Any]]:
    match = FILENAME_RE.match(name)
    if not match:
        return None
    return {
        "model": match["model"],
        "tables": int(match["tables"]),
        "split": match["split"],
        "run": int(match["run"]),
    }


def discover_entries(stats_dir: Path, split: str) -> List[StatEntry]:
    entries: List[StatEntry] = []
    for path in sorted(stats_dir.iterdir()):
        if not path.is_file() or not path.name.endswith("wiki-sql.json"):
            continue
        info = parse_filename(path.name)
        if info is None or info["split"] != split:
            continue
        with open(path, encoding="utf-8") as fp:
            data = json.load(fp)
        entries.append(
            StatEntry(
                model=info["model"],
                tables=info["tables"],
                split=info["split"],
                run=info["run"],
                path=path,
                data=data,
            )
        )
    return entries


# ---------------------------------------------------------------------------
# formatting helpers
# ---------------------------------------------------------------------------


def fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}\\%"


def fmt_num(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "--"
    return f"{value:.{digits}f}"


def safe_div(num: float, den: float) -> float:
    return num / den if den else 0.0


# ---------------------------------------------------------------------------
# metric extraction
# ---------------------------------------------------------------------------


def entry_metrics(entry: StatEntry) -> Dict[str, Any]:
    data = entry.data
    total = data.get("total", 0) or 0
    matched = data.get("matched", 0) or 0
    timed_out = data.get("timed_out", 0) or 0
    with_answer = data.get("with_extracted_answer", 0) or 0

    tc = data.get("tool_calls", {}) or {}
    failed = tc.get("failed", {}) or {}

    latency = (data.get("latency", {}) or {}).get("evaluation_seconds", {}) or {}
    tokens = data.get("tokens", {}) or {}
    input_tokens = tokens.get("input_tokens_per_run", {}) or {}
    output_tokens = tokens.get("output_tokens_per_run", {}) or {}
    cached_tokens = tokens.get("cached_input_tokens_per_run", {}) or {}
    model_calls = tokens.get("model_calls_per_run", {}) or {}

    fatal_total = (data.get("fatal_errors", {}) or {}).get("total", 0) or 0

    runs_without_tool = tc.get("runs_without_tool", {}) or {}
    runs_without_list = int(runs_without_tool.get("list_directory", 0) or 0)
    runs_without_describe = int(runs_without_tool.get("describe", 0) or 0)

    accuracy = safe_div(matched, total)

    return {
        "total": total,
        "matched": matched,
        "timed_out": timed_out,
        "with_answer": with_answer,
        "accuracy": accuracy,
        "tool_calls_total": tc.get("total", 0) or 0,
        "tool_calls_avg": tc.get("avg_per_run", 0.0) or 0.0,
        "failed_avg": failed.get("avg_per_run", 0.0) or 0.0,
        "failed_max": failed.get("max_per_run", 0) or 0,
        "latency_avg": latency.get("avg", 0.0) or 0.0,
        "latency_p95": latency.get("p95", 0.0) or 0.0,
        "model_calls_avg": model_calls.get("avg", 0.0) or 0.0,
        "input_tokens_avg": input_tokens.get("avg", 0.0) or 0.0,
        "output_tokens_avg": output_tokens.get("avg", 0.0) or 0.0,
        "cached_tokens_avg": cached_tokens.get("avg", 0.0) or 0.0,
        "fatal_errors": fatal_total,
        "runs_without_list_directory": runs_without_list,
        "runs_without_describe": runs_without_describe,
    }


def per_operator_accuracy(entry: StatEntry) -> Dict[str, Tuple[int, int]]:
    """Return mapping operator -> (matched, total)."""
    agg = (entry.data.get("aggregation") or {}).get("per_operator") or {}
    out: Dict[str, Tuple[int, int]] = {}
    for op, stats in agg.items():
        matched = stats.get("matched", 0) or 0
        total = stats.get("total", 0) or 0
        out[op] = (matched, total)
    return out


# ---------------------------------------------------------------------------
# CSV table builders
# ---------------------------------------------------------------------------


def build_model_comparison_csv(chosen: List[StatEntry]) -> str:
    if not chosen:
        return ""

    lines: List[str] = []
    lines.append("Model,Total,Correct,Accuracy,N_instr.,N_err. (avg/max),Without list_dir/describe,t_lat. s")
    for entry in chosen:
        m = entry_metrics(entry)
        lines.append(
            f"{entry.model},"
            f"{m['total']},"
            f"{m['matched']},"
            f"{fmt_pct(m['accuracy']).replace('\\%', '%')},"
            f"{fmt_num(m['tool_calls_avg'])},"
            f"{fmt_num(m['failed_avg'])} / {m['failed_max']},"
            f"{m['runs_without_list_directory']} / {m['runs_without_describe']},"
            f"{fmt_num(m['latency_avg'])}"
        )
    return "\n".join(lines)


def build_token_usage_csv(chosen: List[StatEntry]) -> str:
    if not chosen:
        return ""

    lines: List[str] = []
    lines.append("Model,N_model calls,N_in tokens,N_out tokens,N_cached tokens")
    for entry in chosen:
        m = entry_metrics(entry)
        lines.append(
            f"{entry.model},"
            f"{fmt_num(m['model_calls_avg'])},"
            f"{fmt_num(m['input_tokens_avg'], 0)},"
            f"{fmt_num(m['output_tokens_avg'], 0)},"
            f"{fmt_num(m['cached_tokens_avg'], 0)}"
        )
    return "\n".join(lines)


def build_operator_accuracy_csv(chosen: List[StatEntry]) -> str:
    if not chosen:
        return ""

    operators_order = ["", "MAX", "MIN", "COUNT", "SUM", "AVG"]
    discovered: List[str] = []
    per_entry: Dict[str, Dict[str, Tuple[int, int]]] = {}
    for entry in chosen:
        ops = per_operator_accuracy(entry)
        per_entry[entry.model] = ops
        for op in ops:
            if op not in discovered:
                discovered.append(op)

    ordered_ops = [op for op in operators_order if op in discovered] + [
        op for op in discovered if op not in operators_order
    ]
    if not ordered_ops:
        return ""

    lines: List[str] = []
    header = "Model," + ",".join(op if op else "no agg." for op in ordered_ops)
    lines.append(header)
    for entry in chosen:
        ops = per_entry[entry.model]
        row = entry.model
        for op in ordered_ops:
            matched, total = ops.get(op, (0, 0))
            if total == 0:
                row += ",--"
            else:
                row += f",{fmt_pct(matched / total).replace(r'\%', '%')} ({matched}/{total})"
        lines.append(row)
    return "\n".join(lines)


def build_tables_impact_csv(entries: List[StatEntry]) -> Optional[str]:
    by_model: Dict[str, List[StatEntry]] = defaultdict(list)
    for e in entries:
        by_model[e.model].append(e)

    eligible = {model: sorted(es, key=lambda e: e.tables)
                for model, es in by_model.items() if len({e.tables for e in es}) > 1}
    if not eligible:
        return None

    lines: List[str] = []
    lines.append("Model,Tables,Total,Accuracy,N_instr.,N_err.,t_lat. s")
    for model in sorted(eligible):
        for entry in eligible[model]:
            m = entry_metrics(entry)
            lines.append(
                f"{entry.model},"
                f"{entry.tables},"
                f"{m['total']},"
                f"{fmt_pct(m['accuracy']).replace('\\%', '%')},"
                f"{fmt_num(m['tool_calls_avg'])},"
                f"{fmt_num(m['failed_avg'])},"
                f"{fmt_num(m['latency_avg'])}"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# matplotlib charts
# ---------------------------------------------------------------------------


def _configure_matplotlib() -> None:
    plt.rcParams["font.family"] = "DejaVu Sans"
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["axes.grid"] = True
    plt.rcParams["grid.alpha"] = 0.3


def _save_fig(fig, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_model_accuracy(chosen_tables: List[StatEntry], out_path: Path) -> None:
    chosen = sorted(chosen_tables, key=lambda e: entry_metrics(e)["accuracy"], reverse=True)
    if not chosen:
        return
    labels = [e.model for e in chosen]
    values = [entry_metrics(e)["accuracy"] * 100 for e in chosen]

    fig, ax = plt.subplots(figsize=(max(6, 1.1 * len(labels)), 4))
    bars = ax.bar(labels, values, color="#4472C4")
    ax.set_ylabel("Accuracy, %")
    ax.set_xlabel("Model")
    ax.set_title("Model response accuracy")
    ax.set_ylim(0, max(100, max(values) * 1.15))
    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                f"{value:.1f}%", ha="center", va="bottom", fontsize=9)
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")
    _save_fig(fig, out_path)


def plot_model_latency(chosen_tables: List[StatEntry], out_path: Path) -> None:
    chosen = sorted(chosen_tables, key=lambda e: entry_metrics(e)["latency_avg"])
    if not chosen:
        return
    labels = [e.model for e in chosen]
    avg = [entry_metrics(e)["latency_avg"] for e in chosen]
    p95 = [entry_metrics(e)["latency_p95"] for e in chosen]

    x = np.arange(len(labels))
    width = 0.38
    fig, ax = plt.subplots(figsize=(max(6, 1.1 * len(labels)), 4))
    ax.bar(x - width / 2, avg, width, label="Average", color="#4472C4")
    ax.bar(x + width / 2, p95, width, label="p95", color="#ED7D31")
    ax.set_ylabel("Time, s")
    ax.set_xlabel("Model")
    ax.set_title("Processing time per sample")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.legend()
    _save_fig(fig, out_path)


def plot_model_tools(chosen_tables: List[StatEntry], out_path: Path) -> None:
    chosen = sorted(chosen_tables, key=lambda e: entry_metrics(e)["tool_calls_avg"], reverse=True)
    if not chosen:
        return
    labels = [e.model for e in chosen]
    total = [entry_metrics(e)["tool_calls_avg"] for e in chosen]
    failed = [entry_metrics(e)["failed_avg"] for e in chosen]

    x = np.arange(len(labels))
    width = 0.38
    fig, ax = plt.subplots(figsize=(max(6, 1.1 * len(labels)), 4))
    ax.bar(x - width / 2, total, width, label="Total", color="#70AD47")
    ax.bar(x + width / 2, failed, width, label="Failed", color="#C00000")
    ax.set_ylabel("Calls per run (average)")
    ax.set_xlabel("Model")
    ax.set_title("Tool calls")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.legend()
    _save_fig(fig, out_path)


def plot_tables_impact(entries: List[StatEntry], out_path: Path) -> bool:
    by_model: Dict[str, List[StatEntry]] = defaultdict(list)
    for e in entries:
        by_model[e.model].append(e)
    eligible = {m: sorted(es, key=lambda e: e.tables)
                for m, es in by_model.items() if len({e.tables for e in es}) > 1}
    if not eligible:
        return False

    fig, ax = plt.subplots(figsize=(7, 4.5))
    cmap = plt.get_cmap("tab10")
    for idx, (model, items) in enumerate(sorted(eligible.items())):
        xs = [e.tables for e in items]
        ys = [entry_metrics(e)["accuracy"] * 100 for e in items]
        ax.plot(xs, ys, marker="o", color=cmap(idx % 10), label=model)
    ax.set_xlabel("Number of tables in database")
    ax.set_ylabel("Accuracy, %")
    ax.set_title("Impact of number of tables on accuracy")
    ax.legend()
    _save_fig(fig, out_path)
    return True


def plot_tables_impact_tool_calls(entries: List[StatEntry], out_path: Path) -> bool:
    by_model: Dict[str, List[StatEntry]] = defaultdict(list)
    for e in entries:
        by_model[e.model].append(e)
    eligible = {m: sorted(es, key=lambda e: e.tables)
                for m, es in by_model.items() if len({e.tables for e in es}) > 1}
    if not eligible:
        return False

    fig, ax = plt.subplots(figsize=(7, 4.5))
    cmap = plt.get_cmap("tab10")
    for idx, (model, items) in enumerate(sorted(eligible.items())):
        xs = [e.tables for e in items]
        ys = [entry_metrics(e)["tool_calls_avg"] for e in items]
        ax.plot(xs, ys, marker="s", color=cmap(idx % 10), label=model)
    ax.set_xlabel("Number of tables in database")
    ax.set_ylabel("Tool calls per run (average)")
    ax.set_title("Impact of number of tables on tool calls")
    ax.legend()
    _save_fig(fig, out_path)
    return True


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate LaTeX tables and matplotlib charts from "
                    "WikiSQL benchmark statistics for a given split."
    )
    parser.add_argument(
        "split",
        help="Split identifier as it appears in the filename, e.g. "
             "'validation0.01' or 'test0.1'.",
    )
    here = Path(__file__).resolve().parent
    parser.add_argument(
        "--stats-dir",
        type=Path,
        default=here.parent / "statistics",
        help="Directory with statistics JSON files (default: ../statistics).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Where to write the report (default: ./report_output/<split>).",
    )
    args = parser.parse_args(argv)

    stats_dir: Path = args.stats_dir
    if not stats_dir.is_dir():
        print(f"error: stats dir does not exist: {stats_dir}", file=sys.stderr)
        return 2

    output_dir: Path = args.output_dir or (here / "report_output" / args.split)
    figures_dir = output_dir / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    entries = discover_entries(stats_dir, args.split)
    if not entries:
        print(f"error: no statistics files found for split '{args.split}' in {stats_dir}",
              file=sys.stderr)
        return 1

    print(f"Loaded {len(entries)} statistics files for split '{args.split}':")
    for e in entries:
        m = entry_metrics(e)
        print(f"  - {e.path.name} (model={e.model}, tables={e.tables}, "
              f"total={m['total']}, matched={m['matched']}, "
              f"accuracy={m['accuracy']:.3f})")

    _configure_matplotlib()

    target_tables = max(
        sorted({e.tables for e in entries}),
        key=lambda c: sum(1 for e in entries if e.tables == c),
    )
    chosen_tables = [e for e in entries if e.tables == target_tables]
    chosen_tables.sort(key=lambda e: e.model.lower())

    # --- subsubsection 1: model comparison ---
    acc_fig = figures_dir / f"accuracy-{args.split}.pdf"
    lat_fig = figures_dir / f"latency-{args.split}.pdf"
    tools_fig = figures_dir / f"tool-calls-{args.split}.pdf"
    plot_model_accuracy(chosen_tables, acc_fig)
    plot_model_latency(chosen_tables, lat_fig)
    plot_model_tools(chosen_tables, tools_fig)

    # --- subsubsection 2: tables impact ---
    impact_acc_fig = figures_dir / f"tables-impact-accuracy-{args.split}.pdf"
    impact_tools_fig = figures_dir / f"tables-impact-tool-calls-{args.split}.pdf"
    has_impact_acc = plot_tables_impact(entries, impact_acc_fig)
    has_impact_tools = plot_tables_impact_tool_calls(entries, impact_tools_fig)

    # --- print model_table, token_table, op_table, impact_table
    tables_dir = output_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    model_csv = build_model_comparison_csv(chosen_tables)
    if model_csv:
        (tables_dir / "model_comparison.csv").write_text(model_csv, encoding="utf-8")

    token_csv = build_token_usage_csv(chosen_tables)
    if token_csv:
        (tables_dir / "token_usage.csv").write_text(token_csv, encoding="utf-8")

    op_csv = build_operator_accuracy_csv(chosen_tables)
    if op_csv:
        (tables_dir / "operator_accuracy.csv").write_text(op_csv, encoding="utf-8")

    impact_csv = build_tables_impact_csv(entries)
    if impact_csv:
        (tables_dir / "tables_impact.csv").write_text(impact_csv, encoding="utf-8")

    print(f"Wrote tables to: {tables_dir}")
    print(f"Wrote figures to: {figures_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
