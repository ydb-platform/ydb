#!/usr/bin/env python3
"""Per-try suite/area resource totals from the original ya build-results report."""

import argparse
import csv
import json
import math
import sys
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent / "analytics"))
from testowners_utils import get_testowners_for_tests  # noqa: E402

METRICS = ("cpu_seconds", "read_bytes", "write_bytes")


def number(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) and value >= 0 else None


def chunk_usage(metrics):
    user = number(metrics.get("ru_utime"))
    system = number(metrics.get("ru_stime"))
    read = number(metrics.get("ru_inblock"))
    write = number(metrics.get("ru_oublock"))
    # Linux task_io_get_{in,out}block reports 512-byte blocks, not I/O operations.
    return {
        "cpu_seconds": user + system if user is not None and system is not None else None,
        "read_bytes": read * 512 if read is not None else None,
        "write_bytes": write * 512 if write is not None else None,
    }


def empty_totals(**fields):
    return dict(fields, chunks=0, **{k: None for k in METRICS},
                **{k + "_chunks": 0 for k in METRICS})


def add_totals(target, source):
    target["chunks"] += source["chunks"]
    for key in METRICS:
        if source[key] is not None:
            target[key] = (target[key] or 0) + source[key]
        target[key + "_chunks"] += source[key + "_chunks"]


def owner_area(owners, mapping):
    areas = set()
    for owner in owners.split(";;"):
        if owner.startswith("TEAM:@ydb-platform/"):
            team = owner.rsplit("/", 1)[-1].lower()
            areas.add(mapping.get(team, "team/" + team))
    # Shared ownership is one group: never charge the full cost to multiple teams.
    return " + ".join(sorted(areas)) if areas else "unassigned"


def percentage(value, total):
    return 100 * value / total if value is not None and total else None


def build_usage(results, area_mapping, assign_owners=get_testowners_for_tests):
    suites = {}
    for result in results:
        # Individual cases overlap with chunk metrics. Suite summaries may overlap too.
        if result.get("type") != "test" or not (result.get("chunk") or result.get("suite")):
            continue
        path = result.get("path") or "(unknown suite)"
        suite = suites.setdefault(path, empty_totals(suite_folder=path))
        if result.get("suite"):
            continue
        usage = chunk_usage(result.get("metrics") or {})
        add_totals(suite, dict(usage, chunks=1,
                              **{k + "_chunks": int(usage[k] is not None) for k in METRICS}))

    suites = list(suites.values())
    assign_owners(suites)
    areas = {}
    total = empty_totals(suites=len(suites))
    for suite in suites:
        suite["area"] = owner_area(suite.get("owners", ""), area_mapping)
        area = areas.setdefault(suite["area"], empty_totals(area=suite["area"], suites=0))
        area["suites"] += 1
        add_totals(area, suite)
        add_totals(total, suite)
    for row in [*suites, *areas.values()]:
        for key in METRICS:
            row[key + "_pct_total"] = percentage(row[key], total[key])
            if "suite_folder" in row:
                row[key + "_pct_area"] = percentage(row[key], areas[row["area"]][key])
    return {"totals": total,
            "areas": sorted(areas.values(), key=lambda r: (-(r["cpu_seconds"] or 0), r["area"])),
            "suites": sorted(suites, key=lambda r: (-(r["cpu_seconds"] or 0), r["suite_folder"]))}


def format_usage(row, key):
    value = row[key]
    if value is None:
        return "—"
    divisor = 3600 if key == "cpu_seconds" else 2**30
    partial = " †" if row[key + "_chunks"] < row["chunks"] else ""
    return f"{value / divisor:,.3f}{partial}"


def write_report(report_path, output_dir):
    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)
    mapping_path = SCRIPT_DIR.parents[1] / "config" / "owner_area_mapping.json"
    with mapping_path.open(encoding="utf-8") as f:
        usage = build_usage(report.get("results") or [], json.load(f))
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in ("areas", "suites"):
        with (output_dir / (name + ".csv")).open("w", newline="", encoding="utf-8") as f:
            rows = usage[name]
            if rows:
                writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
    (output_dir / "usage.json").write_text(json.dumps(usage, allow_nan=False), encoding="utf-8")
    env = Environment(loader=FileSystemLoader(SCRIPT_DIR / "templates"),
                      autoescape=True, undefined=StrictUndefined)
    env.globals.update(format_usage=format_usage)
    html = env.get_template("resource_usage.html").render(**usage)
    (output_dir / "index.html").write_text(html, encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    write_report(args.report, args.output_dir)
