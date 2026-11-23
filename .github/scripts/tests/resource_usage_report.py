#!/usr/bin/env python3
"""Per-try suite/area resource totals from the original ya build-results report."""

import argparse
import csv
import json
import math
import stat
import struct
import sys
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent / "analytics"))
from testowners_utils import get_testowners_for_tests  # noqa: E402

METRICS = ("cpu_seconds", "read_bytes", "write_bytes")
ALL_METRICS = (*METRICS, "binary_bytes")


def coverage_key(key):
    return key + ("_suites" if key == "binary_bytes" else "_chunks")


def is_elf_executable(path):
    """Recognize executables (including PIE), excluding ELF objects/shared libraries."""
    with path.open("rb") as f:
        header = f.read(64)
        if len(header) < 64 or header[:4] != b"\x7fELF" or header[4] not in (1, 2) or header[5] not in (1, 2):
            return False
        endian = "<" if header[5] == 1 else ">"
        elf_type = struct.unpack_from(endian + "H", header, 16)[0]
        if elf_type == 2:  # ET_EXEC
            return True
        if elf_type != 3:  # ET_DYN: distinguish PIE from a shared library.
            return False
        is64 = header[4] == 2
        phoff = struct.unpack_from(endian + ("Q" if is64 else "I"), header, 32 if is64 else 28)[0]
        phsize, phnum = struct.unpack_from(endian + "HH", header, 54 if is64 else 42)
        if phsize < (56 if is64 else 32):
            return False
        for i in range(phnum):
            f.seek(phoff + i * phsize)
            ph = f.read(56 if is64 else 32)
            if len(ph) < (56 if is64 else 32):
                return False
            kind = struct.unpack_from(endian + "I", ph)[0]
            if kind == 3:  # PT_INTERP: dynamically linked executable.
                return True
            if kind == 2:  # PT_DYNAMIC: static PIE has DF_1_PIE but no interpreter.
                offset = struct.unpack_from(endian + ("Q" if is64 else "I"), ph, 8 if is64 else 4)[0]
                size = struct.unpack_from(endian + ("Q" if is64 else "I"), ph, 32 if is64 else 16)[0]
                f.seek(offset)
                entry_size = 16 if is64 else 8
                for _ in range(size // entry_size):
                    entry = f.read(entry_size)
                    if len(entry) != entry_size:
                        break
                    tag, value = struct.unpack(endian + ("QQ" if is64 else "II"), entry)
                    if tag == 0:  # DT_NULL
                        break
                    if tag == 0x6FFFFFFB and value & 0x08000000:  # DT_FLAGS_1 / DF_1_PIE
                        return True
    return False


def suite_binaries(build_output, suite_path):
    """Measure only the suite's own executable outputs, not recursive dependencies/logs."""
    relative = Path(suite_path)
    if not build_output or relative.is_absolute() or ".." in relative.parts:
        return {}
    directory = Path(build_output) / relative
    binaries = {}
    seen = set()
    try:
        files = sorted(directory.iterdir())
    except OSError:
        return binaries
    for path in files:
        try:
            info = path.stat()  # Follow ya output symlinks; count the target, not link text.
            identity = (info.st_dev, info.st_ino)
            if not stat.S_ISREG(info.st_mode) or identity in seen or not info.st_mode & 0o111:
                continue
            if is_elf_executable(path):
                binaries[str(relative / path.name)] = info.st_size
                seen.add(identity)
        except (OSError, ValueError, OverflowError):
            continue
    return binaries


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
    return dict(fields, chunks=0, **{k: None for k in ALL_METRICS},
                **{coverage_key(k): 0 for k in ALL_METRICS})


def add_totals(target, source):
    target["chunks"] += source["chunks"]
    for key in ALL_METRICS:
        if source.get(key) is not None:
            target[key] = (target[key] or 0) + source[key]
        target[coverage_key(key)] += source.get(coverage_key(key), 0)


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


def build_usage(results, area_mapping, assign_owners=get_testowners_for_tests, build_output=None):
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
        binaries = suite_binaries(build_output, suite["suite_folder"])
        suite["binary_bytes"] = sum(binaries.values()) if binaries else None
        suite["binary_bytes_suites"] = int(bool(binaries))
        suite["binary_paths"] = "; ".join(binaries)
        suite["area"] = owner_area(suite.get("owners", ""), area_mapping)
        area = areas.setdefault(suite["area"], empty_totals(area=suite["area"], suites=0))
        area["suites"] += 1
        add_totals(area, suite)
        add_totals(total, suite)
    for row in [*suites, *areas.values()]:
        for key in ALL_METRICS:
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
    return f"{value / divisor:,.3f}"


def write_report(report_path, output_dir, build_output=None):
    with open(report_path, encoding="utf-8") as f:
        report = json.load(f)
    mapping_path = SCRIPT_DIR.parents[1] / "config" / "owner_area_mapping.json"
    with mapping_path.open(encoding="utf-8") as f:
        usage = build_usage(report.get("results") or [], json.load(f), build_output=build_output)
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
    env.globals.update(format_usage=format_usage, coverage_key=coverage_key)
    html = env.get_template("resource_usage.html").render(**usage)
    # S3 publication generates directory listings named index.html.
    (output_dir / "report.html").write_text(html, encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--build-output", help="ya make --output directory containing built test executables")
    args = parser.parse_args()
    write_report(args.report, args.output_dir, args.build_output)
