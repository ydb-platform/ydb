#!/usr/bin/env python3
import argparse
import heapq
import re
import shlex
import subprocess
from datetime import datetime
from pathlib import Path

REMOTE_SERVICES_GLOB = "/Berkanavt/kikimr*"
ISO_TS_RE = re.compile(r"(?P<iso>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)")
REMOTE_TMP_DIR = "/tmp"


def run(cmd: list[str]) -> None:
    print("+", " ".join(shlex.quote(x) for x in cmd))
    subprocess.run(cmd, check=True)


def extract_sort_key(line: str) -> tuple[int, int, int, int, int, int, int]:
    """Return sortable datetime tuple extracted from ISO timestamp in line."""
    iso_match = ISO_TS_RE.search(line)
    if not iso_match:
        return (9999, 12, 31, 23, 59, 59, 999999)

    iso = iso_match.group("iso")
    try:
        dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        try:
            dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return (9999, 12, 31, 23, 59, 59, 999999)

    return (dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond)


def build_remote_command(pattern: str, log_history_length: str) -> str:
    script = f"""
set -euo pipefail
shopt -s nullglob

REMOTE_SERVICES_GLOB={shlex.quote(REMOTE_SERVICES_GLOB)}
REMOTE_TMP_DIR={shlex.quote(REMOTE_TMP_DIR)}
PATTERN={shlex.quote(pattern)}
LOG_HISTORY_LENGTH={shlex.quote(log_history_length)}

collect_file() {{
    local out_file="$1"
    local f="$2"
    [[ -f "$f" ]] || return
    if [[ "$f" == *.gz ]]; then
        zgrep -E -- "$PATTERN" "$f" >> "$out_file" || true
    else
        grep -E -- "$PATTERN" "$f" >> "$out_file" || true
    fi
}}

for service_dir in $REMOTE_SERVICES_GLOB; do
    [[ -d "$service_dir" ]] || continue
    service_name="$(basename "$service_dir")"
    service_log_dir="$service_dir/log"
    [[ -d "$service_log_dir" ]] || continue

    out_file="$REMOTE_TMP_DIR/filtered.${{service_name}}.log"
    rm -f "$out_file"
    touch "$out_file"

    collect_file "$out_file" "$service_log_dir/kikimr.log"

    if [[ "$LOG_HISTORY_LENGTH" == "all" ]]; then
        for f in "$service_log_dir"/kikimr.log.*; do
            collect_file "$out_file" "$f"
        done
    else
        for ((i=1; i<=LOG_HISTORY_LENGTH; i++)); do
            collect_file "$out_file" "$service_log_dir/kikimr.log.$i.gz"
        done
    fi

    echo "$out_file"
done
"""
    return f"bash -lc {shlex.quote(script)}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect and merge filtered kikimr logs from remote nodes via pssh")
    p.add_argument("--pattern", required=True, help="Regex pattern to grep from logs")
    p.add_argument("--pssh_nodes_pattern", required=True, help="Pattern passed to pssh")
    p.add_argument("--cause", required=True, help="Cause passed to pssh commands")
    p.add_argument("--log_history_length", default="all", help="How many log files into the past: all or N")
    p.add_argument(
        "--parallelism",
        type=int,
        default=100,
        help="Parallelism for pssh run/scp commands (default: 100)",
    )
    p.add_argument(
        "--output_dir",
        default=None,
        help="Local output directory for copied and merged logs (default: ./tmp near script)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.log_history_length != "all" and not args.log_history_length.isdigit():
        raise SystemExit("--log_history_length must be 'all' or a non-negative integer")
    if args.parallelism <= 0:
        raise SystemExit("--parallelism must be a positive integer")

    base_dir = Path(__file__).resolve().parent
    tmp_dir = Path(args.output_dir).expanduser() if args.output_dir else (base_dir / "tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    remote_cmd = build_remote_command(args.pattern, args.log_history_length)

    print("[1/4] Filter logs on remote nodes")
    run([
        "pssh", "run", "-p", str(args.parallelism),
        remote_cmd,
        args.pssh_nodes_pattern,
        "--cause", args.cause,
    ])

    print("[2/4] Copy filtered logs from remote nodes")
    run([
        "pssh", "scp", "-p", str(args.parallelism),
        f"{args.pssh_nodes_pattern}:{REMOTE_TMP_DIR}/filtered.kikimr*.log",
        str(tmp_dir),
        "--cause", args.cause,
    ])

    print("[3/4] Cleanup remote temporary files")
    run([
        "pssh", "run", "-p", str(args.parallelism),
        f"bash -lc {shlex.quote(f'rm -f {REMOTE_TMP_DIR}/filtered.kikimr*.log')}",
        args.pssh_nodes_pattern,
        "--cause", args.cause,
    ])

    print("[4/4] Merge copied logs")
    files = sorted(tmp_dir.rglob("filtered.kikimr*.log"))
    if not files:
        raise SystemExit(f"No filtered files copied into {tmp_dir}")

    merged = tmp_dir / "filtered.log"
    streams = [f.open("r", encoding="utf-8", errors="replace") for f in files]
    try:
        # heap item: (sort_key, stream_idx, tie_breaker, line)
        heap: list[tuple[tuple[int, int, int, int, int, int, int], int, int, str]] = []
        for stream_idx, inp in enumerate(streams):
            line = inp.readline()
            if line:
                heapq.heappush(heap, (extract_sort_key(line), stream_idx, 0, line))

        with merged.open("w", encoding="utf-8") as out:
            while heap:
                _, stream_idx, tie_breaker, line = heapq.heappop(heap)
                out.write(line)

                next_line = streams[stream_idx].readline()
                if next_line:
                    heapq.heappush(
                        heap,
                        (extract_sort_key(next_line), stream_idx, tie_breaker + 1, next_line),
                    )
    finally:
        for s in streams:
            s.close()

    print(f"Done. Merged file: {merged}")
    print(f"Files merged: {len(files)}")


if __name__ == "__main__":
    main()
