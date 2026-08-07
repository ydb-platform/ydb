#!/bin/bash
# Collect S3 router logs (BDRT markers) from a list of cluster hosts.
# Usage:
#   ./collect_router_logs.sh [--logs-dir DIR] [--out DIR] [--marker PATTERN] host1 host2 ...
#
# Example:
#   ./collect_router_logs.sh --logs-dir /Berkanavt/kikimr_31003/logs \
#       sas2-9951.search.yandex.net sas2-9743.search.yandex.net ...

set -euo pipefail

LOGS_DIR="/Berkanavt/kikimr_31003/logs"
OUT_DIR="./s3_router_logs"
MARKER="BDRT"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --logs-dir) LOGS_DIR="$2"; shift 2 ;;
        --out)      OUT_DIR="$2";  shift 2 ;;
        --marker)   MARKER="$2";   shift 2 ;;
        --help|-h)
            sed -n '2,7p' "$0"
            exit 0
            ;;
        -*) echo "Unknown flag: $1" >&2; exit 1 ;;
        *)  break ;;
    esac
done

HOSTS=("$@")
if [[ ${#HOSTS[@]} -eq 0 ]]; then
    echo "No hosts specified. Pass them as positional arguments." >&2
    exit 1
fi

mkdir -p "$OUT_DIR"

collect_one() {
    local host="$1"
    local short
    short=$(echo "$host" | cut -d. -f1)
    local outfile="$OUT_DIR/${short}.log"

    echo "[$short] collecting from $host ..."

    # Search the current (uncompressed) log and all rotated .gz logs.
    # grep -a treats binary as text (safe for mixed logs).
    ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$host" bash -s -- "$LOGS_DIR" "$MARKER" <<'REMOTE_SCRIPT' > "$outfile" 2>&1
LOGS_DIR="$1"
MARKER="$2"
# current log
if [[ -f "$LOGS_DIR/kikimr.start" ]]; then
    grep -a "$MARKER" "$LOGS_DIR/kikimr.start" 2>/dev/null || true
fi
# rotated logs (most recent first)
for f in $(ls -t "$LOGS_DIR"/kikimr.start.*.gz 2>/dev/null); do
    zgrep -a "$MARKER" "$f" 2>/dev/null || true
done
REMOTE_SCRIPT

    local lines
    lines=$(wc -l < "$outfile")
    echo "[$short] done — $lines matching lines -> $outfile"
}

for host in "${HOSTS[@]}"; do
    collect_one "$host" &
done

wait
echo ""
echo "All done.  Results in $OUT_DIR/"
echo "Quick summary:"
wc -l "$OUT_DIR"/*.log | sort -rn | head -20
