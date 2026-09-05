#!/usr/bin/env bash
#
# Timing of Trie::LookupPinned (bridge EnsureString pin) on the IP ACL demo.
#
# Each query materializes one dictionary ($dict scalar subquery) and scans every
# address. Guest BridgeRef + BridgeEnsureString pins the blob once per handle.
#
# Optional NATIVE=1 times TrieNative::Lookup files (suffix _native from
# gen_queries.py --module TrieNative --suffix _native).
#
# Prerequisites: running database, Trie WASM UDF uploaded, tables loaded by
# gen_demo_data.py, load SQL emitted by gen_queries.py.
#
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
YDBD_ROOT=${YDBD_ROOT:-/home/kulaad/ydbd}
YDB=${YDB:-ydb}
ENDPOINT=${ENDPOINT:-grpc://localhost:2146}
DB=${DB:-/Root/test}
ADDR_TABLE=${ADDR_TABLE:-ip_addr}
DICT_TABLE=${DICT_TABLE:-ip_dict}
DICT_FROM=${DICT_FROM:-1}
DICT_TO=${DICT_TO:-10}
WARMUP=${WARMUP:-1}
EVIDENCE=${EVIDENCE:-0}
NATIVE=${NATIVE:-0}
SUFFIX=
if [[ "$NATIVE" == "1" ]]; then
    SUFFIX=_native
fi

STATS_DIR=$(mktemp -d)
trap 'rm -rf "$STATS_DIR"' EXIT

load_sql() {
    local dict_id=$1
    printf '%s/demo_load_%02d%s.sql' "$HERE" "$dict_id" "$SUFFIX"
}

run_sql_file() {
    local src=$1 dst=$2
    local stats=${3:-full}
    local extra=()
    if [[ -n "$stats" && "$stats" != "none" ]]; then
        extra=(--stats "$stats")
    fi
    "$YDB" -e "$ENDPOINT" -d "$DB" sql -f "$src" "${extra[@]}" >"$dst" 2>&1
}

median() {
    sort -n | awk '{v[NR]=$1} END {print (NR % 2) ? v[(NR+1)/2] : int((v[NR/2]+v[NR/2+1])/2)}'
}

summarize() {
    local file=$1
    local wall cpu
    wall=$(cut -d' ' -f1 "$file" | median)
    cpu=$(cut -d' ' -f2 "$file" | median)
    echo "$wall $cpu"
}

# "<wall ms> <server cpu us> <cache|cold>"
timed_run() {
    local tag=$1 dict_id=$2
    local src start end cpu cache
    src=$(load_sql "$dict_id")
    if [[ ! -f "$src" ]]; then
        echo "missing $src — run gen_queries.py --dicts $DICT_TO first" >&2
        exit 1
    fi
    start=$(date +%s%N)
    run_sql_file "$src" "$STATS_DIR/$tag.txt" full
    end=$(date +%s%N)
    if grep -qE '^Status:|^Issues:' "$STATS_DIR/$tag.txt"; then
        echo "query failed ($tag dict=$dict_id):" >&2
        cat "$STATS_DIR/$tag.txt" >&2
        exit 1
    fi
    cpu=$(grep -m1 -oE 'total_cpu_time_us: [0-9]+' "$STATS_DIR/$tag.txt" | grep -oE '[0-9]+' \
        || grep -m1 -oE 'cpu_time_us: [0-9]+' "$STATS_DIR/$tag.txt" | grep -oE '[0-9]+' \
        || echo 0)
    if grep -q 'from_cache: true' "$STATS_DIR/$tag.txt"; then
        cache=cache
    else
        cache=cold
    fi
    echo "$(( (end - start) / 1000000 )) ${cpu:-0} $cache"
}

if [[ "$EVIDENCE" == "1" ]]; then
    EVIDENCE_SQL=${EVIDENCE_SQL:-$HERE/demo_evidence.sql}
    if [[ ! -f "$EVIDENCE_SQL" ]]; then
        echo "missing $EVIDENCE_SQL — run gen_queries.py first" >&2
        exit 1
    fi
    echo "evidence: 1-row LookupWithStringPinned (bridge)"
    run_sql_file "$EVIDENCE_SQL" "$STATS_DIR/evidence.txt" none
    cat "$STATS_DIR/evidence.txt"
    if grep -qE '^Status:|^Issues:' "$STATS_DIR/evidence.txt"; then
        exit 1
    fi
    trap - EXIT
    echo "kept stats in $STATS_DIR"
    exit 0
fi

for ((id = DICT_FROM; id <= DICT_TO; ++id)); do
    src=$(load_sql "$id")
    if [[ ! -f "$src" ]]; then
        echo "missing $src — run gen_queries.py --dicts $DICT_TO first" >&2
        exit 1
    fi
done

n_dicts=$((DICT_TO - DICT_FROM + 1))
label="LookupPinned"
[[ "$NATIVE" == "1" ]] && label="TrieNative::Lookup"
echo "measuring $n_dicts queries: dicts $DICT_FROM..$DICT_TO ($label)"
echo "  warmup=$WARMUP (dict $DICT_FROM, excluded — compartment + compile)"
echo "  endpoint=$ENDPOINT db=$DB addr=$ADDR_TABLE dict=$DICT_TABLE"
if ((WARMUP < 1)); then
    echo "WARMUP must be >= 1: the first execution instantiates the query compartment" >&2
    exit 1
fi
for ((w = 1; w <= WARMUP; ++w)); do
    timed_run "warmup-$w" "$DICT_FROM" >>"$STATS_DIR/warmup"
    echo "  warmup $w/$WARMUP done (excluded)"
done

echo
printf '  %-6s %-8s %-12s %-14s %s\n' "dict" "size_mb" "wall_ms" "cpu_us" "cache"
: >"$STATS_DIR/samples"
for ((id = DICT_FROM; id <= DICT_TO; ++id)); do
    line=$(timed_run "run-$id" "$id")
    echo "$line" >>"$STATS_DIR/samples"
    read -r ms cpu cache <<<"$line"
    printf '  %-6s %-8s %-12s %-14s %s\n' "$id" "$id" "$ms" "$cpu" "$cache"
done

if grep -q ' cold$' "$STATS_DIR/samples"; then
    echo "warning: a measured run compiled from scratch (from_cache != true); median still includes it" >&2
fi

read -r med_ms med_cpu < <(summarize "$STATS_DIR/samples")

echo
echo "addr=$ADDR_TABLE dict=$DICT_TABLE dicts=$DICT_FROM..$DICT_TO warmup=$WARMUP (excluded) ($label)"
printf '  median: %6s ms wall, %8s us cpu\n' "$med_ms" "$med_cpu"
echo
echo "per-query statistics: $STATS_DIR/run-$DICT_FROM.txt .."
trap - EXIT
