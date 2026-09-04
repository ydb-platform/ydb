#!/usr/bin/env bash
#
# A/B timing of PreferWasm on the Text WASM UDF.
#
#   PRAGMA ydb.EnableWasmUdfResidentStringColumns = "true" | "false"
#
# "true"  — the scan writes `txt` into the query compartment once per row;
#           every Text::* call reuses those bytes.
# "false" — the scan builds a host string and every UDF call copies it into
#           linear memory.
#
# Default: text_1mb × probes (16 O(1) byte_at) and letters (O(n) control).
#   probes  — K distinct Text::byte_at; host copies the blob K times
#   letters — one O(n) call; copy often lost in the scan
#
# Prerequisites: running database, Text WASM UDF uploaded, tables loaded by
# gen_demo_data.py, load SQL emitted by gen_queries.py.
# Optional NATIVE=1: also time TextNative load queries (gen_queries.py
# --module TextNative --suffix _native); no PreferWasm pragma on that path.
#
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
YDBD_ROOT=${YDBD_ROOT:-/home/kulaad/ydbd}
YDB=${YDB:-ydb}
ENDPOINT=${ENDPOINT:-grpc://localhost:2146}
DB=${DB:-/Root/test}
TABLES=${TABLES:-"text_1mb"}
SHAPES=${SHAPES:-"probes letters"}
RUNS=${RUNS:-5}
WARMUP=${WARMUP:-1}
EVIDENCE=${EVIDENCE:-0}
NATIVE=${NATIVE:-0}
NATIVE_SUFFIX=${NATIVE_SUFFIX:-_native}
YDB_LOG=${YDB_LOG:-$YDBD_ROOT/logs/db_start_err.log}

STATS_DIR=$(mktemp -d)
trap 'rm -rf "$STATS_DIR"' EXIT

load_sql() {
    local table=$1 shape=$2 suffix=${3:-}
    printf '%s/demo_%s_%s%s.sql' "$HERE" "$table" "$shape" "$suffix"
}

run_sql_file() {
    local enabled=$1 src=$2 dst=$3
    local stats=${4:-full}
    {
        echo "PRAGMA ydb.EnableWasmUdfResidentStringColumns = \"$enabled\";"
        grep -v 'EnableWasmUdfResidentStringColumns' "$src"
    } >"$STATS_DIR/query.sql"
    local extra=()
    if [[ -n "$stats" && "$stats" != "none" ]]; then
        extra=(--stats "$stats")
    fi
    "$YDB" -e "$ENDPOINT" -d "$DB" sql -f "$STATS_DIR/query.sql" "${extra[@]}" \
        >"$dst" 2>&1
}

run_sql_file_plain() {
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
    local enabled=$1 tag=$2 table=$3 shape=$4
    local src start end cpu cache
    src=$(load_sql "$table" "$shape")
    if [[ ! -f "$src" ]]; then
        echo "missing $src — run gen_queries.py first" >&2
        exit 1
    fi
    start=$(date +%s%N)
    run_sql_file "$enabled" "$src" "$STATS_DIR/$tag.txt" full
    end=$(date +%s%N)
    if grep -qE '^Status:|^Issues:' "$STATS_DIR/$tag.txt"; then
        echo "query failed ($tag $table/$shape):" >&2
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

timed_run_native() {
    local tag=$1 table=$2 shape=$3
    local src start end cpu cache
    src=$(load_sql "$table" "$shape" "$NATIVE_SUFFIX")
    if [[ ! -f "$src" ]]; then
        echo "missing $src — run gen_queries.py --module TextNative --suffix $NATIVE_SUFFIX first" >&2
        exit 1
    fi
    start=$(date +%s%N)
    run_sql_file_plain "$src" "$STATS_DIR/$tag.txt" full
    end=$(date +%s%N)
    if grep -qE '^Status:|^Issues:' "$STATS_DIR/$tag.txt"; then
        echo "query failed ($tag $table/$shape native):" >&2
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

grep_log() {
    local pattern=$1
    if [[ -n "$YDB_LOG" && -f "$YDB_LOG" ]]; then
        grep -c -E "$pattern" "$YDB_LOG" || true
    else
        echo "?"
    fi
}

if [[ "$EVIDENCE" == "1" ]]; then
    EVIDENCE_SQL=${EVIDENCE_SQL:-$HERE/demo_evidence.sql}
    if [[ ! -f "$EVIDENCE_SQL" ]]; then
        echo "missing $EVIDENCE_SQL — run gen_queries.py first" >&2
        exit 1
    fi
    echo "evidence: 1-row count_letters + text_length, PreferWasm on then off"
    echo "start the tenant node with YDB_WASM_STRING_DEBUG=1 (see $YDBD_ROOT/start.sh);"
    echo "stderr is $YDBD_ROOT/logs/db_start_err.log"
    echo
    register_before=$(grep_log '\[WasmString\] Register')
    run_sql_file true "$EVIDENCE_SQL" "$STATS_DIR/evidence-true.txt" none
    echo "----- PreferWasm ON (result) -----"
    cat "$STATS_DIR/evidence-true.txt"
    echo
    register_after_true=$(grep_log '\[WasmString\] Register')
    run_sql_file false "$EVIDENCE_SQL" "$STATS_DIR/evidence-false.txt" none
    echo "----- PreferWasm OFF (result) -----"
    cat "$STATS_DIR/evidence-false.txt"
    echo
    if grep -q 'without a query compartment' "$STATS_DIR"/evidence-*.txt; then
        echo "FAIL: FallbackNoCompartment warning in CLI output — read and UDF split across stages" >&2
        exit 1
    fi
    echo "CLI output has no FallbackNoCompartment warning."
    echo "On the node log, PreferWasm ON should print one [WasmString] Register per row"
    echo "for the txt column (size ≈ row bytes); OFF should print none."
    if [[ -n "$YDB_LOG" ]]; then
        echo "YDB_LOG=$YDB_LOG  Register before=$register_before after_true=$register_after_true after_both=$(grep_log '\[WasmString\] Register')  fallback=$(grep_log 'without a query compartment')"
    fi
    trap - EXIT
    echo "kept stats in $STATS_DIR"
    exit 0
fi

for table in $TABLES; do
    for shape in $SHAPES; do
        src=$(load_sql "$table" "$shape")
        if [[ ! -f "$src" ]]; then
            echo "missing $src — run gen_queries.py first" >&2
            exit 1
        fi
        if [[ "$NATIVE" == "1" ]]; then
            nsrc=$(load_sql "$table" "$shape" "$NATIVE_SUFFIX")
            if [[ ! -f "$nsrc" ]]; then
                echo "missing $nsrc — run gen_queries.py --module TextNative --suffix $NATIVE_SUFFIX first" >&2
                exit 1
            fi
        fi
    done
done

if ((WARMUP < 1)); then
    echo "WARMUP must be >= 1: the first execution instantiates the query compartment" >&2
    exit 1
fi

echo "measuring tables=[$TABLES] shapes=[$SHAPES] runs=$RUNS after $WARMUP warmup (excluded)"
echo "  endpoint=$ENDPOINT db=$DB"
if [[ "$NATIVE" == "1" ]]; then
    echo "  native=TextNative (no PreferWasm pragma)"
fi
echo
if [[ "$NATIVE" == "1" ]]; then
    printf '  %-12s %-8s %-12s %-14s %-12s %-14s %-10s %-12s %-14s %s\n' \
        "table" "shape" "res_ms" "res_cpu_us" "host_ms" "host_cpu_us" "delta_cpu" "nat_ms" "nat_cpu_us" "cache"
else
    printf '  %-12s %-8s %-12s %-14s %-12s %-14s %-10s %s\n' \
        "table" "shape" "res_ms" "res_cpu_us" "host_ms" "host_cpu_us" "delta_cpu" "cache"
fi

for table in $TABLES; do
    for shape in $SHAPES; do
        : >"$STATS_DIR/samples-true"
        : >"$STATS_DIR/samples-false"
        if [[ "$NATIVE" == "1" ]]; then
            : >"$STATS_DIR/samples-native"
        fi
        for ((w = 1; w <= WARMUP; ++w)); do
            timed_run true "warmup-true-$table-$shape-$w" "$table" "$shape" >/dev/null
            timed_run false "warmup-false-$table-$shape-$w" "$table" "$shape" >/dev/null
            if [[ "$NATIVE" == "1" ]]; then
                timed_run_native "warmup-native-$table-$shape-$w" "$table" "$shape" >/dev/null
            fi
        done
        for ((run = 1; run <= RUNS; ++run)); do
            timed_run true "true-$table-$shape-$run" "$table" "$shape" >>"$STATS_DIR/samples-true"
            timed_run false "false-$table-$shape-$run" "$table" "$shape" >>"$STATS_DIR/samples-false"
            if [[ "$NATIVE" == "1" ]]; then
                timed_run_native "native-$table-$shape-$run" "$table" "$shape" >>"$STATS_DIR/samples-native"
            fi
        done
        read -r resident_ms resident_cpu < <(summarize "$STATS_DIR/samples-true")
        read -r host_ms host_cpu < <(summarize "$STATS_DIR/samples-false")
        t_cache=$(cut -d' ' -f3 "$STATS_DIR/samples-true" | sort | uniq -c | sed 's/^ *//' | tr '\n' ',' | sed 's/,$//')
        f_cache=$(cut -d' ' -f3 "$STATS_DIR/samples-false" | sort | uniq -c | sed 's/^ *//' | tr '\n' ',' | sed 's/,$//')
        delta=$(awk -v ac="$resident_cpu" -v bc="$host_cpu" 'BEGIN {
            if (bc > 0) printf "%+.1f%%", 100.0 * (ac - bc) / bc;
            else print "n/a";
        }')
        if [[ "$NATIVE" == "1" ]]; then
            read -r native_ms native_cpu < <(summarize "$STATS_DIR/samples-native")
            n_cache=$(cut -d' ' -f3 "$STATS_DIR/samples-native" | sort | uniq -c | sed 's/^ *//' | tr '\n' ',' | sed 's/,$//')
            printf '  %-12s %-8s %-12s %-14s %-12s %-14s %-10s %-12s %-14s %s/%s/%s\n' \
                "$table" "$shape" "$resident_ms" "$resident_cpu" "$host_ms" "$host_cpu" "$delta" \
                "$native_ms" "$native_cpu" "$t_cache" "$f_cache" "$n_cache"
            echo "$table $shape $resident_ms $resident_cpu $host_ms $host_cpu $delta $native_ms $native_cpu" >>"$STATS_DIR/summary"
        else
            printf '  %-12s %-8s %-12s %-14s %-12s %-14s %-10s %s/%s\n' \
                "$table" "$shape" "$resident_ms" "$resident_cpu" "$host_ms" "$host_cpu" "$delta" "$t_cache" "$f_cache"
            echo "$table $shape $resident_ms $resident_cpu $host_ms $host_cpu $delta" >>"$STATS_DIR/summary"
        fi
        if grep -q ' cold$' "$STATS_DIR/samples-true" "$STATS_DIR/samples-false" \
            ${NATIVE:+ "$STATS_DIR/samples-native"}; then
            echo "    warning: a measured run compiled from scratch (from_cache != true)" >&2
        fi
    done
done

echo
echo "summary (medians of $RUNS runs, warmup=$WARMUP excluded):"
cat "$STATS_DIR/summary"
echo
echo "per-run statistics: $STATS_DIR"
trap - EXIT
