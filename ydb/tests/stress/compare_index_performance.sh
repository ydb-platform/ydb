#!/bin/bash
#
# Compare vector/fulltext workload performance between main branch and current branch.
#
# The main branch ydbd is downloaded as a prebuilt binary from S3 (ydb-builds bucket).
# The current branch ydbd is built from the local source tree.
#
# IMPORTANT: Both binaries must use the same build preset for a fair comparison.
# The S3 bucket has builds for: release, relwithdebinfo.
#
# Usage:
#   ./ydb/tests/stress/compare_index_performance.sh [OPTIONS]
#
# Options:
#   --duration SECONDS     Duration of each workload run (default: 60)
#   --build-preset PRESET  Build preset for both binaries (default: relwithdebinfo)
#   --main-ydbd PATH       Path to pre-built main branch ydbd (skips downloading)
#   --current-ydbd PATH    Path to pre-built current branch ydbd (skips building)
#   --ref REF              S3 ref to download ydbd from (default: main)
#   --workload WORKLOAD    Run only specified workload: vector, fulltext, or all (default: all)
#   --targets N            Number of query vectors for vector select (default: 100)
#   --iterations N         Number of iterations per workload (default: 3)
#   --warmup SECONDS       Warmup duration before each measured run (default: 30)
#   --main-feature-flag FLAG     Enable a feature flag for main branch (repeatable)
#   --current-feature-flag FLAG  Enable a feature flag for current branch (repeatable)
#   --main-table-service-config KEY=VALUE     Set table_service_config option for main branch (repeatable)
#   --current-table-service-config KEY=VALUE  Set table_service_config option for current branch (repeatable)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DURATION=60
MAIN_YDBD=""
CURRENT_YDBD=""
S3_REF="main"
BUILD_PRESET="relwithdebinfo"
WORKLOAD="all"
TARGETS=100
ITERATIONS=3
WARMUP=30
MAIN_FEATURE_FLAGS=()
CURRENT_FEATURE_FLAGS=()
MAIN_TABLE_SERVICE_CONFIG=()
CURRENT_TABLE_SERVICE_CONFIG=()
RESULTS_DIR="$REPO_ROOT/benchmark_results"
S3_BASE_URL="https://storage.yandexcloud.net/ydb-builds"

while [[ $# -gt 0 ]]; do
    case $1 in
        --duration) DURATION="$2"; shift 2 ;;
        --build-preset) BUILD_PRESET="$2"; shift 2 ;;
        --main-ydbd) MAIN_YDBD="$(realpath "$2")"; shift 2 ;;
        --current-ydbd) CURRENT_YDBD="$(realpath "$2")"; shift 2 ;;
        --ref) S3_REF="$2"; shift 2 ;;
        --workload) WORKLOAD="$2"; shift 2 ;;
        --targets) TARGETS="$2"; shift 2 ;;
        --iterations) ITERATIONS="$2"; shift 2 ;;
        --warmup) WARMUP="$2"; shift 2 ;;
        --main-feature-flag) MAIN_FEATURE_FLAGS+=("$2"); shift 2 ;;
        --current-feature-flag) CURRENT_FEATURE_FLAGS+=("$2"); shift 2 ;;
        --main-table-service-config) MAIN_TABLE_SERVICE_CONFIG+=("$2"); shift 2 ;;
        --current-table-service-config) CURRENT_TABLE_SERVICE_CONFIG+=("$2"); shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [[ "$WORKLOAD" != "all" && "$WORKLOAD" != "vector" && "$WORKLOAD" != "fulltext" ]]; then
    echo "ERROR: --workload must be one of: all, vector, fulltext"
    exit 1
fi

mkdir -p "$RESULTS_DIR"

# Build feature flags test params (comma-separated)
MAIN_FEATURE_FLAGS_PARAM=""
if [[ ${#MAIN_FEATURE_FLAGS[@]} -gt 0 ]]; then
    MAIN_FEATURE_FLAGS_PARAM=$(IFS=,; echo "${MAIN_FEATURE_FLAGS[*]}")
fi
CURRENT_FEATURE_FLAGS_PARAM=""
if [[ ${#CURRENT_FEATURE_FLAGS[@]} -gt 0 ]]; then
    CURRENT_FEATURE_FLAGS_PARAM=$(IFS=,; echo "${CURRENT_FEATURE_FLAGS[*]}")
fi
# Build table_service_config test params (comma-separated key=value pairs)
MAIN_TABLE_SERVICE_CONFIG_PARAM=""
if [[ ${#MAIN_TABLE_SERVICE_CONFIG[@]} -gt 0 ]]; then
    MAIN_TABLE_SERVICE_CONFIG_PARAM=$(IFS=,; echo "${MAIN_TABLE_SERVICE_CONFIG[*]}")
fi
CURRENT_TABLE_SERVICE_CONFIG_PARAM=""
if [[ ${#CURRENT_TABLE_SERVICE_CONFIG[@]} -gt 0 ]]; then
    CURRENT_TABLE_SERVICE_CONFIG_PARAM=$(IFS=,; echo "${CURRENT_TABLE_SERVICE_CONFIG[*]}")
fi

CURRENT_BRANCH=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)
echo "=== Performance comparison: $S3_REF vs $CURRENT_BRANCH ==="
echo "Build preset: $BUILD_PRESET"
echo "Duration per run: ${DURATION}s"
echo "Workload: $WORKLOAD"
echo "Vector targets: $TARGETS"
echo "Iterations: $ITERATIONS"
echo "Warmup: ${WARMUP}s"
if [[ ${#MAIN_FEATURE_FLAGS[@]} -gt 0 ]]; then
    echo "Main feature flags: ${MAIN_FEATURE_FLAGS[*]}"
fi
if [[ ${#CURRENT_FEATURE_FLAGS[@]} -gt 0 ]]; then
    echo "Current feature flags: ${CURRENT_FEATURE_FLAGS[*]}"
fi
if [[ ${#MAIN_TABLE_SERVICE_CONFIG[@]} -gt 0 ]]; then
    echo "Main table_service_config: ${MAIN_TABLE_SERVICE_CONFIG[*]}"
fi
if [[ ${#CURRENT_TABLE_SERVICE_CONFIG[@]} -gt 0 ]]; then
    echo "Current table_service_config: ${CURRENT_TABLE_SERVICE_CONFIG[*]}"
fi
echo ""

# --- Download ydbd from S3 if not provided ---
if [[ -z "$MAIN_YDBD" ]]; then
    MAIN_YDBD="$RESULTS_DIR/ydbd-${S3_REF}-${BUILD_PRESET}"
    if [[ -x "$MAIN_YDBD" ]]; then
        echo "Using cached $S3_REF ydbd: $MAIN_YDBD"
    else
        echo "=== Downloading ydbd from S3 ($S3_REF/$BUILD_PRESET) ==="
        S3_URL="$S3_BASE_URL/$S3_REF/$BUILD_PRESET/ydbd"
        echo "URL: $S3_URL"
        wget -q --show-progress -O "$MAIN_YDBD" "$S3_URL"
        chmod +x "$MAIN_YDBD"
        echo "Downloaded: $MAIN_YDBD"
    fi
else
    echo "Using pre-built $S3_REF ydbd: $MAIN_YDBD"
fi

# --- Build ydbd from current branch if not provided ---
if [[ -z "$CURRENT_YDBD" ]]; then
    echo "=== Building ydbd from current branch ($CURRENT_BRANCH, preset: $BUILD_PRESET) ==="
    cd "$REPO_ROOT"
    ./ya make --build "$BUILD_PRESET" ydb/apps/ydbd ydb/apps/ydb 2>&1 | tail -5
    CURRENT_YDBD="$REPO_ROOT/ydb/apps/ydbd/ydbd"
    echo "Current ydbd built: $CURRENT_YDBD"
else
    echo "Using pre-built current ydbd: $CURRENT_YDBD"
fi

# --- Verify binaries exist ---
for binary in "$MAIN_YDBD" "$CURRENT_YDBD"; do
    if [[ ! -x "$binary" ]]; then
        echo "ERROR: Binary not found or not executable: $binary"
        exit 1
    fi
done

# --- Helper functions ---
run_test() {
    local label="$1"
    local ydbd_path="$2"
    local test_path="$3"
    local log_file="$4"
    shift 4
    local extra_params=("$@")

    echo "--- Running $test_path with $label ydbd ---"
    cd "$REPO_ROOT"
    ./ya make --build "$BUILD_PRESET" -tA "$test_path" \
        -DYDB_DRIVER_BINARY_PREBUILT="$ydbd_path" \
        --test-param stress_default_duration="$DURATION" \
        "${extra_params[@]}" \
        2>&1 | tee "$log_file" | tail -5 || true
    echo ""
}

extract_total_txs_sec() {
    local log_dir="$1"
    local test_log
    test_log=$(find "$log_dir" -name "*.log" -path "*/testing_out_stuff/*" 2>/dev/null | head -1)
    if [[ -z "$test_log" ]]; then
        echo "N/A"
        return
    fi
    grep -A1 "^Total" "$test_log" | tail -1 | awk '{print $3}'
}

median() {
    # Read values from arguments, sort numerically, return median
    local -a vals=("$@")
    local n=${#vals[@]}
    if [[ $n -eq 0 ]]; then
        echo "N/A"
        return
    fi
    local sorted
    sorted=($(printf '%s\n' "${vals[@]}" | sort -n))
    local mid=$((n / 2))
    if (( n % 2 == 1 )); then
        echo "${sorted[$mid]}"
    else
        awk "BEGIN { printf \"%.3f\", (${sorted[$mid-1]} + ${sorted[$mid]}) / 2 }"
    fi
}

mean() {
    local -a vals=("$@")
    local n=${#vals[@]}
    if [[ $n -eq 0 ]]; then
        echo "N/A"
        return
    fi
    awk "BEGIN { s=0 } { s+=\$1 } END { printf \"%.3f\", s/NR }" < <(printf '%s\n' "${vals[@]}")
}

stddev() {
    # Sample standard deviation
    local -a vals=("$@")
    local n=${#vals[@]}
    if [[ $n -lt 2 ]]; then
        echo "0"
        return
    fi
    awk "BEGIN { s=0; ss=0; n=0 } { s+=\$1; ss+=\$1*\$1; n++ } END { m=s/n; printf \"%.3f\", sqrt((ss - n*m*m)/(n-1)) }" < <(printf '%s\n' "${vals[@]}")
}

calc_diff() {
    local main_val="$1"
    local current_val="$2"
    if [[ "$main_val" == "N/A" || "$current_val" == "N/A" || -z "$main_val" || -z "$current_val" ]]; then
        echo "N/A"
        return
    fi
    awk "BEGIN { diff = ($current_val - $main_val) / $main_val * 100; printf \"%+.1f%%\", diff }"
}

calc_significance() {
    # Check statistical significance using 3-sigma rule on the difference of means.
    # Uses pooled standard error: SE = sqrt(s1^2/n1 + s2^2/n2)
    # Returns "SIGNIFICANT (<diff> > 3σ=<threshold>)" or "not significant (<diff> <= 3σ=<threshold>)"
    local main_mean="$1"
    local current_mean="$2"
    local main_stddev="$3"
    local current_stddev="$4"
    local main_n="$5"
    local current_n="$6"

    if [[ "$main_mean" == "N/A" || "$current_mean" == "N/A" ]]; then
        echo "N/A"
        return
    fi
    if [[ $main_n -lt 2 || $current_n -lt 2 ]]; then
        echo "N/A (need >= 2 iterations)"
        return
    fi

    awk "BEGIN {
        se = sqrt(($main_stddev)^2/$main_n + ($current_stddev)^2/$current_n)
        diff = $current_mean - $main_mean
        absdiff = (diff < 0) ? -diff : diff
        threshold = 3 * se
        pct = diff / $main_mean * 100
        if (threshold > 0 && absdiff > threshold) {
            printf \"SIGNIFICANT (%+.1f%%, |diff|=%.1f > 3sigma=%.1f)\", pct, absdiff, threshold
        } else if (threshold > 0) {
            printf \"not significant (%+.1f%%, |diff|=%.1f <= 3sigma=%.1f)\", pct, absdiff, threshold
        } else {
            printf \"N/A (zero variance)\"
        }
    }"
}

format_values() {
    # Format array of values as "median [v1, v2, ...]"
    local median_val="$1"
    shift
    local -a vals=("$@")
    if [[ ${#vals[@]} -le 1 ]]; then
        echo "$median_val"
        return
    fi
    local joined
    joined=$(printf ", %s" "${vals[@]}")
    joined="${joined:2}"  # remove leading ", "
    echo "$median_val [$joined]"
}

VECTOR_MAIN_TXS="N/A"
VECTOR_CURRENT_TXS="N/A"
FULLTEXT_MAIN_TXS="N/A"
FULLTEXT_CURRENT_TXS="N/A"

# --- Run vector workload ---
if [[ "$WORKLOAD" == "all" || "$WORKLOAD" == "vector" ]]; then
    echo ""
    echo "=========================================="
    echo "  Vector workload ($ITERATIONS iterations, interleaved)"
    echo "=========================================="

    VECTOR_TEST="ydb/tests/stress/vector_workload/tests"
    VECTOR_DATA_DIR="$RESULTS_DIR/vector_data"

    VECTOR_MAIN_VALUES=()
    VECTOR_CURRENT_VALUES=()

    # First iteration: generate mode creates the query table
    # Subsequent iterations: load mode reuses the query table
    for i in $(seq 1 "$ITERATIONS"); do
        echo ""
        echo "=== Vector iteration $i/$ITERATIONS ==="

        # Run main
        local_mode="load"
        if [[ $i -eq 1 ]]; then
            local_mode="generate"
        fi
        VECTOR_EXTRA_PARAMS=(
            --test-param vector_mode="$local_mode"
            --test-param vector_data_dir="$VECTOR_DATA_DIR"
            --test-param vector_targets="$TARGETS"
            --test-param vector_warmup="$WARMUP"
        )
        if [[ -n "$MAIN_FEATURE_FLAGS_PARAM" ]]; then
            VECTOR_EXTRA_PARAMS+=(--test-param feature_flags="$MAIN_FEATURE_FLAGS_PARAM")
        fi
        if [[ -n "$MAIN_TABLE_SERVICE_CONFIG_PARAM" ]]; then
            VECTOR_EXTRA_PARAMS+=(--test-param table_service_config="$MAIN_TABLE_SERVICE_CONFIG_PARAM")
        fi
        run_test "$S3_REF" "$MAIN_YDBD" "$VECTOR_TEST" "$RESULTS_DIR/vector_main_${i}.log" \
            "${VECTOR_EXTRA_PARAMS[@]}"
        VECTOR_LOG_DIR="$REPO_ROOT/$VECTOR_TEST/test-results/py3test/testing_out_stuff"
        val=$(extract_total_txs_sec "$VECTOR_LOG_DIR")
        if [[ "$val" != "N/A" && -n "$val" ]]; then
            VECTOR_MAIN_VALUES+=("$val")
        fi
        echo "  $S3_REF iteration $i: $val Txs/Sec"

        # Run current
        VECTOR_EXTRA_PARAMS=(
            --test-param vector_mode=load
            --test-param vector_data_dir="$VECTOR_DATA_DIR"
            --test-param vector_targets="$TARGETS"
            --test-param vector_warmup="$WARMUP"
        )
        if [[ -n "$CURRENT_FEATURE_FLAGS_PARAM" ]]; then
            VECTOR_EXTRA_PARAMS+=(--test-param feature_flags="$CURRENT_FEATURE_FLAGS_PARAM")
        fi
        if [[ -n "$CURRENT_TABLE_SERVICE_CONFIG_PARAM" ]]; then
            VECTOR_EXTRA_PARAMS+=(--test-param table_service_config="$CURRENT_TABLE_SERVICE_CONFIG_PARAM")
        fi
        run_test "current" "$CURRENT_YDBD" "$VECTOR_TEST" "$RESULTS_DIR/vector_current_${i}.log" \
            "${VECTOR_EXTRA_PARAMS[@]}"
        VECTOR_LOG_DIR="$REPO_ROOT/$VECTOR_TEST/test-results/py3test/testing_out_stuff"
        val=$(extract_total_txs_sec "$VECTOR_LOG_DIR")
        if [[ "$val" != "N/A" && -n "$val" ]]; then
            VECTOR_CURRENT_VALUES+=("$val")
        fi
        echo "  current iteration $i: $val Txs/Sec"
    done

    if [[ ${#VECTOR_MAIN_VALUES[@]} -gt 0 ]]; then
        VECTOR_MAIN_TXS=$(median "${VECTOR_MAIN_VALUES[@]}")
        VECTOR_MAIN_MEAN=$(mean "${VECTOR_MAIN_VALUES[@]}")
        VECTOR_MAIN_STDDEV=$(stddev "${VECTOR_MAIN_VALUES[@]}")
    fi
    if [[ ${#VECTOR_CURRENT_VALUES[@]} -gt 0 ]]; then
        VECTOR_CURRENT_TXS=$(median "${VECTOR_CURRENT_VALUES[@]}")
        VECTOR_CURRENT_MEAN=$(mean "${VECTOR_CURRENT_VALUES[@]}")
        VECTOR_CURRENT_STDDEV=$(stddev "${VECTOR_CURRENT_VALUES[@]}")
    fi
    VECTOR_MAIN_DETAIL=$(format_values "$VECTOR_MAIN_TXS" "${VECTOR_MAIN_VALUES[@]}")
    VECTOR_CURRENT_DETAIL=$(format_values "$VECTOR_CURRENT_TXS" "${VECTOR_CURRENT_VALUES[@]}")
    VECTOR_SIGNIFICANCE=$(calc_significance \
        "${VECTOR_MAIN_MEAN:-N/A}" "${VECTOR_CURRENT_MEAN:-N/A}" \
        "${VECTOR_MAIN_STDDEV:-0}" "${VECTOR_CURRENT_STDDEV:-0}" \
        "${#VECTOR_MAIN_VALUES[@]}" "${#VECTOR_CURRENT_VALUES[@]}")
fi

# --- Run fulltext workload ---
if [[ "$WORKLOAD" == "all" || "$WORKLOAD" == "fulltext" ]]; then
    echo ""
    echo "=========================================="
    echo "  Fulltext workload ($ITERATIONS iterations, interleaved)"
    echo "=========================================="

    FULLTEXT_TEST="ydb/tests/stress/fulltext_workload/tests"

    FULLTEXT_MAIN_VALUES=()
    FULLTEXT_CURRENT_VALUES=()

    for i in $(seq 1 "$ITERATIONS"); do
        echo ""
        echo "=== Fulltext iteration $i/$ITERATIONS ==="

        # Run main
        FULLTEXT_EXTRA_PARAMS=()
        if [[ -n "$MAIN_FEATURE_FLAGS_PARAM" ]]; then
            FULLTEXT_EXTRA_PARAMS+=(--test-param feature_flags="$MAIN_FEATURE_FLAGS_PARAM")
        fi
        if [[ -n "$MAIN_TABLE_SERVICE_CONFIG_PARAM" ]]; then
            FULLTEXT_EXTRA_PARAMS+=(--test-param table_service_config="$MAIN_TABLE_SERVICE_CONFIG_PARAM")
        fi
        run_test "$S3_REF" "$MAIN_YDBD" "$FULLTEXT_TEST" "$RESULTS_DIR/fulltext_main_${i}.log" \
            "${FULLTEXT_EXTRA_PARAMS[@]}"
        FULLTEXT_LOG_DIR="$REPO_ROOT/$FULLTEXT_TEST/test-results/py3test/testing_out_stuff"
        val=$(extract_total_txs_sec "$FULLTEXT_LOG_DIR")
        if [[ "$val" != "N/A" && -n "$val" ]]; then
            FULLTEXT_MAIN_VALUES+=("$val")
        fi
        echo "  $S3_REF iteration $i: $val Txs/Sec"

        # Run current
        FULLTEXT_EXTRA_PARAMS=()
        if [[ -n "$CURRENT_FEATURE_FLAGS_PARAM" ]]; then
            FULLTEXT_EXTRA_PARAMS+=(--test-param feature_flags="$CURRENT_FEATURE_FLAGS_PARAM")
        fi
        if [[ -n "$CURRENT_TABLE_SERVICE_CONFIG_PARAM" ]]; then
            FULLTEXT_EXTRA_PARAMS+=(--test-param table_service_config="$CURRENT_TABLE_SERVICE_CONFIG_PARAM")
        fi
        run_test "current" "$CURRENT_YDBD" "$FULLTEXT_TEST" "$RESULTS_DIR/fulltext_current_${i}.log" \
            "${FULLTEXT_EXTRA_PARAMS[@]}"
        FULLTEXT_LOG_DIR="$REPO_ROOT/$FULLTEXT_TEST/test-results/py3test/testing_out_stuff"
        val=$(extract_total_txs_sec "$FULLTEXT_LOG_DIR")
        if [[ "$val" != "N/A" && -n "$val" ]]; then
            FULLTEXT_CURRENT_VALUES+=("$val")
        fi
        echo "  current iteration $i: $val Txs/Sec"
    done

    if [[ ${#FULLTEXT_MAIN_VALUES[@]} -gt 0 ]]; then
        FULLTEXT_MAIN_TXS=$(median "${FULLTEXT_MAIN_VALUES[@]}")
        FULLTEXT_MAIN_MEAN=$(mean "${FULLTEXT_MAIN_VALUES[@]}")
        FULLTEXT_MAIN_STDDEV=$(stddev "${FULLTEXT_MAIN_VALUES[@]}")
    fi
    if [[ ${#FULLTEXT_CURRENT_VALUES[@]} -gt 0 ]]; then
        FULLTEXT_CURRENT_TXS=$(median "${FULLTEXT_CURRENT_VALUES[@]}")
        FULLTEXT_CURRENT_MEAN=$(mean "${FULLTEXT_CURRENT_VALUES[@]}")
        FULLTEXT_CURRENT_STDDEV=$(stddev "${FULLTEXT_CURRENT_VALUES[@]}")
    fi
    FULLTEXT_MAIN_DETAIL=$(format_values "$FULLTEXT_MAIN_TXS" "${FULLTEXT_MAIN_VALUES[@]}")
    FULLTEXT_CURRENT_DETAIL=$(format_values "$FULLTEXT_CURRENT_TXS" "${FULLTEXT_CURRENT_VALUES[@]}")
    FULLTEXT_SIGNIFICANCE=$(calc_significance \
        "${FULLTEXT_MAIN_MEAN:-N/A}" "${FULLTEXT_CURRENT_MEAN:-N/A}" \
        "${FULLTEXT_MAIN_STDDEV:-0}" "${FULLTEXT_CURRENT_STDDEV:-0}" \
        "${#FULLTEXT_MAIN_VALUES[@]}" "${#FULLTEXT_CURRENT_VALUES[@]}")
fi

# --- Print comparison ---
VECTOR_DIFF=$(calc_diff "$VECTOR_MAIN_TXS" "$VECTOR_CURRENT_TXS")
FULLTEXT_DIFF=$(calc_diff "$FULLTEXT_MAIN_TXS" "$FULLTEXT_CURRENT_TXS")

echo ""
echo "=========================================="
echo "  Performance Comparison Results"
echo "  Build preset: $BUILD_PRESET"
echo "  Iterations: $ITERATIONS (median reported)"
echo "=========================================="
echo ""
printf "%-20s %15s %15s %10s\n" "Workload" "$S3_REF (Txs/Sec)" "$CURRENT_BRANCH (Txs/Sec)" "Diff"
printf "%-20s %15s %15s %10s\n" "--------------------" "---------------" "---------------" "----------"
if [[ "$WORKLOAD" == "all" || "$WORKLOAD" == "vector" ]]; then
    printf "%-20s %15s %15s %10s\n" "vector select" "$VECTOR_MAIN_TXS" "$VECTOR_CURRENT_TXS" "$VECTOR_DIFF"
fi
if [[ "$WORKLOAD" == "all" || "$WORKLOAD" == "fulltext" ]]; then
    printf "%-20s %15s %15s %10s\n" "fulltext select" "$FULLTEXT_MAIN_TXS" "$FULLTEXT_CURRENT_TXS" "$FULLTEXT_DIFF"
fi
echo ""
if [[ "$WORKLOAD" == "all" || "$WORKLOAD" == "vector" ]]; then
    echo "Vector details:"
    echo "  $S3_REF:   ${VECTOR_MAIN_DETAIL:-N/A}  (mean=${VECTOR_MAIN_MEAN:-N/A}, σ=${VECTOR_MAIN_STDDEV:-N/A})"
    echo "  current: ${VECTOR_CURRENT_DETAIL:-N/A}  (mean=${VECTOR_CURRENT_MEAN:-N/A}, σ=${VECTOR_CURRENT_STDDEV:-N/A})"
    echo "  3σ test: ${VECTOR_SIGNIFICANCE:-N/A}"
fi
if [[ "$WORKLOAD" == "all" || "$WORKLOAD" == "fulltext" ]]; then
    echo "Fulltext details:"
    echo "  $S3_REF:   ${FULLTEXT_MAIN_DETAIL:-N/A}  (mean=${FULLTEXT_MAIN_MEAN:-N/A}, σ=${FULLTEXT_MAIN_STDDEV:-N/A})"
    echo "  current: ${FULLTEXT_CURRENT_DETAIL:-N/A}  (mean=${FULLTEXT_CURRENT_MEAN:-N/A}, σ=${FULLTEXT_CURRENT_STDDEV:-N/A})"
    echo "  3σ test: ${FULLTEXT_SIGNIFICANCE:-N/A}"
fi
echo ""
echo "Detailed logs: $RESULTS_DIR/"

# --- Write markdown report ---
REPORT_FILE="$RESULTS_DIR/report.md"
{
    echo "## Performance Comparison: \`$S3_REF\` vs \`$CURRENT_BRANCH\`"
    echo ""
    echo "**Build preset:** \`$BUILD_PRESET\` | **Duration:** ${DURATION}s per workload | **Iterations:** $ITERATIONS (median reported)"
    echo ""
    echo "| Workload | $S3_REF (Txs/Sec) | $CURRENT_BRANCH (Txs/Sec) | Diff | 3σ significance |"
    echo "|---|---|---|---|---|"
    if [[ "$WORKLOAD" == "all" || "$WORKLOAD" == "vector" ]]; then
        echo "| vector select | ${VECTOR_MAIN_DETAIL:-N/A} | ${VECTOR_CURRENT_DETAIL:-N/A} | $VECTOR_DIFF | ${VECTOR_SIGNIFICANCE:-N/A} |"
    fi
    if [[ "$WORKLOAD" == "all" || "$WORKLOAD" == "fulltext" ]]; then
        echo "| fulltext select | ${FULLTEXT_MAIN_DETAIL:-N/A} | ${FULLTEXT_CURRENT_DETAIL:-N/A} | $FULLTEXT_DIFF | ${FULLTEXT_SIGNIFICANCE:-N/A} |"
    fi
} > "$REPORT_FILE"
echo ""
echo "Markdown report: $REPORT_FILE"
