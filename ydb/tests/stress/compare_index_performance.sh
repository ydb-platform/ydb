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
#   ./ydb/tests/stress/compare_performance.sh [OPTIONS]
#
# Options:
#   --duration SECONDS     Duration of each workload run (default: 60)
#   --build-preset PRESET  Build preset for both binaries (default: relwithdebinfo)
#   --main-ydbd PATH       Path to pre-built main branch ydbd (skips downloading)
#   --current-ydbd PATH    Path to pre-built current branch ydbd (skips building)
#   --ref REF              S3 ref to download ydbd from (default: main)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DURATION=60
MAIN_YDBD=""
CURRENT_YDBD=""
S3_REF="main"
BUILD_PRESET="relwithdebinfo"
RESULTS_DIR="$REPO_ROOT/benchmark_results"
S3_BASE_URL="https://storage.yandexcloud.net/ydb-builds"

while [[ $# -gt 0 ]]; do
    case $1 in
        --duration) DURATION="$2"; shift 2 ;;
        --build-preset) BUILD_PRESET="$2"; shift 2 ;;
        --main-ydbd) MAIN_YDBD="$2"; shift 2 ;;
        --current-ydbd) CURRENT_YDBD="$2"; shift 2 ;;
        --ref) S3_REF="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

mkdir -p "$RESULTS_DIR"

CURRENT_BRANCH=$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)
echo "=== Performance comparison: $S3_REF vs $CURRENT_BRANCH ==="
echo "Build preset: $BUILD_PRESET"
echo "Duration per run: ${DURATION}s"
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

# --- Run tests and capture output ---
run_test() {
    local label="$1"
    local ydbd_path="$2"
    local test_path="$3"
    local log_file="$4"

    echo "--- Running $test_path with $label ydbd ---"
    cd "$REPO_ROOT"
    ./ya make --build "$BUILD_PRESET" -tA "$test_path" \
        -DYDB_DRIVER_BINARY_PREBUILT="$ydbd_path" \
        --test-param stress_default_duration="$DURATION" \
        2>&1 | tee "$log_file" | tail -5
    echo ""
}

extract_total_txs_sec() {
    local log_dir="$1"
    # Find the test log and extract the Total Txs/Sec line
    local test_log
    test_log=$(find "$log_dir" -name "*.log" -path "*/testing_out_stuff/*" 2>/dev/null | head -1)
    if [[ -z "$test_log" ]]; then
        echo "N/A"
        return
    fi
    # The Total line format: "10          980 98.0    0       0       5       9       14      18"
    # We want the Txs/Sec column (3rd field in the Total summary line)
    grep -A1 "^Total" "$test_log" | tail -1 | awk '{print $3}'
}

# --- Run vector workload ---
echo ""
echo "=========================================="
echo "  Vector workload"
echo "=========================================="

VECTOR_TEST="ydb/tests/stress/vector_workload/tests"

run_test "$S3_REF" "$MAIN_YDBD" "$VECTOR_TEST" "$RESULTS_DIR/vector_main.log"
VECTOR_MAIN_LOG_DIR="$REPO_ROOT/$VECTOR_TEST/test-results/py3test/testing_out_stuff"
VECTOR_MAIN_TXS=$(extract_total_txs_sec "$VECTOR_MAIN_LOG_DIR")

run_test "current" "$CURRENT_YDBD" "$VECTOR_TEST" "$RESULTS_DIR/vector_current.log"
VECTOR_CURRENT_LOG_DIR="$REPO_ROOT/$VECTOR_TEST/test-results/py3test/testing_out_stuff"
VECTOR_CURRENT_TXS=$(extract_total_txs_sec "$VECTOR_CURRENT_LOG_DIR")

# --- Run fulltext workload ---
echo ""
echo "=========================================="
echo "  Fulltext workload"
echo "=========================================="

FULLTEXT_TEST="ydb/tests/stress/fulltext_workload/tests"

run_test "$S3_REF" "$MAIN_YDBD" "$FULLTEXT_TEST" "$RESULTS_DIR/fulltext_main.log"
FULLTEXT_MAIN_LOG_DIR="$REPO_ROOT/$FULLTEXT_TEST/test-results/py3test/testing_out_stuff"
FULLTEXT_MAIN_TXS=$(extract_total_txs_sec "$FULLTEXT_MAIN_LOG_DIR")

run_test "current" "$CURRENT_YDBD" "$FULLTEXT_TEST" "$RESULTS_DIR/fulltext_current.log"
FULLTEXT_CURRENT_LOG_DIR="$REPO_ROOT/$FULLTEXT_TEST/test-results/py3test/testing_out_stuff"
FULLTEXT_CURRENT_TXS=$(extract_total_txs_sec "$FULLTEXT_CURRENT_LOG_DIR")

# --- Print comparison ---
calc_diff() {
    local main_val="$1"
    local current_val="$2"
    if [[ "$main_val" == "N/A" || "$current_val" == "N/A" || -z "$main_val" || -z "$current_val" ]]; then
        echo "N/A"
        return
    fi
    awk "BEGIN { diff = ($current_val - $main_val) / $main_val * 100; printf \"%+.1f%%\", diff }"
}

VECTOR_DIFF=$(calc_diff "$VECTOR_MAIN_TXS" "$VECTOR_CURRENT_TXS")
FULLTEXT_DIFF=$(calc_diff "$FULLTEXT_MAIN_TXS" "$FULLTEXT_CURRENT_TXS")

echo ""
echo "=========================================="
echo "  Performance Comparison Results"
echo "  Build preset: $BUILD_PRESET"
echo "=========================================="
echo ""
printf "%-20s %15s %15s %10s\n" "Workload" "$S3_REF (Txs/Sec)" "$CURRENT_BRANCH (Txs/Sec)" "Diff"
printf "%-20s %15s %15s %10s\n" "--------------------" "---------------" "---------------" "----------"
printf "%-20s %15s %15s %10s\n" "vector select" "$VECTOR_MAIN_TXS" "$VECTOR_CURRENT_TXS" "$VECTOR_DIFF"
printf "%-20s %15s %15s %10s\n" "fulltext select" "$FULLTEXT_MAIN_TXS" "$FULLTEXT_CURRENT_TXS" "$FULLTEXT_DIFF"
echo ""
echo "Detailed logs: $RESULTS_DIR/"

# --- Write markdown report ---
REPORT_FILE="$RESULTS_DIR/report.md"
cat > "$REPORT_FILE" <<EOF
## Performance Comparison: \`$S3_REF\` vs \`$CURRENT_BRANCH\`

**Build preset:** \`$BUILD_PRESET\` | **Duration:** ${DURATION}s per workload

| Workload | $S3_REF (Txs/Sec) | $CURRENT_BRANCH (Txs/Sec) | Diff |
|---|---|---|---|
| vector select | $VECTOR_MAIN_TXS | $VECTOR_CURRENT_TXS | $VECTOR_DIFF |
| fulltext select | $FULLTEXT_MAIN_TXS | $FULLTEXT_CURRENT_TXS | $FULLTEXT_DIFF |
EOF
echo ""
echo "Markdown report: $REPORT_FILE"
