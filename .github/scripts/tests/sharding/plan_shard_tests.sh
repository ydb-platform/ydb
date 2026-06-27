#!/usr/bin/env bash
# Build shard plan from ya test -L output and optional increment graph filter.
set -euo pipefail

TEST_LIST_LOG="${1:?usage: plan_shard_tests.sh TEST_LIST_LOG TARGET_PREFIX SHARD_COUNT OUTPUT_DIR [INCREMENT_GRAPH]}"
TARGET_PREFIX="${2:?usage: plan_shard_tests.sh TEST_LIST_LOG TARGET_PREFIX SHARD_COUNT OUTPUT_DIR [INCREMENT_GRAPH]}"
SHARD_COUNT="${3:?usage: plan_shard_tests.sh TEST_LIST_LOG TARGET_PREFIX SHARD_COUNT OUTPUT_DIR [INCREMENT_GRAPH]}"
OUTPUT_DIR="${4:?usage: plan_shard_tests.sh TEST_LIST_LOG TARGET_PREFIX SHARD_COUNT OUTPUT_DIR [INCREMENT_GRAPH]}"
INCREMENT_GRAPH="${5:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FULL_SUMMARY="/tmp/list_summary_full.json"
FILTERED_SUMMARY="/tmp/list_summary.json"
TESTS_FILE="/tmp/tests.txt"
PLAN_FILE="/tmp/shard_plan.json"

python3 "$SCRIPT_DIR/extract_suites_from_ya_test_list.py" "$TEST_LIST_LOG" \
  --target-prefix "$TARGET_PREFIX" \
  -o /tmp/tests_full.txt \
  --summary-json "$FULL_SUMMARY"

if [ -n "$INCREMENT_GRAPH" ] && [ -f "$INCREMENT_GRAPH" ]; then
  python3 "$SCRIPT_DIR/filter_suites_by_increment_graph.py" \
    "$FULL_SUMMARY" "$INCREMENT_GRAPH" \
    --target-prefix "$TARGET_PREFIX" \
    -o "$TESTS_FILE" \
    --summary-json "$FILTERED_SUMMARY"
else
  cp /tmp/tests_full.txt "$TESTS_FILE"
  cp "$FULL_SUMMARY" "$FILTERED_SUMMARY"
fi

echo "Planned scope: $(jq '.total_suites' "$FILTERED_SUMMARY") suites, $(jq '.total_tests' "$FILTERED_SUMMARY") tests, weight $(jq '.total_weight' "$FILTERED_SUMMARY")"

python3 "$SCRIPT_DIR/apply_history_suite_weights.py" \
  "$TEST_LIST_LOG" \
  "$FILTERED_SUMMARY" \
  --target-prefix "$TARGET_PREFIX" \
  --build-type "${BUILD_PRESET:-relwithdebinfo}" \
  --branch "${HISTORY_BRANCH:-main}" \
  --days-back "${HISTORY_DAYS_BACK:-3}"

echo "Weighting: $(jq -c '.weighting' "$FILTERED_SUMMARY")"

if [ "$SHARD_COUNT" = "auto" ]; then
  CHOOSE_ARGS=(--threads "${TEST_THREADS:-52}")
  if [ "${DISABLE_PEAK_CAP:-}" = "1" ]; then
    CHOOSE_ARGS+=(--no-peak-cap)
  fi
  # Real pool-capacity cap: count active jobs per runner label, convert to
  # cloud-quota usage (runner_capacity.yml) and cap shards by what still
  # fits. When available it supersedes the static peak-hour heuristic.
  CAPACITY_CONFIG="${CAPACITY_CONFIG:-$SCRIPT_DIR/../../../config/runner_capacity.yml}"
  if [ -z "${MAX_SHARDS:-}" ] && [ -f "$CAPACITY_CONFIG" ] && [ -n "${GITHUB_TOKEN:-}${GH_TOKEN:-}" ]; then
    capacity_cap=$(python3 "$SCRIPT_DIR/estimate_runner_capacity.py" \
      --config "$CAPACITY_CONFIG" \
      --preset-label "build-preset-${BUILD_PRESET:-relwithdebinfo}" || true)
    if [ -n "$capacity_cap" ]; then
      MAX_SHARDS="$capacity_cap"
      CHOOSE_ARGS+=(--no-peak-cap)
      echo "Pool capacity cap: $MAX_SHARDS runners fit into the quota budget"
    fi
  fi
  if [ -n "${MAX_SHARDS:-}" ]; then
    CHOOSE_ARGS+=(--max-shards "$MAX_SHARDS")
  fi
  SHARD_COUNT=$(python3 "$SCRIPT_DIR/choose_shard_count.py" "$FILTERED_SUMMARY" "${CHOOSE_ARGS[@]}")
  echo "Adaptive shard count: $SHARD_COUNT"
fi

if [ "$(jq '.total_suites' "$FILTERED_SUMMARY")" = "0" ]; then
  jq -n \
    --argjson shard_count "$SHARD_COUNT" \
    '{
      requested_shard_count: $shard_count,
      shard_count: 0,
      total_suites: 0,
      total_tests: 0,
      total_weight: 0,
      shards: []
    }' >"$PLAN_FILE"
else
  python3 "$SCRIPT_DIR/split_test_shards.py" \
    --tests-file "$TESTS_FILE" \
    --suite-weights-file "$FILTERED_SUMMARY" \
    --shard-count "$SHARD_COUNT" \
    -o "$PLAN_FILE"
fi

mkdir -p "$OUTPUT_DIR/shard_filters"
cp "$PLAN_FILE" "$OUTPUT_DIR/"
cp "$TESTS_FILE" "$OUTPUT_DIR/"
cp "$FILTERED_SUMMARY" "$OUTPUT_DIR/list_summary.json"

if [ "$(jq '.shard_count' "$PLAN_FILE")" != "0" ]; then
  for id in $(jq -r '.shards[].id' "$PLAN_FILE"); do
    python3 "$SCRIPT_DIR/build_shard_blacklist.py" \
      --plan "$PLAN_FILE" \
      --shard-id "$id" \
      -o "$OUTPUT_DIR/shard_filters/shard_${id}.yaml"
  done
fi
