#!/usr/bin/env bash
# Build increment shard plan from cut-graph JSON (graph replay path).
set -euo pipefail

GRAPH_JSON="${1:?usage: plan_increment_graph_shards.sh GRAPH_JSON SHARD_COUNT OUTPUT_DIR}"
SHARD_COUNT="${2:?usage: plan_increment_graph_shards.sh GRAPH_JSON SHARD_COUNT OUTPUT_DIR}"
OUTPUT_DIR="${3:?usage: plan_increment_graph_shards.sh GRAPH_JSON SHARD_COUNT OUTPUT_DIR}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAN_FILE="$OUTPUT_DIR/shard_plan.json"
LIST_SUMMARY="$OUTPUT_DIR/list_summary.json"

mkdir -p "$OUTPUT_DIR"

CAPACITY_CONFIG="${CAPACITY_CONFIG:-$SCRIPT_DIR/../../../config/runner_capacity.yml}"
if [ -z "${MAX_SHARDS:-}" ] && [ -f "$CAPACITY_CONFIG" ] && [ -n "${GITHUB_TOKEN:-}${GH_TOKEN:-}" ]; then
  capacity_cap=$(python3 "$SCRIPT_DIR/estimate_runner_capacity.py" \
    --config "$CAPACITY_CONFIG" \
    --preset-label "build-preset-${BUILD_PRESET:-relwithdebinfo}" || true)
  if [ -n "$capacity_cap" ]; then
    MAX_SHARDS="$capacity_cap"
    echo "Pool capacity cap: $MAX_SHARDS runners fit into the quota budget"
  fi
fi

run_split() {
  local count="$1"
  python3 "$SCRIPT_DIR/split_graph_result.py" \
    "$GRAPH_JSON" \
    --shard-count "$count" \
    -o "$PLAN_FILE" \
    --build-type "${BUILD_PRESET:-relwithdebinfo}" \
    --branch "${HISTORY_BRANCH:-main}" \
    --days-back "${HISTORY_DAYS_BACK:-3}" \
    --threads "${TEST_THREADS:-52}"
}

run_split "$SHARD_COUNT"

if [ -n "${MAX_SHARDS:-}" ]; then
  active=$(jq '.shard_count' "$PLAN_FILE")
  if [ "$active" -gt "$MAX_SHARDS" ]; then
    echo "Capping increment shards from $active to $MAX_SHARDS (pool capacity)"
    run_split "$MAX_SHARDS"
  fi
fi

# Read plan from file (--slurpfile) to avoid ARG_MAX when uid_assignments is large.
jq -n --slurpfile plan "$PLAN_FILE" \
  '{
    total_suites: 0,
    total_tests: ($plan[0].total_graph_nodes // 0),
    total_weight: ($plan[0].total_weight // 0),
    increment_filtered: true,
    increment_graph_suites: ($plan[0].total_graph_nodes // 0),
    plan_mode: "increment_graph",
    weighting: ($plan[0].weighting // {})
  }' >"$LIST_SUMMARY"

echo "Increment graph plan: $(jq '.shard_count' "$PLAN_FILE") shards, $(jq '.total_graph_nodes' "$PLAN_FILE") graph nodes"
