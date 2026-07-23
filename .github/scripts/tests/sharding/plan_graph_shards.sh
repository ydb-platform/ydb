#!/usr/bin/env bash
# Build shard plan from ya make graph JSON (graph replay on shards).
#
# PLAN_MODE: increment_graph (increment cut) or full_graph (full PR-check scope).
#
# Pool capacity may lower the shard count, but never below the wall-time floor
# (default: estimated slowest shard <= 240 min). If the final plan still
# exceeds that budget, the script fails instead of scheduling a timeout-bound
# shard job.
set -euo pipefail

GRAPH_JSON="${1:?usage: plan_graph_shards.sh GRAPH_JSON SHARD_COUNT OUTPUT_DIR}"
SHARD_COUNT="${2:?usage: plan_graph_shards.sh GRAPH_JSON SHARD_COUNT OUTPUT_DIR}"
OUTPUT_DIR="${3:?usage: plan_graph_shards.sh GRAPH_JSON SHARD_COUNT OUTPUT_DIR}"
PLAN_MODE="${PLAN_MODE:-increment_graph}"
TEST_THREADS="${TEST_THREADS:-52}"
MAX_SHARD_WALL_MIN="${MAX_SHARD_WALL_MIN:-240}"

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

SPLIT_EXTRA_ARGS=()
if [ "${DISABLE_PEAK_CAP:-}" = "1" ]; then
  SPLIT_EXTRA_ARGS+=(--no-peak-cap)
fi
# history (default): nightly regression suite duration p90 from YDB.
# timeout_budget: N_chunk_run_tests * SIZE timeout (opt-in).
WEIGHT_MODE="${WEIGHT_MODE:-history}"
SPLIT_EXTRA_ARGS+=(--weight-mode "$WEIGHT_MODE")

# Real pool-capacity cap supersedes the static peak-hour heuristic (same as
# plan_shard_tests.sh): once we know how many runners fit the quota, applying
# peak-cap on top would double-penalize.
CAPACITY_APPLIED=0
if [ -n "${MAX_SHARDS:-}" ]; then
  CAPACITY_APPLIED=1
  SPLIT_EXTRA_ARGS+=(--no-peak-cap)
fi

CONTEXT_JSON="${CONTEXT_JSON:-}"
if [ -z "$CONTEXT_JSON" ]; then
  # Usual layout: graph.json and context.json sit next to each other.
  candidate="$(dirname "$GRAPH_JSON")/context.json"
  if [ -f "$candidate" ]; then
    CONTEXT_JSON="$candidate"
  fi
fi

run_split() {
  local count="$1"
  local split_args=(
    "$GRAPH_JSON"
    --shard-count "$count"
    --profile "${SHARD_PROFILE:-pr}"
    --plan-mode "$PLAN_MODE"
    -o "$PLAN_FILE"
    --build-type "${BUILD_PRESET:-relwithdebinfo}"
    --branch "${HISTORY_BRANCH:-main}"
    --days-back "${HISTORY_DAYS_BACK:-14}"
    --threads "$TEST_THREADS"
  )
  if [ -n "$CONTEXT_JSON" ] && [ -f "$CONTEXT_JSON" ]; then
    split_args+=(--context "$CONTEXT_JSON")
  fi
  python3 "$SCRIPT_DIR/split_graph_result.py" \
    "${split_args[@]}" \
    "${SPLIT_EXTRA_ARGS[@]}"
}

if [ "$SHARD_COUNT" = "auto" ]; then
  if [ "${DISABLE_PEAK_CAP:-}" = "1" ] || [ "$CAPACITY_APPLIED" = "1" ]; then
    echo "Peak-hour cap disabled for auto shard count (DISABLE_PEAK_CAP=${DISABLE_PEAK_CAP:-0}, capacity=${CAPACITY_APPLIED})"
  else
    echo "Peak-hour cap enabled for auto shard count"
  fi
fi

run_split "$SHARD_COUNT"

# Resolve final shard count: capacity may shrink the plan, wall-time floor may
# raise it back so the slowest shard stays within MAX_SHARD_WALL_MIN.
# timeout_budget weights are worst-case CPU ceilings, not expected duration —
# do not use them to inflate shard count or fail the prepare job.
final_count=$(
  PLAN_FILE="$PLAN_FILE" \
  MAX_SHARDS="${MAX_SHARDS:-}" \
  TEST_THREADS="$TEST_THREADS" \
  MAX_SHARD_WALL_MIN="$MAX_SHARD_WALL_MIN" \
  WEIGHT_MODE="$WEIGHT_MODE" \
  SCRIPT_DIR="$SCRIPT_DIR" \
  python3 - <<'PY'
import json
import os
import sys

sys.path.insert(0, os.environ["SCRIPT_DIR"])
from choose_shard_count import min_shards_for_wall_budget

plan = json.loads(open(os.environ["PLAN_FILE"], encoding="utf-8").read())
active = int(plan.get("shard_count") or 0)
if active <= 0:
    print(0)
    raise SystemExit(0)

weight = float(plan.get("total_weight") or 0.0)
threads = int(os.environ["TEST_THREADS"])
max_wall = float(os.environ["MAX_SHARD_WALL_MIN"])
weight_mode = (os.environ.get("WEIGHT_MODE") or "history").strip().lower()
mode = ((plan.get("weighting") or {}).get("mode") or "")
timeout_budget = weight_mode == "timeout_budget" or mode == "graph_uid_timeout_budget_lpt"

desired = active
capacity = os.environ.get("MAX_SHARDS") or ""
if capacity:
    capacity_n = int(capacity)
    if desired > capacity_n:
        print(
            f"Pool capacity prefers {capacity_n} shards (plan had {desired})",
            file=sys.stderr,
        )
        desired = capacity_n

if not timeout_budget:
    wall_floor = min_shards_for_wall_budget(
        weight, threads=threads, max_shard_wall_min=max_wall
    )
    if desired < wall_floor:
        print(
            f"Raising shards from {desired} to {wall_floor} "
            f"to keep estimated shard wall <= {max_wall:.0f} min "
            f"(single-job ~{float(plan.get('estimated_single_job_min') or 0):.1f} min)",
            file=sys.stderr,
        )
        desired = wall_floor
elif desired < active:
    print(
        f"timeout_budget mode: keeping capacity/profile shard count {desired} "
        f"(worst-case weight would want more; not used as wall SLA)",
        file=sys.stderr,
    )

print(desired)
PY
)

active_count=$(jq '.shard_count' "$PLAN_FILE")
if [ "$final_count" != "$active_count" ]; then
  echo "Replanning graph shards: $active_count -> $final_count"
  run_split "$final_count"
fi

# Wall-time check: hard fail for history/expected weights; warn-only for
# timeout_budget (N * SIZE timeout is a packing ceiling, not ETA).
python3 - <<PY
import json
import sys

plan = json.loads(open("$PLAN_FILE", encoding="utf-8").read())
count = int(plan.get("shard_count") or 0)
if count <= 0:
    raise SystemExit(0)
crit = float(plan.get("estimated_critical_path_min") or 0.0)
max_wall = float("$MAX_SHARD_WALL_MIN")
mode = ((plan.get("weighting") or {}).get("mode") or "")
timeout_budget = mode == "graph_uid_timeout_budget_lpt" or "$WEIGHT_MODE" == "timeout_budget"
if crit > max_wall:
    msg = (
        f"estimated slowest shard ~{crit:.1f} min exceeds "
        f"max_shard_wall_min={max_wall:.0f} with {count} shards"
    )
    if timeout_budget:
        print(
            f"WARNING: {msg} (timeout_budget is worst-case packing weight; "
            f"continuing)",
            file=sys.stderr,
        )
        raise SystemExit(0)
    print(
        f"ERROR: {msg}. "
        f"Increase shard_count / free pool capacity, or shrink test scope.",
        file=sys.stderr,
    )
    raise SystemExit(1)
print(
    f"Wall-time check OK: estimated slowest shard ~{crit:.1f} min "
    f"(budget {max_wall:.0f} min, shards={count})"
)
PY

# Read plan from file (--slurpfile) to avoid ARG_MAX when uid_assignments is large.
jq -n --arg mode "$PLAN_MODE" --slurpfile plan "$PLAN_FILE" \
  '{
    total_suites: 0,
    total_tests: ($plan[0].total_graph_nodes // 0),
    total_weight: ($plan[0].total_weight // 0),
    increment_filtered: ($mode == "increment_graph"),
    increment_graph_suites: (if $mode == "increment_graph" then ($plan[0].total_graph_nodes // 0) else 0 end),
    plan_mode: $mode,
    weighting: ($plan[0].weighting // {})
  }' >"$LIST_SUMMARY"

echo "Graph plan ($PLAN_MODE): $(jq '.shard_count' "$PLAN_FILE") shards, $(jq '.total_graph_nodes' "$PLAN_FILE") graph nodes"
