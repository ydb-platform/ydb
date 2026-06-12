#!/usr/bin/env bash
# List tests for PR-check sharding plan (same scope/filters as shard test jobs).
set -euo pipefail

TARGET="${1:?usage: list_tests_for_shard_plan.sh TARGET OUTPUT_LOG}"
OUTPUT="${2:?usage: list_tests_for_shard_plan.sh TARGET OUTPUT_LOG}"
BUILD_PRESET="${BUILD_PRESET:-relwithdebinfo}"

./ya test -L \
  --build "$BUILD_PRESET" \
  --test-size small \
  --test-size medium \
  -DDEBUGINFO_LINES_ONLY \
  "$TARGET" >"$OUTPUT" 2>&1
