#!/usr/bin/env bash
# List tests for PR-check sharding plan (same scope/filters as shard test jobs).
set -euo pipefail

TARGET="${1:?usage: list_tests_for_shard_plan.sh TARGET OUTPUT_LOG}"
OUTPUT="${2:?usage: list_tests_for_shard_plan.sh TARGET OUTPUT_LOG}"
BUILD_PRESET="${BUILD_PRESET:-relwithdebinfo}"

YA_OPTS=(
  --build "$BUILD_PRESET"
  --test-size small
  --test-size medium
  -DDEBUGINFO_LINES_ONLY
  --cache-tests
)

if [ -n "${REMOTE_CACHE_URL:-}" ] && [ -n "${BAZEL_REMOTE_PASSWORD_FILE:-}" ] && [ -f "$BAZEL_REMOTE_PASSWORD_FILE" ]; then
  YA_OPTS+=(
    --bazel-remote-store
    --bazel-remote-base-uri "$REMOTE_CACHE_URL"
    --bazel-remote-username "${REMOTE_CACHE_USERNAME:-}"
    --bazel-remote-password-file "$BAZEL_REMOTE_PASSWORD_FILE"
  )
fi

echo "Listing tests for shard plan: target=$TARGET preset=$BUILD_PRESET" >&2
set +e
./ya test -L "${YA_OPTS[@]}" "$TARGET" 2>&1 | tee "$OUTPUT"
rc=${PIPESTATUS[0]}
set -e

if [ "$rc" -ne 0 ]; then
  if grep -qE '^Total [0-9]+ tests' "$OUTPUT"; then
    echo "warning: ya test -L exited $rc but produced a test list; continuing" >&2
  else
    echo "ya test -L failed with exit code $rc" >&2
    tail -n 80 "$OUTPUT" >&2 || true
    exit "$rc"
  fi
fi
