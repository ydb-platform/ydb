#!/usr/bin/env bash
# List tests for PR-check sharding plan (same scope/filters as shard test jobs).
set -euo pipefail

TARGET="${1:?usage: list_tests_for_shard_plan.sh TARGET OUTPUT_LOG}"
OUTPUT="${2:?usage: list_tests_for_shard_plan.sh TARGET OUTPUT_LOG}"
BUILD_PRESET="${BUILD_PRESET:-relwithdebinfo}"
TEST_THREADS="${TEST_THREADS:-52}"
LINK_THREADS="${LINK_THREADS:-12}"

# Keep the suite universe identical to what the shard test jobs (and the
# monolith PR-check) actually run: --add-peerdirs-tests all pulls in tests of
# PEERDIR'd modules, otherwise the plan is built over a narrower set than the
# shards execute and full runs undercount (see pr-check-parallel-parity.md).
YA_OPTS=(
  --test-size small
  --test-size medium
  --test-threads "$TEST_THREADS"
  --link-threads "$LINK_THREADS"
  -DDEBUGINFO_LINES_ONLY
  -DUSE_EAT_MY_DATA
  --add-peerdirs-tests all
  --cache-tests
)

case "$BUILD_PRESET" in
  debug)
    YA_OPTS+=(--build debug)
    ;;
  relwithdebinfo)
    YA_OPTS+=(--build relwithdebinfo)
    ;;
  release)
    YA_OPTS+=(--build release)
    ;;
  release-asan)
    YA_OPTS+=(--build release --sanitize=address)
    ;;
  release-tsan)
    YA_OPTS+=(--build release --sanitize=thread)
    ;;
  release-msan)
    YA_OPTS+=(--build release --sanitize=memory)
    ;;
  *)
    echo "Unsupported BUILD_PRESET for ya test -L: $BUILD_PRESET" >&2
    exit 1
    ;;
esac

if [ -n "${REMOTE_CACHE_URL:-}" ] && [ -n "${BAZEL_REMOTE_PASSWORD_FILE:-}" ] && [ -f "$BAZEL_REMOTE_PASSWORD_FILE" ]; then
  YA_OPTS+=(
    --bazel-remote-store
    --bazel-remote-base-uri "$REMOTE_CACHE_URL"
    --bazel-remote-username "${REMOTE_CACHE_USERNAME:-}"
    --bazel-remote-password-file "$BAZEL_REMOTE_PASSWORD_FILE"
  )
fi

echo "Listing tests for shard plan: target=$TARGET preset=$BUILD_PRESET threads=$TEST_THREADS link=$LINK_THREADS" >&2
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
