#!/usr/bin/env bash
# Build test binaries into the local ya cache without running tests.
# Sourced from action.yml so analytics / params / GRAPH_* stay in the same shell.
set +e
echo "Prebuild: ya make -A --build-only-test-deps (no test run), then tests use local cache"
analytics start ya_prebuild
./ya make "${params[@]}" --build-only-test-deps \
  ${GRAPH_PATH:+--build-custom-json=$GRAPH_PATH} \
  ${CONTEXT_PATH:+--custom-context=$CONTEXT_PATH} \
  $YA_MAKE_TARGET
PREBUILD_RC=$?
set -e
if [ "$PREBUILD_RC" -eq 0 ]; then
  analytics end ya_prebuild
else
  analytics end ya_prebuild --rc "$PREBUILD_RC" --error "ya prebuild rc=${PREBUILD_RC}"
  echo "status=failed" >> "$GITHUB_OUTPUT"
  echo "Prebuild ya make failed with rc=$PREBUILD_RC" | GITHUB_TOKEN="${GITHUB_TOKEN}" .github/scripts/tests/comment-pr.py --color red
  exit "$PREBUILD_RC"
fi
