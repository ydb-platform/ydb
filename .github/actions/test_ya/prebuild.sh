#!/usr/bin/env bash
# Build test binaries into the local ya cache without running tests.
# Sourced from action.yml so analytics / params / GRAPH_* stay in the same shell.
set +e
echo "Prebuild: ya make --build-only-test-deps (no test-selection flags), then tests use local cache"
analytics start ya_prebuild

# Keep build configuration, but do not pass test selection or execution flags
# to the prebuild. In particular, -T and -A would make this run tests.
PREBUILD_PARAMS=()
SKIP_PREBUILD_PARAM_VALUE=0
for param in "${params[@]}"; do
  if [ "$SKIP_PREBUILD_PARAM_VALUE" -eq 1 ]; then
    SKIP_PREBUILD_PARAM_VALUE=0
    continue
  fi
  case "$param" in
    --add-peerdirs-tests|--test-failure-code|--test-threads|--link-threads|--test-blacklist-path)
      SKIP_PREBUILD_PARAM_VALUE=1
      ;;
    -T|-A|-X|--retest|--test-*|--cache-tests|--dont-merge-split-tests|--cleanup-child-processes)
      ;;
    *)
      PREBUILD_PARAMS+=("$param")
      ;;
  esac
done

./ya make "${PREBUILD_PARAMS[@]}" --build-only-test-deps \
  --save-graph-to "$GRAPH_PATH" --save-context-to "$CONTEXT_PATH" \
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
