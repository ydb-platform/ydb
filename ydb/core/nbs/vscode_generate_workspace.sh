#!/usr/bin/env bash

EXTRA_ARGS=()
WORKSPACE_DIR="workspace"

if [[ "${1:-}" == "cursor" ]]; then
    EXTRA_ARGS+=(--cursor)
    WORKSPACE_DIR="workspace/cursor"
fi

../../../ya ide vscode "${EXTRA_ARGS[@]}" --cpp --allow-project-inside-arc --use-arcadia-root -P="${WORKSPACE_DIR}" \
../../../library/cpp        \
../../../util               \
../../apps                  \
../../core                  \
../../library               \
../../public                \
../../services              \
../../tests
