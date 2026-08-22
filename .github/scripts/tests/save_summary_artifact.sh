#!/bin/bash
# Save GITHUB_STEP_SUMMARY to artifact for later collection

set -e

# Get job name from environment or construct it
JOB_NAME="${GITHUB_JOB:-unknown}"
# If job name contains branch:build_preset format, use it
# Otherwise try to construct from matrix variables
if [[ "$JOB_NAME" == *":"* ]]; then
    SUMMARY_NAME="summary-${JOB_NAME}.md"
else
    # Try to get from matrix context
    BRANCH="${MATRIX_BRANCH:-${GITHUB_REF_NAME:-unknown}}"
    BUILD_PRESET="${MATRIX_BUILD_PRESET:-${INPUTS_BUILD_PRESET:-unknown}}"
    SUMMARY_NAME="summary-${BRANCH}:${BUILD_PRESET}.md"
fi

SUMMARY_DIR="${GITHUB_WORKSPACE}/summary_artifacts"
mkdir -p "$SUMMARY_DIR"

SUMMARY_FILE="${SUMMARY_DIR}/${SUMMARY_NAME}"

if [ -f "$GITHUB_STEP_SUMMARY" ]; then
    cp "$GITHUB_STEP_SUMMARY" "$SUMMARY_FILE"
    echo "Saved summary to $SUMMARY_FILE"
else
    echo "Warning: GITHUB_STEP_SUMMARY not found"
    # Create empty file so artifact upload doesn't fail
    touch "$SUMMARY_FILE"
fi
