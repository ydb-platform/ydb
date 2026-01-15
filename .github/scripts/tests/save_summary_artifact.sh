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

echo "DEBUG: GITHUB_STEP_SUMMARY=${GITHUB_STEP_SUMMARY}"
echo "DEBUG: SUMMARY_FILE=${SUMMARY_FILE}"
echo "DEBUG: SUMMARY_DIR=${SUMMARY_DIR}"

if [ -f "$GITHUB_STEP_SUMMARY" ]; then
    echo "DEBUG: GITHUB_STEP_SUMMARY exists, size: $(wc -c < "$GITHUB_STEP_SUMMARY") bytes"
    cp "$GITHUB_STEP_SUMMARY" "$SUMMARY_FILE"
    echo "Saved summary to $SUMMARY_FILE (size: $(wc -c < "$SUMMARY_FILE") bytes)"
    echo "DEBUG: First 200 chars of summary:"
    head -c 200 "$SUMMARY_FILE" || true
    echo ""
else
    echo "Warning: GITHUB_STEP_SUMMARY not found at $GITHUB_STEP_SUMMARY"
    # Create empty file so artifact upload doesn't fail
    touch "$SUMMARY_FILE"
    echo "Created empty summary file: $SUMMARY_FILE"
fi
