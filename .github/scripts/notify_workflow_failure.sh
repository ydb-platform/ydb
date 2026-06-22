#!/bin/bash
# Open or update a GitHub Issue that tracks failures of the current workflow.
#
# Usage: notify_workflow_failure.sh <label> <title> <owners>
#
# Looks for an open issue with <label>. If found, appends a comment with a link
# to the failing run and a mention of <owners>. If not found, opens a new
# issue. The label is created if it does not exist yet.
#
# Required env:
#   GH_TOKEN              auth for `gh` (use a bot token like YDBOT_TOKEN so
#                         team @-mentions actually notify — GITHUB_TOKEN
#                         suppresses them).
#   GITHUB_REPOSITORY     "owner/repo"  (set by GitHub Actions)
#   GITHUB_WORKFLOW       human workflow name (set by GitHub Actions)
#   GITHUB_SERVER_URL     e.g. https://github.com (set by GitHub Actions)
#   GITHUB_RUN_ID         current run id (set by GitHub Actions)
#   GITHUB_EVENT_NAME     trigger, e.g. "schedule" / "workflow_dispatch"
set -euo pipefail

LABEL="$1"
TITLE="$2"
OWNERS="$3"

: "${GITHUB_REPOSITORY:?must be set}"
: "${GITHUB_SERVER_URL:?must be set}"
: "${GITHUB_RUN_ID:?must be set}"

RUN_URL="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"
WORKFLOW_NAME="${GITHUB_WORKFLOW:-the workflow}"
TRIGGER="${GITHUB_EVENT_NAME:-unknown}"

gh label create "$LABEL" \
  --repo "$GITHUB_REPOSITORY" \
  --color "B60205" \
  --description "Automated: $TITLE" \
  --force >/dev/null 2>&1 || true

COMMENT=$(cat <<EOF
Run failed (trigger: \`$TRIGGER\`): $RUN_URL

cc $OWNERS
EOF
)

ISSUE=$(gh issue list --repo "$GITHUB_REPOSITORY" --state open --label "$LABEL" \
  --json number --jq '.[0].number // empty')

if [ -n "$ISSUE" ]; then
  echo "Updating existing issue #$ISSUE"
  gh issue comment "$ISSUE" --repo "$GITHUB_REPOSITORY" --body "$COMMENT"
else
  echo "Creating new failure issue"
  BODY=$(cat <<EOF
The \`$WORKFLOW_NAME\` workflow is failing.

Latest failed run (trigger: \`$TRIGGER\`): $RUN_URL

This issue was opened automatically; it will receive a new comment on every subsequent failure and should be closed once the workflow is green again.

cc $OWNERS
EOF
)
  gh issue create --repo "$GITHUB_REPOSITORY" \
    --title "$TITLE" \
    --label "$LABEL" \
    --body "$BODY"
fi
