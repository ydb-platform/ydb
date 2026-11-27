#!/usr/bin/env bash
# Observability only for postcommit gate: writes GITHUB_STEP_SUMMARY.
# Does not change runner labels or gate decision.
set -euo pipefail

: "${GITHUB_REPOSITORY:?}"
: "${GITHUB_TOKEN:?}"
: "${GATE_ACTION:?}"
: "${QUEUE_SIZE:?}"
: "${QUEUE_THRESHOLD:?}"
: "${MAX_AGE_HOURS:?}"
: "${IGNORED_STALE:?}"

API_VERSION="${GITHUB_API_VERSION:-2022-11-28}"
SUMMARY_FILE="${GITHUB_STEP_SUMMARY:-/dev/stdout}"
max_age_seconds=$(jq -n --argjson h "$MAX_AGE_HOURS" '$h * 3600')

api_get() {
  local url=$1
  curl --retry 5 --retry-all-errors --retry-delay 10 --fail --show-error -Ls \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "X-GitHub-Api-Version: ${API_VERSION}" \
    "$url"
}

queued_json=$(api_get \
  "https://api.github.com/repos/${GITHUB_REPOSITORY}/actions/runs?per_page=100&page=1&status=queued&event=pull_request_target") || queued_json='{"workflow_runs":[]}'

pr_check_items=$(jq --argjson max_age "$max_age_seconds" '
  def fmt_duration:
    if . < 60 then "\(.|floor)s"
    elif . < 3600 then "\((./60)|floor)m"
    elif . < 86400 then "\((./3600)|floor)h \(((./60)|floor % 60))m"
    else "\((./86400)|floor)d \(((./3600)|floor % 24))h"
    end;
  [.workflow_runs[]
    | select((.name // "") == "PR-check")
    | . + { waiting_seconds: (now - (.updated_at | fromdate)) }
    | select(.waiting_seconds > 60)
    | {
        title: (.display_title // .head_branch // ("run #" + (.id|tostring))),
        html_url,
        waiting: (.waiting_seconds | fmt_duration),
        waiting_seconds,
        ignored: (.waiting_seconds > $max_age)
      }
  ]
  | sort_by(-.waiting_seconds)
' <<<"$queued_json")
pr_check_queue_size=$(jq 'length' <<<"$pr_check_items")

postcommit_fetch_ok=1
postcommit_queued_json=$(api_get \
  "https://api.github.com/repos/${GITHUB_REPOSITORY}/actions/runs?per_page=100&page=1&status=queued&event=push") || postcommit_fetch_ok=0
if (( postcommit_fetch_ok == 0 )); then
  postcommit_queued_json='{"workflow_runs":[]}'
fi

postcommit_parse_ok=1
postcommit_items=$(jq '
  def fmt_duration:
    if . < 60 then "\(.|floor)s"
    elif . < 3600 then "\((./60)|floor)m"
    elif . < 86400 then "\((./3600)|floor)h \(((./60)|floor % 60))m"
    else "\((./86400)|floor)d \(((./3600)|floor % 24))h"
    end;
  [.workflow_runs[]
    | (.name // "") as $name
    | select($name == "PR-check" or ($name | startswith("Postcommit_")))
    | . + { waiting_seconds: (now - (.updated_at | fromdate)) }
    | select(.waiting_seconds > 60)
    | {
        name: $name,
        title: (.display_title // .head_branch // ("run #" + (.id|tostring))),
        html_url,
        waiting: (.waiting_seconds | fmt_duration),
        waiting_seconds
      }
  ]
  | sort_by(-.waiting_seconds)
' <<<"$postcommit_queued_json") || postcommit_parse_ok=0
if (( postcommit_parse_ok == 0 )); then
  postcommit_items='[]'
  postcommit_fetch_ok=0
fi
postcommit_queue_size=$(jq 'length' <<<"$postcommit_items")

{
  echo "## Postcommit gate: ${GATE_ACTION}"
  echo ""
  echo "| | |"
  echo "|---|---|"
  echo "| Decision | **${GATE_ACTION}** |"
  echo "| Queue size | ${QUEUE_SIZE} (vars.POSTCOMMIT_QUEUE_THRESHOLD=${QUEUE_THRESHOLD}) |"
  echo "| Max age filter | ${MAX_AGE_HOURS}h (vars.POSTCOMMIT_GATE_CONFIG.ignore_pr_check_older_than_hours) |"
  echo "| Ignored stale PR-check | ${IGNORED_STALE} |"
  echo ""
  echo "### Queued PR-check (>60s): ${pr_check_queue_size} total, ${QUEUE_SIZE} in queue, ${IGNORED_STALE} ignored"
  echo ""
  if (( pr_check_queue_size > 0 )); then
    echo "| Gate | Waiting | Run |"
    echo "|------|---------|-----|"
    jq -r '
      def esc_link: tostring | gsub("\\]"; "\\\\]") | gsub("\\|"; "\\\\|") | gsub("\n"; " ");
      .[]
      | (if .ignored then "ignored" else "counted" end) as $gate
      | (.title | esc_link) as $title
      | (if .ignored then "~~[\($title)](\(.html_url))~~" else "[\($title)](\(.html_url))" end) as $run
      | "| \($gate) | \(.waiting) | \($run) |"
    ' <<<"$pr_check_items"
  else
    echo "_No queued PR-check runs._"
  fi
  echo ""
  if (( postcommit_fetch_ok == 0 )); then
    echo "### Queued postcommit (>60s): unavailable"
    echo ""
    echo "_Failed to fetch queued push runs._"
  else
    echo "### Queued postcommit (>60s): ${postcommit_queue_size}"
    echo ""
    if (( postcommit_queue_size > 0 )); then
      echo "| Waiting | Workflow | Run |"
      echo "|---------|----------|-----|"
      jq -r '
        def esc_cell: tostring | gsub("\\|"; "\\\\|") | gsub("\n"; " ");
        def esc_link: tostring | gsub("\\]"; "\\\\]") | gsub("\\|"; "\\\\|") | gsub("\n"; " ");
        .[]
        | (.title | esc_link) as $title
        | "| \(.waiting) | `\(.name | esc_cell)` | [\($title)](\(.html_url)) |"
      ' <<<"$postcommit_items"
    else
      echo "_No queued postcommit runs._"
    fi
  fi
} >> "$SUMMARY_FILE"
