#!/usr/bin/env bash
# Stage graph/plan files from prepare and sync to S3 under .../x86-64/plan/.
set -euo pipefail

GRAPH_JSON="${1:?usage: upload_plan_artifacts_to_s3.sh GRAPH_JSON CONTEXT_JSON PLAN_DIR}"
CONTEXT_JSON="${2:?usage: upload_plan_artifacts_to_s3.sh GRAPH_JSON CONTEXT_JSON PLAN_DIR}"
PLAN_DIR="${3:?usage: upload_plan_artifacts_to_s3.sh GRAPH_JSON CONTEXT_JSON PLAN_DIR}"

STAGING="${STAGING_DIR:-plan_upload}"
S3_BUCKET_PATH="${S3_BUCKET_PATH:?S3_BUCKET_PATH is required}"

rm -rf "$STAGING"
mkdir -p "$STAGING"

cp "$GRAPH_JSON" "$STAGING/graph.json"
cp "$CONTEXT_JSON" "$STAGING/context.json"

for name in shard_plan.json list_summary.json shard_plan_summary.md; do
  if [ -f "$PLAN_DIR/$name" ]; then
    cp "$PLAN_DIR/$name" "$STAGING/$name"
  fi
done

.github/scripts/Indexer/indexer.py -r "$STAGING/"
s3cmd sync --follow-symlinks --acl-public --no-progress --stats --no-mime-magic --guess-mime-type --no-check-md5 \
  "$STAGING/" "$S3_BUCKET_PATH/"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ] && [ -n "${S3_URL_PREFIX:-}" ]; then
  {
    echo ""
    echo "## Plan artifacts (S3)"
    echo ""
    echo "[Plan index](${S3_URL_PREFIX}/index.html) | [graph.json](${S3_URL_PREFIX}/graph.json) | [shard_plan.json](${S3_URL_PREFIX}/shard_plan.json)"
  } >> "$GITHUB_STEP_SUMMARY"
fi
