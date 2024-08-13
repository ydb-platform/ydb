#!/usr/bin/env bash
if [ -z "$BUCKET_PATH" ]; then
  echo "BUCKET_PATH is empty"
  exit 1
fi

set -xeuo pipefail

BUCKET_LATEST_PATH=$BUCKET_PATH/latest
RELEASES_JSON=https://api.github.com/repos/actions/runner/releases

latest_release=$(gh api $RELEASES_JSON |  jq -r 'limit(1; .[].assets[] | select(.name | test("-linux-x64-\\d+\\.\\d+\\.\\d+.tar.gz")).browser_download_url)')
latest_fn=$(basename $latest_release)

s3cmd info $BUCKET_PATH/$latest_fn && ret=$? || ret=$?

# EX_NOTFOUND = 12 (https://github.com/s3tools/s3cmd/blob/master/S3/ExitCodes.py#L10)
if [ $ret -eq 12 ]; then
  curl -sSL -w "%{url_effective} %{remote_ip} %{speed_download} %{size_download}\n" -o $latest_fn $latest_release
  s3cmd put --acl-public --no-preserve $latest_fn $BUCKET_PATH/$latest_fn
  echo "$latest_fn" | s3cmd put --acl-public - $BUCKET_LATEST_PATH
elif [ $ret -ne 0 ]; then
  exit 1
fi

