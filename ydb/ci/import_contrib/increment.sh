# Usage: kikimr/scripts/oss/import_contrib/increment.sh <source_git_repo_root> <target_arc_repo_root>
# Expects previous github SHA in <target_arc_repo_root>/contrib/ydb/git_sha.txt
# Will get/patch only files changed since that commit

set -e
set -o pipefail

GIT_ROOT=$1
if [ -z "${GIT_ROOT}" ]; then
  echo "Source Git root must be provided as a first free arg"
  exit 1
fi
echo "Source Git root: ${GIT_ROOT}"

ARC_ROOT=$2
if [ -z "${ARC_ROOT}" ]; then
  echo "Target Arc root must be provided as a second free arg"
  exit 1
fi
echo "Target Arc root: ${ARC_ROOT}"

srcdir=$GIT_ROOT/ydb

shaloc="$ARC_ROOT/contrib/ydb/github_sha.txt"
base=$(cat $shaloc)
if [ -z "${shaloc}" ]; then
  echo "Could not retrieve last Git SHA from $shaloc"
  exit 1
fi
echo "Last Git SHA from $shaloc: $base"

tmpdir0=$(mktemp -d)
echo "Copying current versions of appended/modified files to $tmpdir0..."

cp -r $GIT_ROOT/ydb/* $tmpdir0
echo "Files in $tmpdir0: $(find $tmpdir0 -type f | wc -l)"

(cd $ARC_ROOT && ARC_ROOT=$ARC_ROOT YDBDIR=$tmpdir0 bash $ARC_ROOT/kikimr/scripts/oss/check-contrib.sh)

echo "Removing new files in blacklist folders..."
# Blacklist catalogs not to generate diffs

echo 'arc status'
(cd $ARC_ROOT && arc status | cat)

