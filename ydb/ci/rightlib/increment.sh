# Usage: cherrypick.sh <source_ydblib_repo_root> <target_ydb_repo_root>
# Expects previous github SHA in <target_ydb_repo_root>/library_import.txt
# Will loop through the commits in the source repo after previous SHA, and cherry-pick is to the target.

set -e
set -o pipefail

LIB_ROOT=$1
if [ -z "${LIB_ROOT}" ]; then
  echo "Source lib root must be provided as a first free arg"
  exit 1
fi
echo "Source library root: ${LIB_ROOT}"
newsha=$(cd ${LIB_ROOT} && git rev-parse HEAD)
echo "Source current commit sha: $newsha"

MAIN_ROOT=$2
if [ -z "${MAIN_ROOT}" ]; then
  echo "Target main root must be provided as a second free arg"
  exit 1
fi
echo "Target Main root: ${MAIN_ROOT}"
shapath="${MAIN_ROOT}/library_import.txt"
prevsha=$(cat ${shapath}) || true
if [ -z "${prevsha}" ]; then
  echo "File ${shapath} not found, which must contain previous completed import commit SHA"
  exit 1
fi
echo "Previous sha: ${prevsha}"

list=$(cd ${LIB_ROOT} && git log ${prevsha}..HEAD --pretty=oneline --no-decorate | awk '{print $1}')
for sha in $list;do
  echo $sha
  (cd ${MAIN_ROOT} && git --git-dir=${LIB_ROOT}/.git format-patch -k -1 --stdout $sha | git am -3 -k)
  echo "---"
done

exit

rsync -r $LIB_ROOT/ya $LIB_ROOT/build $LIB_ROOT/certs $LIB_ROOT/cmake $LIB_ROOT/contrib $LIB_ROOT/library $LIB_ROOT/tools $LIB_ROOT/util $LIB_ROOT/vendor $LIB_ROOT/yt $MAIN_ROOT \
  --filter '- **/a.yaml' --filter '- **/.arcignore' --filter '- **/.yandex_meta/' --filter '- contrib/ydb/' --filter '- build/internal/' --filter '- build/ext_mapping.conf.json' \
  --filter '- **/CMakeLists*.txt' --delete

