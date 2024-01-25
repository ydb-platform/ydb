# Usage: cherrypick.sh <ydb_repo_root>
# Expects previous github SHA in <ydb_repo_root>/library/rightlib_sha.txt
# Reads new commits from the 'rightlib' branch, cheery-picks them to the current branch, updates library/rightlib_sha.txt in the working tree

set -e
set -o pipefail

ROOT=$1
if [ -z "${ROOT}" ]; then
  echo "YDB repo root must be provided as a first free arg"
  exit 1
fi
echo "YDB repo root: ${ROOT}"

shapath="${ROOT}/library/rightlib_sha.txt"
prevsha=$(cat ${shapath}) || true
if [ -z "${prevsha}" ]; then
  echo "File ${shapath} not found, which must contain previous completed import commit SHA"
  exit 1
fi
echo "Previous sha: ${prevsha}"

newsha=$(cd ${ROOT} && git rev-parse rightlib)
echo "Rightlib current commit sha: $newsha"

list=$(cd ${ROOT} && git log ${prevsha}..rightlib --reverse --pretty=oneline --no-decorate | awk '{print $1}')
for sha in $list;do
  echo $sha
  (cd ${ROOT} && git cherry-pick $sha)
  echo "---"
done

exit

rsync -r $LIB_ROOT/ya $LIB_ROOT/build $LIB_ROOT/certs $LIB_ROOT/cmake $LIB_ROOT/contrib $LIB_ROOT/library $LIB_ROOT/tools $LIB_ROOT/util $LIB_ROOT/vendor $LIB_ROOT/yt $MAIN_ROOT \
  --filter '- **/a.yaml' --filter '- **/.arcignore' --filter '- **/.yandex_meta/' --filter '- contrib/ydb/' --filter '- build/internal/' --filter '- build/ext_mapping.conf.json' \
  --filter '- **/CMakeLists*.txt' --delete

echo ${newsha} > ${shapath}

