# Usage: squash.sh <source_ydblib_repo_root> <target_ydb_repo_root>

set -e
set -o pipefail

LIB_ROOT=$1
if [ -z "${LIB_ROOT}" ]; then
  echo "Source lib root must be provided as a first free arg"
  exit 1
fi
echo "Source library root: ${LIB_ROOT}"
sha=$(cd ${LIB_ROOT} && git rev-parse HEAD)
echo "Source commit sha: $sha"

MAIN_ROOT=$2
if [ -z "${MAIN_ROOT}" ]; then
  echo "Target main root must be provided as a second free arg"
  exit 1
fi
echo "Target Main root: ${MAIN_ROOT}"

sharel="library/rightlib_sha.txt"
shaabs="${MAIN_ROOT}/${sharel}"
echo "Sha file: ${shaabs}"
echo $sha > ${shaabs}

rsync -r $LIB_ROOT/ya $LIB_ROOT/build $LIB_ROOT/certs $LIB_ROOT/cmake $LIB_ROOT/contrib $LIB_ROOT/library $LIB_ROOT/tools $LIB_ROOT/util $LIB_ROOT/vendor $LIB_ROOT/yt $MAIN_ROOT \
  --filter '- **/a.yaml' --filter '- **/.arcignore' --filter '- **/.yandex_meta/' --filter '- contrib/ydb/' --filter '- build/internal/' --filter '- build/ext_mapping.conf.json' \
  --filter '- **/CMakeLists*.txt' --filter "- ${sharel}" --delete

