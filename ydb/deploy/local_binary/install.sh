#!/usr/bin/env bash

set -euo pipefail

VERBOSE=${VERBOSE:-}
if [[ ${VERBOSE} != "" ]]; then
    set -x
fi

SYSTEM=${YDB_INSTALL_TEST_SYSTEM:-$(uname -s)} # $(uname -o) is not supported on macOS for example.
MACHINE=${YDB_INSTALL_TEST_MACHINE:-$(uname -m)}

GOOS=""
GOARCH=""
YDB_BIN="ydb"
SHELL_NAME=$(basename "${SHELL}")

CONTACT_SUPPORT_MESSAGE="If you think that this should not be, contact support and attach this message.
System info: $(uname -a)"

case ${SYSTEM} in
    Linux | GNU/Linux)
        GOOS="linux"
        ;;
#    Darwin)
#        GOOS="darwin"
#        ;;
#    CYGWIN* | MINGW* | MSYS* | Windows_NT | WindowsNT )
#        GOOS="windows"
#        YDB_BIN="ydb.exe"
#        ;;
     *)
        printf "'%s' system is not supported.\\n" "${SYSTEM}"
        exit 1
          ;;
esac

case ${MACHINE} in
    x86_64 | amd64 | i686-64)
        GOARCH="amd64"
        ;;
#    arm64)
#        GOARCH="arm64"
#        ;;
     *)
        printf "'%s' machines are not supported .\\n%s" "${MACHINE}"
        exit 1
          ;;
esac

DEFAULT_INSTALL_PATH=$PWD
YDB_INSTALL_PATH="${DEFAULT_INSTALL_PATH}"

while getopts "hi:r:na" opt ; do
    case "$opt" in
        i)
            YDB_INSTALL_PATH="${OPTARG}"
            ;;
        h)
            echo "Usage: install [options...]"
            echo "Options:"
            echo " -i [INSTALL_DIR]    Installs to specified dir, pwd/ydbd by default"
            echo " -h                  Prints help."
            exit 0
            ;;
    esac
done

YDB_STORAGE_URL="${YDB_STORAGE_URL:-"https://binaries.ydb.tech"}"
YDB_VERSION="$(curl -s "${YDB_STORAGE_URL}/release/current" | tr -d [:space:])"

echo "YDB version: $YDB_VERSION"
echo "Preparing temporary directory"

# Download to temp dir, check that executable is healthy, only then move to install path.
# That prevents partial download in case of download error or cancel.
TMPDIR="${TMPDIR:-/tmp}"
TMP_INSTALL_PATH=$(mktemp -d "${TMPDIR}/ydbd-install.XXXXXXXXX")
function cleanup {
    rm -rf "${TMP_INSTALL_PATH}"
}
trap cleanup EXIT

mkdir -p "${TMP_INSTALL_PATH}/ydbd"

echo "Downloading and extracting binary archive to temporary directory"

# Download and show progress.
curl "${YDB_STORAGE_URL}/release/${YDB_VERSION}/ydbd-${YDB_VERSION}-${GOOS}-${GOARCH}.tar.gz" | tar -xz --strip-components=1 -C "${TMP_INSTALL_PATH}/ydbd"

echo "Downloading and extracting scripts and configs to temporary directory"
curl "${YDB_STORAGE_URL}/local_scripts/${GOOS}.tar.gz" | tar -xz -C "${TMP_INSTALL_PATH}"

echo "Preparing target directory ${YDB_INSTALL_PATH}"
mkdir -p "${YDB_INSTALL_PATH}"
mkdir -p "${YDB_INSTALL_PATH}/logs"

YDB="${YDB_INSTALL_PATH}"

echo "Moving files from temporary to target directory"

mv -f "${TMP_INSTALL_PATH}"/* "${YDB}"

echo "
Installation completed successfully.

You may run 'start.sh ram' or 'start.sh disk' in the ${YDB_INSTALL_PATH} directory to start server/database, stop.sh to stop.
"

