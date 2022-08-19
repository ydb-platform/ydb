#!/usr/bin/env bash

# The MIT License (MIT)
#
# Copyright (c) 2020 YANDEX LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

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
    Darwin)
        GOOS="darwin"
        ;;
    CYGWIN* | MINGW* | MSYS* | Windows_NT | WindowsNT )
        GOOS="windows"
        YDB_BIN="ydb.exe"
        ;;
     *)
        printf "'%s' system is not supported yet, or something is going wrong.\\n" "${SYSTEM}", "${CONTACT_SUPPORT_MESSAGE}"
        exit 1
          ;;
esac

case ${MACHINE} in
    x86_64 | amd64 | i686-64)
        GOARCH="amd64"
        ;;
    arm64)
        GOARCH="arm64"
        ;;
     *)
        printf "'%s' machines are not supported yet, or something is going wrong.\\n%s" "${MACHINE}" "${CONTACT_SUPPORT_MESSAGE}"
        exit 1
          ;;
esac

DEFAULT_RC_PATH="${HOME}/.bashrc"

if [ "${SHELL_NAME}" != "bash" ]; then
    DEFAULT_RC_PATH="${HOME}/.${SHELL_NAME}rc"
elif [ "${SYSTEM}" = "Darwin" ]; then
    DEFAULT_RC_PATH="${HOME}/.bash_profile"
fi

DEFAULT_INSTALL_PATH="${HOME}/ydb"
YDB_INSTALL_PATH="${DEFAULT_INSTALL_PATH}"
RC_PATH=
NO_RC=
AUTO_RC=

while getopts "hi:r:na" opt ; do
    case "$opt" in
        i)
            YDB_INSTALL_PATH="${OPTARG}"
            ;;
        r)
            RC_PATH="${OPTARG}"
            ;;
        n)
            NO_RC=yes
            ;;
        a)
            AUTO_RC=yes
            ;;
        h)
            echo "Usage: install [options...]"
            echo "Options:"
            echo " -i [INSTALL_DIR]    Installs to specified dir."
            echo " -r [RC_FILE]        Automatically modify RC_FILE with PATH modification."
            echo " -n                  Don't modify rc file and don't ask about it."
            echo " -a                  Automatically modify default rc file with PATH modification."
            echo " -h                  Prints help."
            exit 0
            ;;
    esac
done

CURL_HELP="${YDB_TEST_CURL_HELP:-$(curl --help)}"
CURL_OPTIONS=("-fS")
function curl_has_option {
    echo "${CURL_HELP}" | grep -e "$@" > /dev/null
}
if curl_has_option "--retry"; then
    # Added in curl 7.12.3
    CURL_OPTIONS=("${CURL_OPTIONS[@]}" "--retry" "5" "--retry-delay" "0" "--retry-max-time" "120")
fi
if curl_has_option "--connect-timeout"; then
    # Added in curl 7.32.0.
    CURL_OPTIONS=("${CURL_OPTIONS[@]}" "--connect-timeout" "5" "--max-time" "300")
fi
if curl_has_option "--retry-connrefused"; then
    # Added in curl 7.52.0.
    CURL_OPTIONS=("${CURL_OPTIONS[@]}" "--retry-connrefused")
fi
function curl_with_retry {
    curl "${CURL_OPTIONS[@]}" "$@"
}

YDB_STORAGE_URL="${YDB_STORAGE_URL:-"https://storage.yandexcloud.net/yandexcloud-ydb"}"
YDB_VERSION="${YDB_VERSION:-$(curl_with_retry -s "${YDB_STORAGE_URL}/release/stable" | tr -d [:space:])}"

if [ ! -t 0 ]; then
    # stdin is not terminal - we're piped. Skip all interactivity.
    AUTO_RC=yes
fi

echo "Downloading ydb ${YDB_VERSION}"

# Download to temp dir, check that executable is healthy, only then move to install path.
# That prevents partial download in case of download error or cancel.
TMPDIR="${TMPDIR:-/tmp}"
TMP_INSTALL_PATH=$(mktemp -d "${TMPDIR}/ydb-install.XXXXXXXXX")
function cleanup {
    rm -rf "${TMP_INSTALL_PATH}"
}
trap cleanup EXIT

# Download and show progress.
TMP_YDB="${TMP_INSTALL_PATH}/${YDB_BIN}"
curl_with_retry "${YDB_STORAGE_URL}/release/${YDB_VERSION}/${GOOS}/${GOARCH}/${YDB_BIN}" -o "${TMP_YDB}"

chmod +x "${TMP_YDB}"
# Check that all is ok, and print full version to stdout.
${TMP_YDB} version || echo "Installation failed. Please contact support. System info: $(uname -a)"

mkdir -p "${YDB_INSTALL_PATH}/bin"
YDB="${YDB_INSTALL_PATH}/bin/${YDB_BIN}"
mv -f "${TMP_YDB}" "${YDB}"
mkdir -p "${YDB_INSTALL_PATH}/install"

case "${SHELL_NAME}" in
    bash | zsh)
        ;;
    *)
        echo "ydb is installed to ${YDB}"
        exit 0
        ;;
esac

YDB_BASH_PATH="${YDB_INSTALL_PATH}/path.bash.inc"

if [ "${SHELL_NAME}" = "bash" ]; then
    cat >"${YDB_BASH_PATH}" <<EOF
ydb_dir="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
bin_path="\${ydb_dir}/bin"
export PATH="\${bin_path}:\${PATH}"
EOF
else
    cat >"${YDB_BASH_PATH}" <<EOF
ydb_dir="\$(cd "\$(dirname "\${(%):-%N}")" && pwd)"
bin_path="\${ydb_dir}/bin"
export PATH="\${bin_path}:\${PATH}"
EOF
fi

if [ "${NO_RC}" = "yes" ]; then
    exit 0
fi

function modify_rc() {
    if ! grep -Fq "if [ -f '${YDB_BASH_PATH}' ]; then source '${YDB_BASH_PATH}'; fi" "$1"; then
        cat >> "$1" <<EOF

# The next line updates PATH for YDB CLI.
if [ -f '${YDB_BASH_PATH}' ]; then source '${YDB_BASH_PATH}'; fi
EOF
        echo ""
        echo "ydb PATH has been added to your '${1}' profile"
    fi

    echo "" >> "$1"
    echo "To complete installation, start a new shell (exec -l \$SHELL) or type 'source \"$1\"' in the current one"
}

function input_yes_no() {
    while read answer; do
        case "${answer}" in
        "Yes" | "y" | "yes" | "")
            return 0
            ;;
        "No" | "n" | "no")
            return 1
            ;;
        *)
            echo "Please enter 'y' or 'n': "
            ;;
        esac
    done
}

function ask_for_rc_path() {
    echo "Enter a path to an rc file to update, or leave blank to use"
    echo -n "[${DEFAULT_RC_PATH}]: "
    read filepath
    if [ "${filepath}" = "" ]; then
        filepath="${DEFAULT_RC_PATH}"
    fi
    RC_PATH="$filepath"
}

function print_rc_guide() {
    echo "Source '${YDB_BASH_PATH}' in your profile to add the command line directory to your \$PATH."
}

if [ "${RC_PATH}" != "" ] ; then
    modify_rc "${RC_PATH}"
    exit 0
fi

if [ "${AUTO_RC}" = "yes" ]; then
    modify_rc "${DEFAULT_RC_PATH}"
    exit 0
fi


echo -n "Modify profile to update your \$PATH ? [Y/n] "

if input_yes_no ; then
    ask_for_rc_path
    modify_rc "${RC_PATH}"
else
    print_rc_guide
fi
