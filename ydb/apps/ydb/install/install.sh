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

CURRENT_OS=""
CURRENT_ARCH=""
YDB_CLI_BIN="ydb"
SHELL_NAME=$(basename "${SHELL}")

CONTACT_SUPPORT_MESSAGE="If you think that this should not be, contact support and attach this message.
System info: $(uname -a)"

SYSTEM=${YDB_CLI_INSTALL_TEST_SYSTEM:-$(uname -s)} # $(uname -o) is not supported on macOS for example.
case ${SYSTEM} in
    Linux | GNU/Linux)
        CURRENT_OS="linux"
        ;;
    Darwin)
        CURRENT_OS="darwin"
        ;;
    CYGWIN* | MINGW* | MSYS* | Windows_NT | WindowsNT )
        CURRENT_OS="windows"
        YDB_CLI_BIN="ydb.exe"
        ;;
     *)
        printf "'%s' system is not supported yet, or something is going wrong.\\n" "${SYSTEM}", "${CONTACT_SUPPORT_MESSAGE}"
        exit 1
          ;;
esac

MACHINE=${YDB_CLI_INSTALL_TEST_MACHINE:-$(uname -m)}
case ${MACHINE} in
    x86_64 | amd64 | i686-64)
        CURRENT_ARCH="amd64"
        ;;
    arm64 | aarch64)
        CURRENT_ARCH="arm64"
        ;;
     *)
        printf "'%s' machines are not supported yet, or something is going wrong.\\n%s" "${MACHINE}" "${CONTACT_SUPPORT_MESSAGE}"
        exit 1
          ;;
esac

CANONICAL_BIN_DIR="${HOME}/.local/bin"
CANONICAL_BIN_PATH="${CANONICAL_BIN_DIR}/${YDB_CLI_BIN}"
LEGACY_ROOT="${HOME}/ydb"
LEGACY_BIN_DIR="${LEGACY_ROOT}/bin"
LEGACY_PATH_HELPER="${LEGACY_ROOT}/path.bash.inc"
RC_PATH=

CURL_HELP="${YDB_CLI_TEST_CURL_HELP:-$(curl --help)}"
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

YDB_CLI_STORAGE_URL="${YDB_CLI_STORAGE_URL:-"https://storage.yandexcloud.net/yandexcloud-ydb"}"
YDB_CLI_VERSION="${YDB_CLI_VERSION:-$(curl_with_retry -s "${YDB_CLI_STORAGE_URL}/release/stable" | tr -d [:space:])}"

if [ ! -t 0 ]; then
    # stdin is not terminal - we're piped. Skip all interactivity.
    AUTO_RC=yes
fi

echo "Downloading ydb ${YDB_CLI_VERSION}"

# Download to temp dir, check that executable is healthy, only then move to install path.
# That prevents partial download in case of download error or cancel.
TMPDIR="${TMPDIR:-/tmp}"
TMP_INSTALL_PATH=$(mktemp -d "${TMPDIR}/ydb-install.XXXXXXXXX")
function cleanup {
    rm -rf "${TMP_INSTALL_PATH}"
}
trap cleanup EXIT

# Download and show progress.
TMP_YDB="${TMP_INSTALL_PATH}/${YDB_CLI_BIN}"
curl_with_retry "${YDB_CLI_STORAGE_URL}/release/${YDB_CLI_VERSION}/${CURRENT_OS}/${CURRENT_ARCH}/${YDB_CLI_BIN}" -o "${TMP_YDB}"

chmod +x "${TMP_YDB}"
# Check that all is ok, and print full version to stdout.
${TMP_YDB} version || echo "Installation failed. Please contact support. System info: $(uname -a)"

mkdir -p "${CANONICAL_BIN_DIR}"
mv -f "${TMP_YDB}" "${CANONICAL_BIN_PATH}"

case "${SHELL_NAME}" in
    bash | zsh)
        ;;
    *)
        echo "ydb is installed to ${CANONICAL_BIN_PATH}"
        exit 0
        ;;
esac

function remove_file_if_exists() {
    local target="$1"
    if [ -f "${target}" ]; then
        rm -f "${target}"
    fi
}

function is_dir_empty() {
    local dir="$1"
    if [ ! -d "${dir}" ]; then
        return 0
    fi
    if [ -z "$(ls -A "${dir}")" ]; then
        return 0
    fi
    return 1
}

function cleanup_legacy_rc_file() {
    local file="$1"
    if [ ! -f "${file}" ]; then
        return
    fi
    local tmp
    tmp=$(mktemp)
    # Strip legacy helper sourcing and direct ~/ydb/bin edits.
    sed -e '/ydb\/path\.bash\.inc/d' \
        -e '/ydb\/bin/d' \
        -e '/The next line updates PATH for YDB CLI/d' \
        "${file}" > "${tmp}"
    if ! cmp -s "${tmp}" "${file}"; then
        mv "${tmp}" "${file}"
        echo "Removed legacy YDB PATH settings from ${file}"
    else
        rm -f "${tmp}"
    fi
}

function append_local_bin_block() {
    local file="$1"
    mkdir -p "$(dirname "${file}")"
    touch "${file}"
    cleanup_legacy_rc_file "${file}"
    if grep -Fq ".local/bin" "${file}"; then
        echo "${HOME}/.local/bin already present in ${file}"
        return
    fi
    cat <<'EOF' >> "${file}"
# YDB CLI: append ~/.local/bin to PATH
if ! echo "${PATH}" | tr ':' '\n' | grep -Fq "${HOME}/.local/bin"; then
    export PATH="${HOME}/.local/bin:${PATH}"
fi
EOF
    echo "Updated PATH settings in ${file}"
}

function cleanup_legacy_environment() {
    # Remove old helper script and clean known rc/profile files.
    remove_file_if_exists "${LEGACY_PATH_HELPER}"
    cleanup_legacy_rc_file "${HOME}/.zprofile"
    cleanup_legacy_rc_file "${HOME}/.zshrc"
    cleanup_legacy_rc_file "${HOME}/.bash_profile"
    cleanup_legacy_rc_file "${HOME}/.bashrc"
    cleanup_legacy_rc_file "${HOME}/.profile"
    if is_dir_empty "${LEGACY_ROOT}"; then
        rmdir -p "${LEGACY_ROOT}" 2>/dev/null || true
    fi
}

function configure_zsh_profiles() {
    append_local_bin_block "${HOME}/.zprofile"
}

function configure_bash_profiles() {
    append_local_bin_block "${HOME}/.bash_profile"
    append_local_bin_block "${HOME}/.bashrc"
}

function configure_custom_profile() {
    local file="$1"
    append_local_bin_block "${file}"
}

function configure_default_profiles() {
    if [ "${SHELL_NAME}" = "zsh" ]; then
        configure_zsh_profiles
    else
        configure_bash_profiles
    fi
}

function describe_default_profiles() {
    # Show the user exactly which files will be touched if they keep defaults.
    if [ "${SHELL_NAME}" = "zsh" ]; then
        echo "${HOME}/.zprofile"
    else
        echo "${HOME}/.bash_profile and ${HOME}/.bashrc"
    fi
}

function ask_for_rc_path() {
    local defaults
    defaults=$(describe_default_profiles)
    # Allow users to override the target profile; pressing Enter keeps defaults.
    echo "Enter a profile file to update, or leave blank to modify ${defaults}:"
    printf "> "
    read filepath
    RC_PATH="${filepath}"
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

cleanup_legacy_environment

if [ "${AUTO_RC}" = "yes" ]; then
    configure_default_profiles
    exit 0
fi

echo -n "Modify profile to update your \$PATH ? [Y/n] "

if input_yes_no ; then
    ask_for_rc_path
    if [ -n "${RC_PATH}" ]; then
        configure_custom_profile "${RC_PATH}"
    else
        configure_default_profiles
    fi
else
    echo "Add '${HOME}/.local/bin' to your PATH in the appropriate shell profile to use 'ydb' without specifying the full path."
fi
