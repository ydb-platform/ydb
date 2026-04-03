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

DEFAULT_RC_PATH="${HOME}/.bashrc"

if [ "${SHELL_NAME}" != "bash" ]; then
    DEFAULT_RC_PATH="${HOME}/.${SHELL_NAME}rc"
elif [ "${SYSTEM}" = "Darwin" ]; then
    DEFAULT_RC_PATH="${HOME}/.bash_profile"
fi

DEFAULT_INSTALL_PATH="${HOME}/ydb"
YDB_CLI_INSTALL_PATH="${DEFAULT_INSTALL_PATH}"
RC_PATH=
NO_RC=
AUTO_RC=

while getopts "hi:r:na" opt ; do
    case "$opt" in
        i)
            YDB_CLI_INSTALL_PATH="${OPTARG}"
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
if ydb_full_version=$("${TMP_YDB}" version < /dev/null 2>&1); then
    echo "${ydb_full_version} installed successfully."
else
    echo "Installation failed. Please contact support. System info: $(uname -a)"
    exit 1
fi

mkdir -p "${YDB_CLI_INSTALL_PATH}/bin"
YDB_CLI="${YDB_CLI_INSTALL_PATH}/bin/${YDB_CLI_BIN}"
mv -f "${TMP_YDB}" "${YDB_CLI}"
mkdir -p "${YDB_CLI_INSTALL_PATH}/install"

COMPLETION_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/ydb"

function generate_completion_files() {
    mkdir -p "${COMPLETION_DIR}"

    if "${YDB_CLI}" config completion bash < /dev/null > "${COMPLETION_DIR}/completion.bash.inc" 2>/dev/null; then
        :
    else
        echo "Warning: failed to generate bash completion script"
        rm -f "${COMPLETION_DIR}/completion.bash.inc"
    fi

    local zsh_completion
    if zsh_completion=$("${YDB_CLI}" config completion zsh < /dev/null 2>/dev/null); then
        {
            printf '%s\n' 'if ! (( $+functions[compdef] )); then'
            printf '%s\n' '  autoload -Uz compinit'
            printf '%s\n' '  compinit'
            printf '%s\n' 'fi'
            printf '\n'
            printf '%s\n' "${zsh_completion}"
            printf '\n'
            printf '%s\n' 'compdef _ydb ydb'
        } > "${COMPLETION_DIR}/completion.zsh.inc"
    else
        echo "Warning: failed to generate zsh completion script"
    fi
}

function bash_completion_installed() {
    [ -f /usr/share/bash-completion/bash_completion ] && return 0
    [ -f /etc/bash_completion ] && return 0
    [ -f /opt/homebrew/etc/bash_completion ] && return 0
    [ -f /usr/local/etc/bash_completion ] && return 0
    return 1
}

function completion_already_in_rc() {
    local rc_file="$1"
    local inc_name="$2"
    [ -f "${rc_file}" ] && grep -qF "ydb/${inc_name}" "${rc_file}"
}

function print_bash_completion_warning() {
    local yellow="\033[33m"
    local reset="\033[0m"
    if ! bash_completion_installed; then
        printf "${yellow}%s${reset}\n" "Warning: bash-completion does not seem to be installed. Shell completion requires it."
    fi
}

function print_completion_instructions() {
    local green="\033[32m"
    local reset="\033[0m"
    local src_zsh='_ydb_comp="${XDG_DATA_HOME:-$HOME/.local/share}/ydb/completion.zsh.inc" && [ -f "$_ydb_comp" ] && source "$_ydb_comp"'
    local src_bash='_ydb_comp="${XDG_DATA_HOME:-$HOME/.local/share}/ydb/completion.bash.inc" && [ -f "$_ydb_comp" ] && source "$_ydb_comp"'
    case "${SHELL_NAME}" in
        zsh)
            if completion_already_in_rc "${HOME}/.zshrc" "completion.zsh.inc"; then
                return
            fi
            echo ""
            printf "${green}%s${reset}\n" "(!) To enable tab completion for commands and options, run:"
            echo ""
            echo "  echo '${src_zsh}' >> ~/.zshrc"
            echo ""
            ;;
        bash)
            if completion_already_in_rc "${HOME}/.bashrc" "completion.bash.inc"; then
                return
            fi
            echo ""
            printf "${green}%s${reset}\n" "(!) To enable tab completion for commands and options, run:"
            echo ""
            echo "  echo '${src_bash}' >> ~/.bashrc"
            echo ""
            print_bash_completion_warning
            ;;
        *)
            local show_bash=true
            local show_zsh=true
            if completion_already_in_rc "${HOME}/.bashrc" "completion.bash.inc"; then
                show_bash=false
            fi
            if completion_already_in_rc "${HOME}/.zshrc" "completion.zsh.inc"; then
                show_zsh=false
            fi
            if [ "${show_bash}" = "false" ] && [ "${show_zsh}" = "false" ]; then
                return
            fi
            echo ""
            printf "${green}%s${reset}\n" "(!) To enable tab completion for commands and options, run one of these:"
            echo ""
            if [ "${show_bash}" = "true" ]; then
                echo "  bash:"
                echo "    echo '${src_bash}' >> ~/.bashrc"
                print_bash_completion_warning
                echo ""
            fi
            if [ "${show_zsh}" = "true" ]; then
                echo "  zsh:"
                echo "    echo '${src_zsh}' >> ~/.zshrc"
            fi
            echo ""
            ;;
    esac
}

generate_completion_files
if [ -f "${COMPLETION_DIR}/completion.bash.inc" ] || [ -f "${COMPLETION_DIR}/completion.zsh.inc" ]; then
    print_completion_instructions
fi

case "${SHELL_NAME}" in
    bash | zsh)
        ;;
    *)
        echo ""
        echo "ydb is installed to ${YDB_CLI}"
        exit 0
        ;;
esac

YDB_CLI_BASH_PATH="${YDB_CLI_INSTALL_PATH}/path.bash.inc"

if [ "${SHELL_NAME}" = "bash" ]; then
    cat >"${YDB_CLI_BASH_PATH}" <<EOF
ydb_cli_dir="\$(cd "\$(dirname "\${BASH_SOURCE[0]}")" && pwd)"
bin_path="\${ydb_cli_dir}/bin"
export PATH="\${bin_path}:\${PATH}"
EOF
else
    cat >"${YDB_CLI_BASH_PATH}" <<EOF
ydb_cli_dir="\$(cd "\$(dirname "\${(%):-%N}")" && pwd)"
bin_path="\${ydb_cli_dir}/bin"
export PATH="\${bin_path}:\${PATH}"
EOF
fi

if [ "${NO_RC}" = "yes" ]; then
    exit 0
fi

function modify_rc() {
    if ! grep -Fq "if [ -f '${YDB_CLI_BASH_PATH}' ]; then source '${YDB_CLI_BASH_PATH}'; fi" "$1"; then
        cat >> "$1" <<EOF

# The next line updates PATH for YDB CLI.
if [ -f '${YDB_CLI_BASH_PATH}' ]; then source '${YDB_CLI_BASH_PATH}'; fi
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
    echo "Source '${YDB_CLI_BASH_PATH}' in your profile to add the command line directory to your \$PATH."
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
