#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
TARGET="ydb/tools/ssh_ya"
BIN_NAME="ssh_ya"

choose_default_install_dir() {
    if [[ -d "$HOME/bin" ]] || [[ ":$PATH:" == *":$HOME/bin:"* ]]; then
        printf '%s\n' "$HOME/bin"
        return
    fi

    if [[ -d "$HOME/.local/bin" ]] || [[ ":$PATH:" == *":$HOME/.local/bin:"* ]]; then
        printf '%s\n' "$HOME/.local/bin"
        return
    fi

    printf '%s\n' "$HOME/bin"
}

INSTALL_DIR=$(choose_default_install_dir)

usage() {
    cat <<EOF
Usage: $(basename "$0") [--dir INSTALL_DIR]

Builds $BIN_NAME via ya and installs it into INSTALL_DIR.

Options:
  --dir DIR   Install into DIR. Default: $INSTALL_DIR
  -h, --help  Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dir)
            if [[ $# -lt 2 ]]; then
                echo "--dir requires a value" >&2
                exit 2
            fi
            INSTALL_DIR=$2
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

TMPDIR_BASE=${TMPDIR:-/tmp}
BUILD_DIR=$(mktemp -d "$TMPDIR_BASE/ssh_ya-build.XXXXXX")
cleanup() {
    rm -rf "$BUILD_DIR"
}
trap cleanup EXIT

echo "Building $BIN_NAME into $BUILD_DIR"
(cd "$REPO_ROOT" && ./ya make --output "$BUILD_DIR" "$TARGET")

BIN_PATH=$(find "$BUILD_DIR" -type f -name "$BIN_NAME" -perm -u+x | head -n 1)
if [[ -z "$BIN_PATH" ]]; then
    echo "Built binary $BIN_NAME was not found under $BUILD_DIR" >&2
    exit 1
fi

mkdir -p "$INSTALL_DIR"

if command -v install >/dev/null 2>&1; then
    install -m 0755 "$BIN_PATH" "$INSTALL_DIR/$BIN_NAME"
else
    cp "$BIN_PATH" "$INSTALL_DIR/$BIN_NAME"
    chmod 0755 "$INSTALL_DIR/$BIN_NAME"
fi

echo "Installed $BIN_NAME to $INSTALL_DIR/$BIN_NAME"

if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
    echo "Warning: $INSTALL_DIR is not in PATH" >&2
fi
