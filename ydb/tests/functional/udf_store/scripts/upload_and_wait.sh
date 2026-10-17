#!/usr/bin/env bash
# Upload a WASM UDF/LIBRARY via `ydb experimental udf` and poll until compile is ready.
#
# Usage:
#   upload_and_wait.sh --file path/to/libwasm-sdk.so --manifest path/to/sdk.manifest.json
#   upload_and_wait.sh --file path/to/mod.so --manifest path/to/manifest.json
#   upload_and_wait.sh --package path/to/module.tar.gz
#   upload_and_wait.sh --wait-only --name Md5 --uid <uid>   # poll existing upload
#
# Env (optional):
#   YDB_BIN       path to ydb CLI (default: ydb on PATH)
#   YDB_ENDPOINT  default grpc://localhost:31011
#   YDB_DATABASE  default /Root/test
#   TIMEOUT_SEC   default 180
#   POLL_SEC      default 2
#   SETTLE_SEC    sleep after ready before exit (default 0; optional metadata refresh delay)
set -euo pipefail

ENDPOINT="${YDB_ENDPOINT:-grpc://localhost:31011}"
DATABASE="${YDB_DATABASE:-/Root/test}"
TIMEOUT_SEC="${TIMEOUT_SEC:-180}"
POLL_SEC="${POLL_SEC:-2}"
SETTLE_SEC="${SETTLE_SEC:-0}"

NAME=""
FILE=""
MANIFEST=""
PACKAGE=""
UID_EXPECT=""
WAIT_ONLY=0
EXTRA_UPLOAD_ARGS=()

usage() {
    sed -n '2,16p' "$0" | sed 's/^# \?//'
    exit "${1:-0}"
}

die() { echo "error: $*" >&2; exit 1; }

resolve_ydb_bin() {
    if [[ -n "${YDB_BIN:-}" ]]; then
        [[ -x "$YDB_BIN" ]] || die "YDB_BIN is not executable: $YDB_BIN"
        echo "$YDB_BIN"
        return
    fi
    if command -v ydb >/dev/null 2>&1 && ydb experimental udf --help >/dev/null 2>&1; then
        command -v ydb
        return
    fi
    die "no ydb CLI with 'experimental udf' subcommand found; set YDB_BIN to the built ydb/apps/ydb/experimental/ydb/ydb"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage 0 ;;
        --name) NAME="$2"; shift 2 ;;
        --file|-f) FILE="$2"; shift 2 ;;
        --manifest) MANIFEST="$2"; shift 2 ;;
        --package) PACKAGE="$2"; shift 2 ;;
        --uid) UID_EXPECT="$2"; shift 2 ;;
        --wait-only) WAIT_ONLY=1; shift ;;
        --endpoint|-e) ENDPOINT="$2"; shift 2 ;;
        --database|-d) DATABASE="$2"; shift 2 ;;
        --timeout) TIMEOUT_SEC="$2"; shift 2 ;;
        --poll) POLL_SEC="$2"; shift 2 ;;
        --settle) SETTLE_SEC="$2"; shift 2 ;;
        --create-only|--replace-only)
            EXTRA_UPLOAD_ARGS+=("$1"); shift ;;
        --write-mode|--expected-uid|--expected-md5)
            EXTRA_UPLOAD_ARGS+=("$1" "$2"); shift 2 ;;
        *) die "unknown arg: $1 (see --help)" ;;
    esac
done

YDB_BIN="$(resolve_ydb_bin)"
YDB=("$YDB_BIN" -e "$ENDPOINT" -d "$DATABASE")

need_jq() { command -v jq >/dev/null 2>&1 || die "jq is required"; }
need_jq

upload_module() {
    local -a args=(experimental udf upload)
    if [[ -n "$PACKAGE" ]]; then
        [[ -z "$FILE" && -z "$MANIFEST" ]] || die "--package cannot be combined with --file or --manifest"
        [[ -f "$PACKAGE" ]] || die "package not found: $PACKAGE"
        args+=(--package "$PACKAGE")
    else
        [[ -n "$FILE" ]] || die "--file is required"
        [[ -f "$FILE" ]] || die "file not found: $FILE"
        [[ -n "$MANIFEST" ]] || die "--manifest is required"
        [[ -f "$MANIFEST" ]] || die "manifest not found: $MANIFEST"
        args+=(--file "$FILE" --manifest "$MANIFEST")
    fi
    args+=(--format json)
    if (( ${#EXTRA_UPLOAD_ARGS[@]} )); then
        args+=("${EXTRA_UPLOAD_ARGS[@]}")
    fi

    echo "+ ${YDB[*]} ${args[*]}" >&2
    local out
    out=$("${YDB[@]}" "${args[@]}")
    echo "$out" >&2
    UID_EXPECT=$(echo "$out" | jq -r .uid)
    NAME=$(echo "$out" | jq -r .name)
    [[ -n "$UID_EXPECT" && "$UID_EXPECT" != null ]] || die "upload returned no uid: $out"
    [[ -n "$NAME" && "$NAME" != null ]] || die "upload returned no name: $out"
    echo "uploaded name=$NAME uid=$UID_EXPECT" >&2
}

module_ready() {
    local desc="$1"
    local cur
    cur=$(echo "$desc" | jq -r .module.uid)
    [[ "$cur" == "$UID_EXPECT" ]] || {
        echo "uid mismatch: want=$UID_EXPECT got=$cur" >&2
        return 2
    }
    [[ $(echo "$desc" | jq -r .module.module_kind) == wasm ]] || return 3
    echo "$desc" | jq -c '{uid:.module.uid, platforms:.platforms}' >&2
    if echo "$desc" | jq -e 'any(.platforms[]; .status == "failed")' >/dev/null; then
        return 3
    fi
    echo "$desc" | jq -e '(.platforms | length) > 0 and all(.platforms[]; .status == "ready")' >/dev/null

}

wait_ready() {
    [[ -n "$NAME" ]] || die "--name is required for wait"
    [[ -n "$UID_EXPECT" ]] || die "--uid is required for wait (or upload first)"

    local deadline=$((SECONDS + TIMEOUT_SEC))
    local desc rc remaining
    while (( SECONDS < deadline )); do
        remaining=$((deadline - SECONDS))
        desc=$("${YDB[@]}" experimental udf describe \
            --name "$NAME" --format json --timeout "${remaining}s")
        (( SECONDS < deadline )) ||
            die "timeout ${TIMEOUT_SEC}s waiting for name=$NAME uid=$UID_EXPECT"
        set +e
        module_ready "$desc"
        rc=$?
        set -e
        case "$rc" in
            0)
                echo "ready name=$NAME uid=$UID_EXPECT" >&2
                if (( SETTLE_SEC > 0 )); then
                    echo "settle ${SETTLE_SEC}s for metadata refresh" >&2
                    sleep "$SETTLE_SEC"
                fi
                echo "$desc"
                return 0
                ;;
            2) die "describe uid mismatch for name=$NAME" ;;
            3) die "compile failed for name=$NAME uid=$UID_EXPECT: $desc" ;;
        esac
        sleep "$POLL_SEC"
    done
    die "timeout ${TIMEOUT_SEC}s waiting for name=$NAME uid=$UID_EXPECT"
}

if (( WAIT_ONLY )); then
    wait_ready
else
    upload_module
    wait_ready
fi
