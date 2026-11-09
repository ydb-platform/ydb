#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
    --shell)
        shift
        exec /bin/bash "$@"
        ;;
    --check)
        uname -m
        id
        for limit in memory.max memory.swap.max cpu.max; do
            if [[ -r "/sys/fs/cgroup/$limit" ]]; then
                printf '%s: %s\n' "$limit" "$(cat "/sys/fs/cgroup/$limit")"
            fi
        done
        for path in /out /home/builder/.ya; do
            if [[ ! -w "$path" ]]; then
                printf 'Not writable: %s; check YDB_BUILD_UID/GID and host mount permissions.\n' "$path" >&2
                exit 1
            fi
        done
        df -h /home/builder/.ya /out
        df -i /home/builder/.ya /out
        printf 'Source: /src; results: /out; cache: /home/builder/.ya\n'
        exit 0
        ;;
esac

if [[ $# -eq 0 ]]; then
    printf 'Pass a ya make target, or use --shell / --check.\n' >&2
    exit 2
fi

exec ./ya make --build "${YDB_BUILD_TYPE:-relwithdebinfo}" \
    --memory-limit="${YA_MEMORY_LIMIT:-20G}" --no-src-links --output=/out "$@"
