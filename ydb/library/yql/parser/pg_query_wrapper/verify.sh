#!/usr/bin/env bash
set -eu
ya make
err=0
objdump libyql-parser-pg_query_wrapper.a -t | grep -E "\.data\.|\.bss\." | \
grep -v -E "progname|pg_popcount32|pg_popcount64|pg_comp_crc32c|_ZN4NYqlL10GlobalInitE|BlockSig|StartupBlockSig|UnBlockSig" | \
grep -v -E "on_proc_exit_index|on_shmem_exit_index|before_shmem_exit_index" || err=$?
if [ $err -eq 1 ]; then
    echo "***PASSED***"
fi
if [ $err -ne 1 ]; then
    echo "***UNEXPECTED SYMBOLS***"
fi
