#!/usr/bin/env bash
set -eu
ya make
cnt=$(objdump libyql-parser-pg_query_wrapper.a -t | grep -E "\.data\.|\.bss\." | \
grep -v -E "progname|pg_popcount32|pg_popcount64|pg_comp_crc32c|_ZN4NYqlL10GlobalInitE|BlockSig|StartupBlockSig|UnBlockSig" | \
grep -v -E "on_proc_exit_index|on_shmem_exit_index|before_shmem_exit_index" | wc -l)
if [ $cnt -eq 0 ]; then
    echo "***PASSED***"
else
    echo "***GOT $cnt UNEXPECTED SYMBOLS***"
    exit 1
fi
