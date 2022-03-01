#!/usr/bin/env bash
set -eu

echo -n "Building library: "
ya make || exit $?

echo -n "Checking static variables: "

data=$(objdump libyql-parser-pg_query_wrapper.a -t | grep -E "\.data\.|\.bss\." | \
grep -v -E "progname|pg_popcount32|pg_popcount64|pg_comp_crc32c|TMkqlPgAdapter|_ZN4NYqlL10GlobalInitE|BlockSig|StartupBlockSig|UnBlockSig" | \
grep -v -E "on_proc_exit_index|on_shmem_exit_index|before_shmem_exit_index")

if [ ${#data} -eq 0 ]; then
    echo "OK";
    exit 0;
fi

cnt=$(echo "$data" | wc -l)

echo "***GOT $cnt UNEXPECTED SYMBOLS***"
echo "$data"
exit 1;
