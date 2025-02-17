#!/usr/bin/env bash
set -u

echo -n "Building library: "
yag make -DBUILD_POSTGRES_ONLY || exit $?

echo -n "Checking static variables: "

data=$(objdump libyql-parser-pg_wrapper.a -t | grep -E "\.data\.|\.bss\." | \
	grep -v -E "\.data\.rel\.ro\." | \
        grep -v -E "pg_comp_crc32c|pg_popcount32|pg_popcount64" | \
        grep -v -E "BlockSig|StartupBlockSig|UnBlockSig"
)

if [ ${#data} -eq 0 ]; then
    echo "OK";
    exit 0;
fi

cnt=$(echo "$data" | wc -l)

echo "***GOT $cnt UNEXPECTED SYMBOLS***"
echo "$data"
exit 1;
