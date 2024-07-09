#!/bin/bash

set -eu

echo "Start script"

rm -rf /test-result 2> /dev/null || true

mkdir -p /exchange
mkdir -p /test-result/raw

if [ -e /exchange/sources ]; then
    echo "Skip prepare sources, because it is exist"
else
    echo "Copy sources"
    mkdir -p /exchange/sources
    cp -R /project/sources/. /exchange/sources
    chmod -R a+rw /exchange/sources
fi

cd /project/sources/

export YDB_PG_TESTNAME="${YDB_PG_TESTNAME:-}"  # set YDB_PG_TESTNAME to empty string if it not set

if [ -n "${YDB_PG_TESTNAME:-}" ]; then
    SKIP_TESTS="^\$"
fi

echo "Run test: '$YDB_PG_TESTNAME'"

echo "Start test"

# PQTEST_BINARY_PARAMETERS=no go test -test.skip="$SKIP_TESTS"

export SKIP_TESTS

PQTEST_BINARY_PARAMETERS=no /go-run-separate-tests.bash

sed -e 's|classname=""|classname="golang-lib-pq"|' -i /test-result/raw/result.xml

if [ -n "${YDB_PG_TESTNAME:-}" ]; then
    cat /test-result/raw/result.txt
fi

chmod -R a+rw /test-result