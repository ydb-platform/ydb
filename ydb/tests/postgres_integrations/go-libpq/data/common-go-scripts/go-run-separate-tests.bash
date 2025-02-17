#!/bin/bash

set -eu

ONE_TEST_TIMEOUT=5s
TEST_BINARY=./test.binary

echo "Get test list"
TESTS=$($TEST_BINARY --test.list "^Test" | sort)


echo "Shell $SHELL"

rm -f /test-result/raw/result.txt
for TEST_NAME in $TESTS; do
    echo -n "Test: $TEST_NAME "
    if echo "$TEST_NAME" | grep -Eq "$YDB_PG_TESTFILTER"; then
        echo start
    else
        echo skip
        continue
    fi
    CMD="$TEST_BINARY --test.run '^$TEST_NAME\$' --test.v --test.timeout='$ONE_TEST_TIMEOUT'"
    echo "$CMD"
    bash -c "$CMD" >> /test-result/raw/result.txt 2>&1 || true
done

go-junit-report < /test-result/raw/result.txt > /test-result/raw/result.xml
