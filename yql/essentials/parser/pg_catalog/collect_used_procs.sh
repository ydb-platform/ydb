#!/usr/bin/env bash
set -eux
rm -rf tmp
mkdir -p tmp
yag make --build=relwithdebinfo -tA -C ut -C ../pg_wrapper/ut -C ../pg_wrapper/test -C ../../sql/pg/ut --test-env=YQL_EXPORT_PG_FUNCTIONS_DIR=$(realpath ./tmp)
cat tmp/* | sort | uniq > used_procs.h
rm -rf tmp

