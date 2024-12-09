#!/bin/bash

# Used to find targets without RECURSE/RECURCE_FOR_TESTS/.. paths from ydb/ya.make
# TODO: run this check in CI 

set -e

# find all modules+tests from ydb/ya.make
./ya make -Gj0 -ttt ydb --build release -k --cache-tests --build-all > output_dump
cat output_dump | jq '.graph[]' | jq 'select( ."node-type"=="test")' | jq -r ".kv.path" | sort | uniq | sed -E 's/\/[^/]*$//g;/^null$/d'  | sort | uniq | { grep "^ydb" || true; } > tests.txt
cat output_dump | jq '.graph[]' | jq -r 'select( ."target_properties"."module_type" != null) | select( ( ."target_properties"."module_tag" // "-" | strings | contains("proto") ) | not ) | .target_properties.module_dir' | sort | uniq | { grep "^ydb" || true; } > modules.txt
cat modules.txt tests.txt | (echo 'RECURSE(';cat;echo ')') > ya.make


# find all modules+tests from generated ya.make, which contains all paths as RECURSE from previous step
./ya make -Gj0 -ttt . --build release -k --cache-tests --build-all  > output_dump2
cat output_dump2 | jq '.graph[]' | jq 'select( ."node-type"=="test")' | jq -r ".kv.path" | sort | uniq | sed -E 's/\/[^/]*$//g;/^null$/d'  | sort | uniq | { grep "^ydb" || true; } > tests2.txt
cat output_dump2 | jq '.graph[]' | jq -r 'select( ."target_properties"."module_type" != null) | select( ( ."target_properties"."module_tag" // "-" | strings | contains("proto") ) | not ) | .target_properties.module_dir' | sort | uniq | { grep "^ydb" || true; } > modules2.txt

# put all targets together
cat modules.txt tests.txt | sort | uniq > targets.txt
cat modules2.txt tests2.txt | sort | uniq > targets2.txt

# print targets which need to be fixes
comm -13 targets.txt targets2.txt
