#!/usr/bin/env bash
set -eu
ya make ../../tools/sql_functions_dump
../../tools/sql_functions_dump/sql_functions_dump | jq -c > sql_functions.json
