#!/bin/bash
set -euo pipefail

# this is required because of the wildcard expansion. Passing protos/*.proto in CMD gets escaped -_-. So instead leaving
# off the [FILES] will put protos/*.proto in from here which will expand correctly.
args=("$@")
if [ "${#args[@]}" -lt 2 ]; then args+=(protos/*.proto); fi

exec protoc -I/usr/include -Iprotos --doc_out=/out "${args[@]}"
