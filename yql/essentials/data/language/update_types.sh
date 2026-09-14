#!/usr/bin/env bash
set -eu
ya make ../../tools/types_dump
../../tools/types_dump/types_dump | jq > types.json
