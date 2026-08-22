#!/usr/bin/env bash
set -eu
ya make ../../tools/langver_dump
../../tools/langver_dump/langver_dump | jq > langver.json

