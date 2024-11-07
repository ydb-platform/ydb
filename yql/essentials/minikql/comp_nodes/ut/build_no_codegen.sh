#!/usr/bin/env bash
set -ex

ya make -D MKQL_DISABLE_CODEGEN --target-platform=DEFAULT-LINUX-X86_64 --target-platform-flag=CFLAGS='-DMKQL_DISABLE_CODEGEN'
