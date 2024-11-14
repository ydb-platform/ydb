#!/usr/bin/env bash
set -ex
~/.ya/tools/v3/531642148/bin/clang++ -emit-llvm -c 128_bit_ir.cpp -S -O2 -o 128_bit.ll
