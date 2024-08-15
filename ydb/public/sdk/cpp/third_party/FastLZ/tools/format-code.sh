#!/usr/bin/env bash

cwd=$(pwd)
clang-format-6.0 -i --style='{BasedOnStyle: "google", ColumnLimit: 120}' $cwd/*.h $cwd/*.c $cwd/tests/*.c $cwd/examples/*.c
