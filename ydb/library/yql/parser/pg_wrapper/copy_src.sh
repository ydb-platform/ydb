#!/usr/bin/env bash
set -eux
rm -f ../../../../../contrib/libs/postgresql/libpostgres.a
ya make ../../../../../contrib/libs/postgresql
python3 copy_src.py
