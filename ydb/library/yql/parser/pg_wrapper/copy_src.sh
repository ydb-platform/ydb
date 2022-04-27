#!/usr/bin/env bash
set -eux
rm -f ../../../../../contrib/libs/postgresql/libpostgres.a
ya make ../../../../../contrib/libs/postgresql --checkout
python3 copy_src.py
for i in $(ls patches/); do (cd postgresql && patch -p1 < ../patches/$i); done
