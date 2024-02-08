#!/usr/bin/env bash
set -eux
echo copy data files...
cp ../../../../../contrib/libs/postgresql/src/include/catalog/*.dat postgresql/src/include/catalog/
cp ../../../../../contrib/libs/postgresql/src/backend/catalog/*.sql postgresql/src/backend/catalog/

rm -f ../../../../../contrib/libs/postgresql/libpostgres.a
ya make ../../../../../contrib/libs/postgresql --checkout

export LANG=ru_RU.UTF-8
python3 copy_src.py
for i in $(ls patches/); do (cd postgresql && patch -p1 < ../patches/$i); done
