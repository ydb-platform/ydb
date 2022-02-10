#!/usr/bin/env bash
set -eux
mkdir -p contrib 
 
cp original/pg_query.h contrib/
cp original/.gitignore contrib/
cp original/CHANGELOG.md contrib/
cp original/LICENSE contrib/
cp original/Makefile contrib/
cp original/README.md contrib/

cp -R original/.github contrib/
cp -R original/examples contrib/
cp -R original/patches contrib/
cp -R original/protobuf contrib/
cp -R original/scripts contrib/
cp -R original/srcdata contrib/
cp -R original/test contrib/
cp -R original/testdata contrib/
cp -R original/tmp contrib/
cp -R original/vendor contrib/

cp -R original/src contrib/
cp patches/src/postgres/include/pg_config.h contrib/src/postgres/include
cp patches/src/postgres/include/pg_config_manual.h contrib/src/postgres/include
 
for i in $(find patches -name "*.patch" | sort); do 
    cat $i | patch -p0 
done 
