#!/usr/bin/env bash
set -eux
BASEDIR="$(dirname $(readlink -f $0))"
PGM=$(awk '/PROGRAM\((.*)\)/{ print substr($1, 9, length($1) - 9) }' ya.make)
yag make --build relwithdebinfo -DDUMP_LINKER_MAP
du -s -BM $(readlink $PGM)
yag tool bloat --input ./$PGM --linker-map ./$PGM.map.lld --save-json ./$PGM.json
$BASEDIR/parse_bloat.py ./$PGM

