#!/bin/sh

set -xue

find . -type f | while read l; do
    sed -e 's|namespace DB|namespace DB_CHDB|g' \
        -e 's|\<DB::|DB_CHDB::|g' \
        -e 's|namespace pcg_detail|namespace pcg_detail_CHDB|g' \
        -e 's|pcg_detail::|pcg_detail_CHDB::|g' \
        -e 's|namespace pcg_extras|namespace pcg_extras_CHDB|g' \
        -e 's|pcg_extras::|pcg_extras_CHDB::|g' \
        -e 's|namespace common|namespace common_CHDB|g' \
        -e 's|common::|common_CHDB::|g' \
        -e 's|namespace wide|namespace wide_CHDB|g' \
        -e 's|wide::|wide_CHDB::|g' \
        -i ${l}
done
