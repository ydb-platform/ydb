#!/usr/bin/env bash
set -eux
if [ ! -d tpc/ds/10 ]; then
mkdir -p tpc/ds/10
fi

b=`pwd`
cd tpc/ds/10

base=https://storage.yandexcloud.net/tpc/ds/s10/parquet

source $b/download_lib.sh
source $b/download_tpcds_tables.sh

