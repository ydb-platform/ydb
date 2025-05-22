#!/usr/bin/env bash
set -eux
if [ ! -d tpc/ds/1 ]; then
mkdir -p tpc/ds/1
fi

b=`pwd`
cd tpc/ds/1

base=https://storage.yandexcloud.net/tpc/ds/s1/parquet

source $b/download_lib.sh
source $b/download_tpcds_tables.sh

