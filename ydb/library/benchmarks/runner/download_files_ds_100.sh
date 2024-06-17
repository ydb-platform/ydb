#!/usr/bin/env bash
set -eux
if [ ! -d tpc/ds/100 ]; then
mkdir -p tpc/ds/100
fi

b=`pwd`
cd tpc/ds/100

base=https://storage.yandexcloud.net/tpc/ds/s100/parquet

source $b/download_lib.sh
source $b/download_tpcds_tables.sh

