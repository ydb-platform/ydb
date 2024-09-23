#!/usr/bin/env bash
set -eux
if [ ! -d tpc/h/100 ]; then
mkdir -p tpc/h/100
fi

b=`pwd`
cd tpc/h/100

base=https://storage.yandexcloud.net/tpc/h/s100/parquet

source $b/download_lib.sh
source $b/download_tables.sh

