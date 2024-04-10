#!/usr/bin/env bash
set -eux
if [ ! -d tpc/h/10 ]; then
mkdir -p tpc/h/10
fi

b=`pwd`
cd tpc/h/10

base=https://storage.yandexcloud.net/tpc/h/s10/parquet

source $b/download_lib.sh
source $b/download_tables.sh

