#!/usr/bin/env bash
set -eux
if [ ! -d tpc/h/1 ]; then
mkdir -p tpc/h/1
fi

b=`pwd`
cd tpc/h/1

base=https://storage.yandexcloud.net/tpc/h/s1/parquet

source $b/download_lib.sh
source $b/download_tables.sh

