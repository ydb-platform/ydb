#!/bin/sh

set -xue

find . -type f | while read l; do
    sed -e 's|namespace arrow|namespace arrow20|' \
        -e 's|namespace parquet::arrow|namespace parquet::arrow20|' \
        -e 's|arrow::|arrow20::|g' \
        -e 's|arrow_vendored::|arrow20_vendored::|g' \
        -e 's|namespace parquet|namespace parquet20|' \
        -e 's|parquet::|parquet20::|g' \
        -e 's|arrow_strptime|arrow20_strptime|g' \
        -i ${l}
done
