#!/bin/sh

set -xue

find tests -type f | while read l; do
    sed -e 's|from tests |from __tests__ |' \
        -i ${l}
done
