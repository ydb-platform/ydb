#!/bin/sh

set -xue

find . -type f | grep -v patches | grep '\.h' | while read l; do
    echo '#pragma clang system_header' > _
    cat ${l} >> _
    mv _ ${l}
done
