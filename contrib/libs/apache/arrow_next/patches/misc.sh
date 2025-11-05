#!/bin/sh

sed -e 's|.*define XXH_INLINE_ALL.*||' \
    -i cpp/src/parquet/xxhasher.cc

cat << EOF > cpp/src/generated/ya.make
LIBRARY()

FLATC_FLAGS(--scoped-enums)

SRCS(
    Schema.fbs
)

END()
EOF
