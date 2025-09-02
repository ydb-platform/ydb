LIBRARY()

YQL_ABI_VERSION(
    2
    23
    0
)

SRCS(
    compress_base_udf.cpp
)

PEERDIR(
    yql/essentials/public/udf
    contrib/libs/snappy
    library/cpp/streams/brotli
    library/cpp/streams/bzip2
    library/cpp/streams/lzma
    library/cpp/streams/xz
    library/cpp/streams/zstd
)

END()
