RECURSE(
    clickhouse/client
    compress_base
    datetime
    datetime2
    digest
    file
    histogram
    hyperloglog
    ip_base
    json
    json2
    math
    knn
    pire
    protobuf
    re2
    roaring
    set
    stat
    streaming
    string
    top
    topfreq
    unicode_base
    url_base
    yson2
)

IF (ARCH_X86_64)
    RECURSE(
        hyperscan
    )
ENDIF()

