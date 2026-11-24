YQL_LIBRARY()

SRCS(
    discovery.cpp
)

PEERDIR(
    library/cpp/digest/md5
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
)

END()
