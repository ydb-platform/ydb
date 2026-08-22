LIBRARY()

SRCS(
    discovery.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
)

END()
