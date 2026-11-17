LIBRARY()

SRCS(
    checksum.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/core/tx/long_tx_service/public
    ydb/core/tx/scheme_cache
    ydb/library/query_actor
    ydb/public/api/protos
)

END()
