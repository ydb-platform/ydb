LIBRARY()

SRCS(
    propose.cpp
    types.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/public/lib/scheme_types
    ydb/services/sql/parse
)

END()
