YQL_LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/core/base
    ydb/library/actors/core
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
)

END()

RECURSE(
    initializer
    optimization
    secret
)
