LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
)

END()

RECURSE(
    initializer
    secret
)
