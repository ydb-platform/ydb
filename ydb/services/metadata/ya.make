LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/services/metadata/abstract
    ydb/services/metadata/manager
)

END()

RECURSE(
    secret
    initializer
)