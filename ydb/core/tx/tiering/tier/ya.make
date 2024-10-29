LIBRARY()

SRCS(
    manager.cpp
    object.cpp
    initializer.cpp
    checker.cpp
    snapshot.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/services/metadata/initializer
    ydb/services/metadata/abstract
    ydb/services/metadata/secret
    ydb/core/tx/schemeshard
    ydb/core/tx/tiering/fetcher
)

YQL_LAST_ABI_VERSION()

END()
