LIBRARY()

SRCS(
    GLOBAL behaviour.cpp
    fetcher.cpp
    initializer.cpp
    manager.cpp
    object.cpp
    snapshot.cpp
)

PEERDIR(
    ydb/services/metadata/abstract
    ydb/services/metadata/initializer
    ydb/services/metadata/manager
)

YQL_LAST_ABI_VERSION()

END()