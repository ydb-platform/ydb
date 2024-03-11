LIBRARY()

SRCS(
    manager.cpp
    object.cpp
    GLOBAL behaviour.cpp
    initializer.cpp
    checker.cpp
    ss_checker.cpp
    GLOBAL ss_fetcher.cpp
)

PEERDIR(
    ydb/services/metadata/abstract
    ydb/services/metadata/common
    ydb/services/metadata/initializer
    ydb/services/metadata/manager
    ydb/services/bg_tasks/abstract
    ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
