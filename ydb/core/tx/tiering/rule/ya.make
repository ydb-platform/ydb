LIBRARY()

SRCS(
    manager.cpp
    object.cpp
    GLOBAL behaviour.cpp
    ss_checker.cpp
    checker.cpp
    GLOBAL ss_fetcher.cpp
    GLOBAL update.cpp
)

PEERDIR(
    ydb/services/metadata
    ydb/services/metadata/common
    ydb/services/metadata/initializer
    ydb/services/metadata/manager
    ydb/services/bg_tasks/abstract
    ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
