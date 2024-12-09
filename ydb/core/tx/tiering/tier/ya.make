LIBRARY()

SRCS(
    manager.cpp
    object.cpp
    initializer.cpp
    checker.cpp
    GLOBAL behaviour.cpp
    ss_checker.cpp
)

PEERDIR(
    ydb/services/bg_tasks/abstract
    ydb/services/metadata/initializer
    ydb/services/metadata/abstract
    ydb/services/metadata/secret
    ydb/core/tx/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
