LIBRARY()

SRCS(
    manager.cpp
    object.cpp
    GLOBAL behaviour.cpp
    initializer.cpp
    checker.cpp
)

PEERDIR(
    ydb/services/metadata/abstract
    ydb/services/metadata/common
    ydb/services/metadata/initializer
    ydb/services/metadata/manager
    ydb/services/bg_tasks/abstract
)

YQL_LAST_ABI_VERSION()

END()
