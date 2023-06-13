LIBRARY()

SRCS(
    manager.cpp
    initializer.cpp
    GLOBAL behaviour.cpp
)

PEERDIR(
    ydb/services/metadata/initializer
    ydb/services/metadata/abstract
    ydb/core/kqp/gateway/actors
    ydb/core/kqp/gateway/behaviour/tablestore/operations
)

YQL_LAST_ABI_VERSION()

END()
