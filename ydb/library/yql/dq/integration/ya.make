LIBRARY()

SRCS(
    yql_dq_integration.cpp
    yql_dq_optimization.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/yson
    ydb/library/yql/ast
    ydb/library/yql/core
    ydb/library/yql/dq/tasks
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    transform
)
