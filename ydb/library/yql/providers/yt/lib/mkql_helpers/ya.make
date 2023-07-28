LIBRARY()

SRCS(
    mkql_helpers.cpp
)

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/core
    ydb/library/yql/ast
    ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
