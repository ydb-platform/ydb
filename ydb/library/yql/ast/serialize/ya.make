LIBRARY()

SRCS(
    yql_expr_serialize.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/core/issue
    ydb/library/yql/minikql
)

END()
