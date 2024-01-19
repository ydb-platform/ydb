LIBRARY()

SRCS(
    yql_hash_builder.cpp
    yql_op_hash.cpp
)

PEERDIR(
    contrib/libs/openssl
    ydb/library/yql/ast
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
)

END()
