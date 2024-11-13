LIBRARY()

SRCS(
    yql_hash_builder.cpp
    yql_op_hash.cpp
)

PEERDIR(
    contrib/libs/openssl
    yql/essentials/ast
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/core/expr_nodes
)

END()
