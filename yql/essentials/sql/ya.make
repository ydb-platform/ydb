LIBRARY()

PEERDIR(
    library/cpp/deprecated/split
    yql/essentials/ast
    yql/essentials/parser/proto_ast
    yql/essentials/parser/lexer_common
    yql/essentials/public/issue
    yql/essentials/sql/settings
    yql/essentials/utils
)

SRCS(
    cluster_mapping.cpp
    sql.cpp
)

END()

RECURSE(
    pg
    pg_dummy
    settings
    v0
    v1
)
