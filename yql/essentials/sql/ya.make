LIBRARY()

PEERDIR(
    library/cpp/deprecated/split
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/sql/settings
    yql/essentials/sql/v0
    yql/essentials/sql/v0/lexer
    yql/essentials/sql/v1
    yql/essentials/sql/v1/format
    yql/essentials/sql/v1/lexer
    yql/essentials/sql/v1/proto_parser
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
