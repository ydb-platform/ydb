LIBRARY()

SRCS(
    database_config_audit.cpp
)

PEERDIR(
    ydb/core/kqp/common/simple
    yql/essentials/sql/settings
    yql/essentials/sql/v1/proto_parser
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
)

END()

RECURSE_FOR_TESTS(
    ut
)
