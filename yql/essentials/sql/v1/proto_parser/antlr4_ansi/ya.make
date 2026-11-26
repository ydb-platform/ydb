LIBRARY()

PEERDIR(
    yql/essentials/utils
    yql/essentials/parser/proto_ast/antlr4
    yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4
)

SRCS(
    parser_cache.cpp
    proto_parser.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    cml/util/yql_validate_metric/lib
)

END()
