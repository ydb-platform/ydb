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
    classifieds/vsdwh/tools/ci/jobs/yql_udf/parser
    extsearch/geo/tools/yql_formatter
    logos/projects/ads/mldwh/querygen
)

END()
