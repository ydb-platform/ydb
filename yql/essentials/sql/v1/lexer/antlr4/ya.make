LIBRARY()

PEERDIR(
    yql/essentials/parser/lexer_common
    yql/essentials/public/issue
    yql/essentials/parser/proto_ast/collect_issues
    yql/essentials/parser/proto_ast/gen/v1_antlr4
)

SRCS(
    lexer.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    classifieds/vsdwh/tools/ci/jobs/yql_udf/parser
    cml/util/yql_split_code/lib
    extsearch/geo/tools/yql_formatter
    logos/projects/ads/mldwh/querygen
    toloka/analytics/programs/yql_parser
)

END()
