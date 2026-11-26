YQL_LIBRARY()

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

PEERDIR_FROM_POLICY(
    ALLOW
    logos/projects/ads/mldwh/querygen
    toloka/analytics/programs/yql_parser
)

END()

RECURSE(
    pg
    pg_dummy
    settings
    v1
)
RECURSE_FOR_TESTS(
    ut
)
