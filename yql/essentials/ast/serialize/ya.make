YQL_LIBRARY()

SRCS(
    yql_expr_serialize.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/issue
    yql/essentials/minikql
)

PEERDIR_FROM_POLICY(
    ALLOW
    statbox/qb3/ast_builder
    statbox/qb4/udfs/enum_based
    statbox/uv
    classifieds/vsdwh/tools/ci/jobs/yql_udf/parser
)

END()
