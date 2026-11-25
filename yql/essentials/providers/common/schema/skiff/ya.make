YQL_LIBRARY()

SRCS(
    yql_skiff_schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/public/udf
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/parser
)

PEERDIR_FROM_POLICY(
    ALLOW
    statbox/qb3/specs/skiff/helpers
)

END()
