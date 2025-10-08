LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    yql_schema_utils.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/utils
)

END()

RECURSE(
    expr
    mkql
    parser
    skiff
)
