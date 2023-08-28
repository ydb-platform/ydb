LIBRARY()

SRCS(
    yql_schema_utils.cpp
)

PEERDIR(
    library/cpp/yson/node
    ydb/library/yql/utils
)

END()

RECURSE(
    expr
    mkql
    parser
    skiff
)
