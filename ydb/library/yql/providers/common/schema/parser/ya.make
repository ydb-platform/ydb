LIBRARY()

SRCS(
    yql_type_parser.cpp
)

PEERDIR(
    ydb/library/yql/parser/pg_catalog
    library/cpp/yson/node
    library/cpp/yson
)

END()
