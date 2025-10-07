LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    yql_type_parser.cpp
)

PEERDIR(
    yql/essentials/parser/pg_catalog
    library/cpp/yson/node
    library/cpp/yson
)

END()
