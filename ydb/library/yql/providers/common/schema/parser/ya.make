LIBRARY()

OWNER(g:yql g:yql_ydb_core)

SRCS(
    yql_type_parser.cpp
)

PEERDIR(
    library/cpp/yson/node
    library/cpp/yson
)

END()
