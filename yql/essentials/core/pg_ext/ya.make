LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    yql_pg_ext.cpp
)

PEERDIR(
    yql/essentials/protos
    yql/essentials/parser/pg_catalog
)

END()
