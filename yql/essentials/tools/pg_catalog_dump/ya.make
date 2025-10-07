PROGRAM(pg_catalog_dump)

ENABLE(YQL_STYLE_CPP)

SRCS(
    pg_catalog_dump.cpp
)

PEERDIR(
    library/cpp/getopt
    yql/essentials/parser/pg_catalog
    yql/essentials/utils/backtrace
    library/cpp/json
)

END()
