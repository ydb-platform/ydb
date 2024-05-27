PROGRAM(pg_catalog_dump)

SRCS(
    pg_catalog_dump.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/utils/backtrace
    library/cpp/json
)

END()
