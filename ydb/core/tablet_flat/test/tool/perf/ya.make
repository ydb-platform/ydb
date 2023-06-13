PROGRAM(table-perf)

SRCS(
    colons.cpp
    main.cpp
)

PEERDIR(
    ydb/core/tablet_flat/test/libs/table
    library/cpp/charset
    library/cpp/getopt
    ydb/core/tablet_flat
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/exception_policy
)

END()
