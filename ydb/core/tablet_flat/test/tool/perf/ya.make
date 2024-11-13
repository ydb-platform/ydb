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
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

END()
