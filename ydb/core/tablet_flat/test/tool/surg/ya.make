PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/core/tablet_flat
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/exception_policy
)

END()
