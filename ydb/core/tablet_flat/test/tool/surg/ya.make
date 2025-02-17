PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/core/tablet_flat
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

END()
