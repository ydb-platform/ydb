PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/engine
    ydb/core/scheme
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

END()
