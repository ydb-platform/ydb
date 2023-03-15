PROGRAM()

SRCS(
    parse.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql
    ydb/library/yql/sql/v1
    ydb/library/yql/sql/pg_dummy
)

END()
