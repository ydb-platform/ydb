PROGRAM()

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    alloc.cpp
)

YQL_LAST_ABI_VERSION()

END()
