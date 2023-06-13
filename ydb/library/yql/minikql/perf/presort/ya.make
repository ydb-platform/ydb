PROGRAM()

PEERDIR(
    library/cpp/presort
    ydb/library/yql/minikql
    ydb/library/yql/minikql/computation
    ydb/library/yql/minikql/invoke_builtins
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    presort.cpp
)

END()
