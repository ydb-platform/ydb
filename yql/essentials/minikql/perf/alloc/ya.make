PROGRAM()

PEERDIR(
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

SRCS(
    alloc.cpp
)

YQL_LAST_ABI_VERSION()

END()
