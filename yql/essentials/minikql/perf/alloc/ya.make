PROGRAM()

PEERDIR(
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    alloc.cpp
)

YQL_LAST_ABI_VERSION()

END()
