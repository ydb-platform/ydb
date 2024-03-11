PROGRAM()

PEERDIR(
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    param.cpp
)

END()
