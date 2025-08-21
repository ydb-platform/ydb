UNITTEST()

TAG(ya:manual)

SRCS(
    array_builder_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/arrow
    ydb/library/yql/core/ut_common
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/minikql
    ydb/library/yql/minikql/invoke_builtins/llvm14
)

YQL_LAST_ABI_VERSION()

END()
