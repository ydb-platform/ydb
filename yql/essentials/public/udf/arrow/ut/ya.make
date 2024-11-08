UNITTEST()

SRCS(
    array_builder_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/arrow
    yql/essentials/core/ut_common
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm14
)

YQL_LAST_ABI_VERSION()

END()
