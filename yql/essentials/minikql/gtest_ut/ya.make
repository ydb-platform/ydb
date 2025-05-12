GTEST()

PEERDIR(
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    contrib/libs/apache/arrow
    yql/essentials/sql/pg_dummy
)

SRC(
    allocator_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
