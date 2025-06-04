GTEST()

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRC(
    sanitizer_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
