PROGRAM()

PEERDIR(
    library/cpp/presort
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    presort.cpp
)

END()
