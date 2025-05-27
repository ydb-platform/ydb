UNITTEST_FOR(yql/essentials/minikql/arrow)

TIMEOUT(600)
SIZE(MEDIUM)

SRCS(
    mkql_functions_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/minikql/invoke_builtins/llvm16
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()


END()
