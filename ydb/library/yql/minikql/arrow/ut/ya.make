UNITTEST_FOR(ydb/library/yql/minikql/arrow)

TIMEOUT(600)
SIZE(MEDIUM)

SRCS(
    mkql_functions_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()


END()
