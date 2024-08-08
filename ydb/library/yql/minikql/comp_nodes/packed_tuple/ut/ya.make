UNITTEST_FOR(ydb/library/yql/minikql/comp_nodes/packed_tuple)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

REQUIREMENTS(ram:32)

SRCS(
    packed_tuple_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/arrow
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

CFLAGS(
    -mprfchw
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()


END()
