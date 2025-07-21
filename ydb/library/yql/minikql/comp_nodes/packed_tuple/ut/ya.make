UNITTEST_FOR(ydb/library/yql/minikql/comp_nodes/packed_tuple)

IF (SANITIZER_TYPE OR NOT OPENSOURCE)
    REQUIREMENTS(ram:32)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()


SRCS(
    packed_tuple_ut.cpp
    hash_table_ut.cpp
    neumann_hash_table_ut.cpp
    accumulator_ut.cpp
    cardinality_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

CFLAGS(
    -mavx2
    -mprfchw
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()


END()
