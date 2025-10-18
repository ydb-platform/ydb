UNITTEST_FOR(ydb/library/yql/dq/comp_nodes/hash_join_utils)

IF (SANITIZER_TYPE OR NOT OPENSOURCE)
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()


SRCS(
    accumulator_ut.cpp
    block_layout_converter_ut.cpp
    hash_table_ut.cpp
    packed_tuple_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
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