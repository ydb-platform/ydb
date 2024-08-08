UNITTEST_FOR(ydb/library/yql/parser/pg_wrapper)

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

INCLUDE(../cflags.inc)

SRCS(
    arrow_ut.cpp
    codegen_ut.cpp
    error_ut.cpp
    memory_ut.cpp
    pack_ut.cpp
    parser_ut.cpp
    proc_ut.cpp
    sort_ut.cpp
    type_cache_ut.cpp
    ../../../minikql/comp_nodes/ut/mkql_test_factory.cpp
)


ADDINCL(
    ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    ydb/library/yql/minikql/arrow
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/minikql/codegen/llvm14
    library/cpp/resource
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
