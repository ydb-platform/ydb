UNITTEST_FOR(yql/essentials/parser/pg_wrapper)

TIMEOUT(600)
SIZE(MEDIUM)

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
    yql/essentials/minikql/comp_nodes/ut/mkql_test_factory.cpp
)


ADDINCL(
    yql/essentials/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    yql/essentials/minikql/arrow
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/parser/pg_catalog
    yql/essentials/minikql/codegen/llvm14
    library/cpp/resource
)

YQL_LAST_ABI_VERSION()

IF (YQL_USE_PG_BC)
    CFLAGS(
        -DYQL_USE_PG_BC
    )
ENDIF()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
