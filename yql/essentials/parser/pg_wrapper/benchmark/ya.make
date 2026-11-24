G_BENCHMARK()

TIMEOUT(600)

SIZE(MEDIUM)

INCLUDE(../cflags.inc)

INCLUDE(../pg_include_dirs.inc)

SRCS(
    codegen_bench.cpp
    error_bench.cpp
)

ADDINCL(
    yql/essentials/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    yql/essentials/parser/pg_wrapper
    yql/essentials/minikql/arrow
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/parser/pg_catalog
    yql/essentials/minikql/codegen/llvm16
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

IF (OS_WINDOWS OR SANITIZER_TYPE OR BUILD_TYPE == "DEBUG")
    CFLAGS(
        -DUSE_SLOW_PG_KERNELS
    )
ENDIF()

END()
