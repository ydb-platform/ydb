G_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)
TIMEOUT(600)

IF (BENCHMARK_MAKE_LARGE_PART)
    CFLAGS(
        -DBENCHMARK_MAKE_LARGE_PART=1
    )
    TIMEOUT(1200)
ENDIF()

SRCS(
    write.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/types/binary_json
    yql/essentials/minikql/dom
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/core/issue/protos
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/devtools/large_on_multi_slots.inc)

END()
