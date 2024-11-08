G_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)

IF (BENCHMARK_MAKE_LARGE_PART)
    CFLAGS(
        -DBENCHMARK_MAKE_LARGE_PART=1
    )
ENDIF()

SRCS(
    write.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/types/binary_json
    yql/essentials/minikql/dom
    yql/essentials/minikql/invoke_builtins/llvm14
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/core/issue/protos
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
