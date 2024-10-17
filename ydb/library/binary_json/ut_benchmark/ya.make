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
    ydb/library/binary_json
    ydb/library/yql/minikql/dom
    ydb/library/yql/minikql/invoke_builtins/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/core/issue/protos
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
