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
)

YQL_LAST_ABI_VERSION()

END()
