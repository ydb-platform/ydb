G_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)

IF (BENCHMARK_MAKE_LARGE_PART)
    CFLAGS(
        -DBENCHMARK_MAKE_LARGE_PART=1
    )
ENDIF()

SRCS(
    b_part.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/core/scheme
    ydb/core/tablet_flat/test/libs/exec
    ydb/core/tablet_flat/test/libs/table
    ydb/core/testlib/default
)

END()
