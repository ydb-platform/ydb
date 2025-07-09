G_BENCHMARK(core_tablet_flat_benchmark)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
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
