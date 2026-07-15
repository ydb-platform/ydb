G_BENCHMARK(library_aclib_benchmark)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
SIZE(LARGE)

SRCS(
    b_aclib.cpp
)

PEERDIR(
    ydb/library/aclib
)

END()
