G_BENCHMARK(library_aclib_benchmark)

TAG(ya:fat)
SIZE(LARGE)

SRCS(
    b_aclib.cpp
)

PEERDIR(
    ydb/library/aclib
)

END()
