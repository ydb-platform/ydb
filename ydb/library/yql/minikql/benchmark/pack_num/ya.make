Y_BENCHMARK()

ALLOCATOR(B)

PEERDIR(
    ydb/library/yql/minikql
    library/cpp/packedtypes
)

SRCS(
    pack.cpp
    pack_num_bench.cpp
)

END()
