Y_BENCHMARK()

ALLOCATOR(B)

OWNER(
    udovichenko-r
)

PEERDIR(
    ydb/library/yql/minikql
    library/cpp/packedtypes
    dict/dictutil
)

SRCS(
    pack_num_bench.cpp
)

END()
