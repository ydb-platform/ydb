Y_BENCHMARK()

SRCS(codec_bench.cpp)

PEERDIR(
    contrib/libs/isa-l/erasure_code
    library/cpp/json
    ydb/core/erasure
)

END()
