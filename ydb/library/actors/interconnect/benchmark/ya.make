G_BENCHMARK(interconnect_benchmark)

SIZE(SMALL)

# Keeps the ~110-case matrix inside the SMALL test budget. Run the binary directly without this option
# (or with a larger --benchmark_min_time) when comparing numbers before and after a change.
BENCHMARK_OPTS(--benchmark_min_time=0.05s)

SRCS(
    b_checksum.cpp
    b_v2_event_queue.cpp
    b_v2_event_serializer.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/actors/protos
    contrib/libs/xxhash
    library/cpp/digest/crc32c
)

END()
