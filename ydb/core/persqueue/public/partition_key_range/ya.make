LIBRARY()

SRCS(
    key_range_split.cpp
    partition_key_range.cpp
    partition_key_range_sequence.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/scheme
    ydb/library/kll_median
    ydb/services/lib/sharding
)

END()

RECURSE_FOR_TESTS(ut)
