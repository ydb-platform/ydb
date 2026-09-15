LIBRARY()

SRCS(
    partition_key_range.cpp
    partition_key_range_sequence.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/scheme
)

END()

RECURSE_FOR_TESTS(ut)
