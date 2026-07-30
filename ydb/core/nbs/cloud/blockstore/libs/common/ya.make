LIBRARY()

SRCS(
    block_range_algorithms.cpp
    block_range_field.cpp
    block_range_map.cpp
    block_range.cpp
    record_id.cpp
    thread_checker.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/coroutine
    library/cpp/lwtrace
    util
)

END()

RECURSE_FOR_TESTS(
    ut
)
