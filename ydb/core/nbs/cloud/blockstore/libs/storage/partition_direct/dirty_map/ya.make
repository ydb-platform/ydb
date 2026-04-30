LIBRARY()

SRCS(
    dirty_map.cpp
    inflight_info.cpp
    range_locker.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host

    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
