LIBRARY()

SRCS(
    range_locker_access.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map
)

END()
