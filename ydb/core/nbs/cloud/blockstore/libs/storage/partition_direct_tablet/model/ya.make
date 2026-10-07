LIBRARY()

SRCS(
    touched_vchunks.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model
)

END()

RECURSE_FOR_TESTS(
    ut
)
