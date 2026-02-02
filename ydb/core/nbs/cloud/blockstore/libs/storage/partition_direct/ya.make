LIBRARY()

SRCS(
    partition_direct.cpp
    partition_direct_actor.cpp
)

PEERDIR(
    library/cpp/containers/absl_flat_hash
    ydb/core/nbs/cloud/blockstore/config
    ydb/core/nbs/cloud/blockstore/libs/storage/api
    ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group
    ydb/core/nbs/cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
