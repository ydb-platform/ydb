LIBRARY()

SRCS(
    partition_direct.cpp
    partition_direct_actor.cpp
)

PEERDIR(
    library/cpp/containers/absl_flat_hash
    ydb/library/actors/core
    ydb/core/nbs/cloud/blockstore/libs/storage/api
    ydb/core/nbs/cloud/storage/core/libs/common
)

END()
