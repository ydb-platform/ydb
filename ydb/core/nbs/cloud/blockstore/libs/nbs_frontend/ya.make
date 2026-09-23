LIBRARY()

SRCS(
    blockstore_facade.cpp
    partition_registry.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/libs/storage/model
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session
    library/cpp/logger
    library/cpp/threading/hot_swap
    ydb/core/protos
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/libs/diagnostics
)

END()

RECURSE_FOR_TESTS(
    ut
)
