LIBRARY()

SRCS(
    blockstore_facade.cpp
    frontend_runtime.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/session
    ydb/library/actors/core
    library/cpp/logger
    library/cpp/threading/atomic_shared_ptr
    ydb/core/protos
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/libs/diagnostics
)

END()

RECURSE_FOR_TESTS(
    ut
)
