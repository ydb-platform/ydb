LIBRARY()

SRCS(
    frontend_state.cpp
    blockstore_facade.cpp
    frontend_runtime.cpp
)

PEERDIR(
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
