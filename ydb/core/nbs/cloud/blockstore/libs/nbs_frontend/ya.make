LIBRARY()

SRCS(
    blockstore_facade.cpp
    frontend_runtime.cpp
)

PEERDIR(
    ydb/core/nbs/nbs1_compat_api/cloud/blockstore/libs/service
    ydb/core/nbs/cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
