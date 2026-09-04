LIBRARY()

SRCS(
    blockstore_facade.cpp
    frontend_runtime.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/compat/libs/service
    ydb/core/nbs/cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
