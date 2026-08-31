LIBRARY()

SRCS(
    iovector.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/libs/common
    ydb/core/nbs/cloud/blockstore/compat/public/api/protos

    ydb/core/nbs/cloud/storage/core/libs/common

    library/cpp/digest/crc32c
    library/cpp/threading/future
    library/cpp/deprecated/atomic

    ydb/library/actors/prof
)

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(
        -DPROFILE_MEMORY_ALLOCATIONS
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
