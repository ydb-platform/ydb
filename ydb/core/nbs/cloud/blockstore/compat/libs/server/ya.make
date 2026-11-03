LIBRARY()

NO_LINT()

SRCS(
    config.cpp
    server.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/compat/public/api/grpc
    ydb/core/nbs/cloud/blockstore/compat/public/api/protos

    ydb/core/nbs/cloud/blockstore/compat/config
    ydb/core/nbs/cloud/blockstore/compat/libs/common
    ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics
    ydb/core/nbs/cloud/blockstore/compat/libs/service
    ydb/core/nbs/cloud/storage/core/libs/grpc
    ydb/core/nbs/cloud/storage/core/libs/uds

    ydb/library/actors/prof
    library/cpp/monlib/service
    library/cpp/monlib/service/pages

    contrib/libs/grpc
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        tsan.supp
    )
ENDIF()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

END()

RECURSE_FOR_TESTS(ut)
