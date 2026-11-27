LIBRARY()

SRCS(
    client.cpp
    config.cpp
    durable.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/compat/public/api/grpc
    ydb/core/nbs/cloud/blockstore/compat/public/api/protos

    ydb/core/nbs/cloud/blockstore/compat/config
    ydb/core/nbs/cloud/blockstore/compat/libs/common
    ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics
    ydb/core/nbs/cloud/blockstore/compat/libs/service
    ydb/core/nbs/cloud/blockstore/libs/storage/model
    ydb/core/nbs/cloud/storage/core/libs/grpc
    ydb/core/nbs/cloud/storage/core/libs/throttling

    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/threading/future
    library/cpp/monlib/service
    library/cpp/monlib/service/pages

    contrib/libs/grpc
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        tsan.supp
    )
ENDIF()

END()

# Until DEVTOOLSSUPPORT-25698 is not solved.
IF (SANITIZER_TYPE == "address" OR SANITIZER_TYPE == "memory")
    RECURSE_FOR_TESTS(
        ut
    )
ELSE()
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
