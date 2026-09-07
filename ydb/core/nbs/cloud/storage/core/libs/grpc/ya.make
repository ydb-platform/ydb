LIBRARY()

SRCS(
    auth_metadata.cpp
    completion.cpp
    credentials.cpp
    init.cpp
    keepalive.cpp
    periodic_tls_certificate_provider.cpp
    request.cpp
    threadpool.cpp
    tls_certificate_provider.cpp
    tls_utils.cpp
    utils.cpp
)

ADDINCL(
    contrib/libs/grpc
)

NO_COMPILER_WARNINGS()

PEERDIR(
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/libs/diagnostics
    ydb/core/nbs/cloud/storage/core/protos

    library/cpp/deprecated/atomic
    ydb/library/grpc/common
    library/cpp/logger

    contrib/libs/grpc
    contrib/proto/grpc/grpc/reflection/v1alpha
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_shutdown
)
