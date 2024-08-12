LIBRARY()

SRCS(
    context.cpp
    log.cpp
    profile.cpp
    tls_backend.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/logger
    library/cpp/logger/global
    library/cpp/deprecated/atomic
    ydb/library/yql/utils/log/proto
)

END()

RECURSE_FOR_TESTS(
    ut
)
