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
    yql/essentials/utils/log/proto
    yql/essentials/utils/backtrace
)

END()

RECURSE_FOR_TESTS(
    ut
)
