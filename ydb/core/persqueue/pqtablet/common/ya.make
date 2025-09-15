LIBRARY()

SRCS(
    event_helpers.cpp
    tracing_support.cpp
)



PEERDIR(
    ydb/core/base
    ydb/core/persqueue/common
    ydb/library/logger
    ydb/public/sdk/cpp/src/client/persqueue_public
)

END()

RECURSE_FOR_TESTS(
)
