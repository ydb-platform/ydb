LIBRARY()

SRCS(
    kafka.h
    pqv1.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/topic
)

END()

RECURSE_FOR_TESTS(
    ut
)
