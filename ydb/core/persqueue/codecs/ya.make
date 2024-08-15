LIBRARY()

SRCS(
    pqv1.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/persqueue_public
)

END()

RECURSE_FOR_TESTS(
    ut
)
