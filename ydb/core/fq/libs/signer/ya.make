LIBRARY()

SRCS(
    signer.cpp
)

PEERDIR(
    ydb/core/fq/libs/hmac
)

END()

RECURSE_FOR_TESTS(
    ut
)
