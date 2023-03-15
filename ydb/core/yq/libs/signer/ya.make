LIBRARY()

SRCS(
    signer.cpp
)

PEERDIR(
    ydb/core/yq/libs/hmac
)

END()

RECURSE_FOR_TESTS(
    ut
)
