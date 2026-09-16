LIBRARY()

SRCS(
    path_normalizer.cpp
)

PEERDIR(
    contrib/libs/re2
    library/cpp/openssl/crypto
    ydb/core/protos
)

END()

RECURSE_FOR_TESTS(
    context/ut
    ut
)
