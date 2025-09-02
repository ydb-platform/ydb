LIBRARY()

PEERDIR(
    ydb/public/sdk/cpp/src/client/iam
    library/cpp/digest/crc32c
    ydb/public/sdk/cpp/src/client/types/credentials
)

SRCS(
    util.cpp
    ydb_credentials_provider_factory.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
