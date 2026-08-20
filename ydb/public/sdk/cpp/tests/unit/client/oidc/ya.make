UNITTEST()

SRCS(
    credentials_ut.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types/core_facility
    ydb/public/sdk/cpp/src/client/types/credentials/oidc
)

END()
