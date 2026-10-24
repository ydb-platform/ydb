UNITTEST()

SRCS(
    oidc_provider_ut.cpp
)

PEERDIR(
    library/cpp/json
    ydb/public/sdk/cpp/src/client/discovery
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/types/credentials/oidc
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/security/oidc/recipe.inc)

END()
