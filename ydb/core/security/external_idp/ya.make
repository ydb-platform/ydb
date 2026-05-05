LIBRARY()

SRCS(
    external_idp_provider.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    contrib/libs/openssl
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/core/base
    ydb/core/protos
    ydb/core/security/util
    ydb/library/actors/core
    ydb/library/actors/http
    ydb/library/services
)

END()

RECURSE_FOR_TESTS(
    ut
)
