UNITTEST()

SRCS(
    credentials_ut.cpp
    jwt_token_source_ut.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    library/cpp/http/server
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/public/sdk/cpp/src/client/types/credentials/oauth2_token_exchange
    ydb/public/sdk/cpp/tests/unit/client/oauth2_token_exchange/helpers
)

END()
