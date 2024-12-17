UNITTEST_FOR(ydb/public/sdk/cpp/client/ydb_types/credentials/oauth2_token_exchange)

SRCS(
    credentials_ut.cpp
    jwt_token_source_ut.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    library/cpp/http/server
    library/cpp/json
    library/cpp/string_utils/base64
)

END()
