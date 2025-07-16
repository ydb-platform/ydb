LIBRARY()

SRCS(
    test_token_exchange_server.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    library/cpp/cgiparam
    library/cpp/http/misc
    library/cpp/http/server
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/public/sdk/cpp/src/client/types/credentials/oauth2_token_exchange
)

END()
