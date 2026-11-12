LIBRARY()

SRCS(test_server.cpp)

PEERDIR(
    contrib/libs/openssl
    library/cpp/cgiparam
    library/cpp/http/misc
    library/cpp/http/server
    library/cpp/json
    library/cpp/testing/unittest
    ydb/public/sdk/cpp/src/client/types/credentials/oidc
)

END()
