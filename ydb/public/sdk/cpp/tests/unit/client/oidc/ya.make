UNITTEST()

SRCS(
    credentials_ut.cpp
    protocol_ut.cpp
    test_server.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/tests/unit/client/oidc/helpers
    library/cpp/cgiparam
    library/cpp/http/server
    library/cpp/json
    library/cpp/string_utils/base64
    ydb/public/sdk/cpp/src/client/discovery
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/types/core_facility
    ydb/public/sdk/cpp/src/client/types/credentials/oidc
)

END()
