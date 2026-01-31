UNITTEST_FOR(ydb/library/actors/http)

SIZE(SMALL)

PEERDIR(
    contrib/libs/poco/NetSSL_OpenSSL
    contrib/libs/poco/Crypto
    contrib/libs/poco/Foundation
    contrib/libs/poco/Net
    ydb/library/actors/testlib
)

FILES(
    resources/generate_certs.sh
    resources/ca.crt
    resources/server.crt
    resources/server.key
    resources/client.crt
    resources/client.key
    resources/untrusted_ca.crt
    resources/untrusted_client.crt
    resources/untrusted_client.key
)

IF (NOT OS_WINDOWS)
SRCS(
    http_ut.cpp
    tls_client_connection.cpp
)
ELSE()
ENDIF()

END()
