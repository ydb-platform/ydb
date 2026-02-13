UNITTEST_FOR(ydb/library/actors/http)

SIZE(SMALL)

PEERDIR(
    contrib/libs/poco/NetSSL_OpenSSL
    contrib/libs/poco/Crypto
    contrib/libs/poco/Foundation
    contrib/libs/poco/Net
    ydb/core/security/certificate_check
    ydb/library/actors/testlib
)


IF (NOT OS_WINDOWS)
SRCS(
    http_ut.cpp
    tls_client_connection.cpp
)
ELSE()
ENDIF()

END()
