UNITTEST_FOR(ydb/library/actors/http)

SIZE(SMALL)

PEERDIR(
    contrib/libs/poco/NetSSL_OpenSSL
    contrib/libs/poco/Crypto
    contrib/libs/poco/Foundation
    contrib/libs/poco/Net
    ydb/core/security/certificate_check/test_utils
    ydb/library/actors/testlib
)


IF (NOT OS_WINDOWS)
SRCS(
    http_cache_ut.cpp
    http_ut.cpp
    http2_ut.cpp
    http_proxy_ut.cpp
    tls_client_connection.cpp
)
ELSE()
ENDIF()

END()
