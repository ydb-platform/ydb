UNITTEST_FOR(ydb/library/http_proxy/authorization)

PEERDIR(
    ydb/library/http_proxy/error
)

SRCS(
    auth_helpers_ut.cpp
    signature_ut.cpp
)

END()
