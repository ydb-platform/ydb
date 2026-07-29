UNITTEST_FOR(ydb/core/security/external_idp)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/security/external_idp/test_utils
    ydb/core/util/actorsys_test
    ydb/library/actors/http
    ydb/library/services
    library/cpp/json
    library/cpp/monlib/service/pages
    library/cpp/string_utils/base64
    library/cpp/testing/unittest
    contrib/libs/jwt-cpp
    contrib/libs/openssl
)

SRCS(
    external_idp_provider_ut.cpp
)

END()
