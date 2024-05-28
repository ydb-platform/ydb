UNITTEST_FOR(ydb/core/security)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
    ydb/library/testlib/service_mocks
    ydb/library/testlib/service_mocks/ldap_mock
)

YQL_LAST_ABI_VERSION()

SRCS(
    ticket_parser_ut.cpp
    ldap_utils_ut.cpp
    cert_utils_ut.cpp
)

END()
