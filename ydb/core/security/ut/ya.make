UNITTEST_FOR(ydb/core/security)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
    ydb/core/testlib/audit_helpers
    ydb/library/testlib/service_mocks
    ydb/library/testlib/service_mocks/ldap_mock
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
)

YQL_LAST_ABI_VERSION()

SRCS(
    audit_ut.cpp
    ticket_parser_ut.cpp
    ut_common.cpp
)

END()
