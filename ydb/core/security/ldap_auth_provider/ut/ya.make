UNITTEST_FOR(ydb/core/security/ldap_auth_provider)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/core/testlib/default
    ydb/library/testlib/service_mocks/ldap_mock
)

YQL_LAST_ABI_VERSION()

SRCS(
    ldap_auth_provider_ut.cpp
    ldap_utils_ut.cpp
)

END()
