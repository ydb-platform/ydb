UNITTEST_FOR(ydb/core/security/certificate_check)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    cert_check_ut.cpp
    cert_utils_ut.cpp
)

END()
