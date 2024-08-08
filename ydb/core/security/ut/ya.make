UNITTEST_FOR(ydb/core/security)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    ydb/core/testlib/default
    ydb/library/testlib/service_mocks
)

YQL_LAST_ABI_VERSION()

SRCS(
    ticket_parser_ut.cpp
)

END()
