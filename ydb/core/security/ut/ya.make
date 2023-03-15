UNITTEST_FOR(ydb/core/security)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    ticket_parser_ut.cpp
)

END()
