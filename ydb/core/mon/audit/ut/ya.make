UNITTEST_FOR(ydb/core/mon/audit)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    audit_ut.cpp
    url_matcher_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
