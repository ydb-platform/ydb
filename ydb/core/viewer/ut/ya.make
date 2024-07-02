UNITTEST_FOR(ydb/core/viewer)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    viewer_ut.cpp
)

PEERDIR(
    library/cpp/http/misc
    library/cpp/http/simple
    ydb/core/testlib/default
)

END()
