UNITTEST_FOR(ydb/public/lib/deprecated/kicli)

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

FORK_SUBTESTS()

PEERDIR(
    ydb/core/client
    ydb/core/testlib/default
    ydb/public/lib/deprecated/kicli
)

YQL_LAST_ABI_VERSION()

SRCS(
    cpp_ut.cpp
)

END()
