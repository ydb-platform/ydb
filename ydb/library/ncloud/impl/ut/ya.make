UNITTEST_FOR(ydb/library/ncloud/impl)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/testlib
    ydb/core/testlib/actors
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
)

END()
