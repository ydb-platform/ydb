UNITTEST_FOR(ydb/core/mon)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    mon_ut.cpp
)

PEERDIR(
    library/cpp/http/misc
    library/cpp/http/simple
    ydb/core/base
    ydb/core/mon
    ydb/core/protos
    ydb/core/testlib/default
    ydb/library/aclib
    ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()
