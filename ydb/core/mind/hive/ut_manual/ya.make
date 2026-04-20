UNITTEST_FOR(ydb/core/mind/hive)

FORK_SUBTESTS()

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc) 

TAG(ya:manual)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/library/actors/helpers
    ydb/core/base
    ydb/core/mind
    ydb/core/mind/hive
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    manual_ut.cpp
)

END()
