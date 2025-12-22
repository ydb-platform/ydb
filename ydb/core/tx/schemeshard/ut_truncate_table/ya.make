UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    SPLIT_FACTOR(40)
ENDIF()

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/schemeshard/ut_helpers
)

SRCS(
    ut_truncate_table_reboots.cpp
    ut_truncate_table_simple.cpp
)

YQL_LAST_ABI_VERSION()

END()
