UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    kqp_locks_tricky_ut.cpp
    kqp_locks_ut.cpp
    kqp_mvcc_ut.cpp
    kqp_tx_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
