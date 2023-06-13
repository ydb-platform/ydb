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
ENDIF()

SRCS(
    kqp_effects_ut.cpp
    kqp_immediate_effects_ut.cpp
    kqp_inplace_update_ut.cpp
    kqp_write_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
