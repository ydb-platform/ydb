UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE)
    TIMEOUT(600)
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:4)
ELSE()
    SIZE(SMALL)
ENDIF()

SRCS(
    kqp_tli_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/library/actors/wilson/test_util
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
