UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:12)
ENDIF()

SIZE(SMALL)

SRCS(
    kqp_data_integrity_trails_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
