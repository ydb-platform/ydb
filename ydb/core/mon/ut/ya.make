UNITTEST_FOR(ydb/core/mon)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

PEERDIR(
    ydb/core/mon
    ydb/core/mon/ut_utils
    ydb/core/testlib/default
    ydb/library/aclib
    ydb/library/actors/core
)

SRCS(
    mon_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
