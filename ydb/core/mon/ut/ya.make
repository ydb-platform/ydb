UNITTEST_FOR(ydb/core/mon)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(tsan.supp)
ENDIF()

PEERDIR(
    ydb/core/mon
    ydb/core/mon/ut_utils
    ydb/core/testlib/default
    ydb/core/testlib/audit_helpers
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/security
)

SRCS(
    mon_audit_ut.cpp
    mon_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
