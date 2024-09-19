UNITTEST_FOR(ydb/library/ncloud/impl)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/testlib
    ydb/core/testlib/actors
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    access_service_ut.cpp
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:10)
ENDIF()

END()
