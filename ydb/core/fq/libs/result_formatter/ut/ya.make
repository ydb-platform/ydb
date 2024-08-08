UNITTEST_FOR(ydb/core/fq/libs/result_formatter)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    result_formatter_ut.cpp
)

PEERDIR(
    ydb/library/yql/sql/pg_dummy
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
