UNITTEST_FOR(ydb/core/kqp/rm_service)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_rm_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    #ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
