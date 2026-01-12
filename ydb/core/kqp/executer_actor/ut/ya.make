UNITTEST_FOR(ydb/core/kqp/executer_actor)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

SRCS(
    kqp_executer_ut.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
