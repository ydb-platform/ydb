UNITTEST_FOR(ydb/library/query_actor)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

PEERDIR(
    ydb/core/testlib
    yql/essentials/sql/pg_dummy
)

SRCS(
    query_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
