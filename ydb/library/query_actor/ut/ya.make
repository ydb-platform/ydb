UNITTEST_FOR(ydb/library/query_actor)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
    SIZE(MEDIUM)
ELSE()
    SIZE(SMALL)
ENDIF()

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/testlib
    yql/essentials/sql/pg_dummy
)

SRCS(
    checksum_table_ut.cpp
    mvcc_version_ut.cpp
    query_actor_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
