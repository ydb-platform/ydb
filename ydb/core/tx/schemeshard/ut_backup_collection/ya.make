UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(2)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/protos
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_backup_collection.cpp
)

YQL_LAST_ABI_VERSION()

END()
