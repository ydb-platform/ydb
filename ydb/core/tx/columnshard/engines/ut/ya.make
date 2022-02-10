UNITTEST_FOR(ydb/core/tx/columnshard/engines)

OWNER(
    chertus
    g:kikimr
)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND) 
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/apache/arrow 
    ydb/core/base
    ydb/core/tablet
    ydb/core/tablet_flat
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_insert_table.cpp
    ut_logs_engine.cpp
)

END()
