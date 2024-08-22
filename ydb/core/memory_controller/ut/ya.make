UNITTEST_FOR(ydb/core/memory_controller)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    #ydb/library/yql/sql/pg_dummy
    ydb/core/testlib
    ydb/core/tx/datashard/ut_common
    ydb/core/tablet_flat
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

SRCS(
    memory_controller_ut.cpp
    memtable_collection_ut.cpp
)

END()
