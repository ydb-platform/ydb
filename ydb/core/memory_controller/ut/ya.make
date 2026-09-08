UNITTEST_FOR(ydb/core/memory_controller)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    yql/essentials/sql/pg_dummy
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
