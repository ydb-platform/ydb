UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(150)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    GLOBAL blobs_sharing_ut.cpp
    GLOBAL kqp_olap_ut.cpp
    aggregations_ut.cpp
    clickbench_ut.cpp
    dictionary_ut.cpp
    locks_ut.cpp
    optimizer_ut.cpp
    sys_view_ut.cpp
    tiering_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard
    ydb/core/kqp/ut/olap/helpers
    ydb/core/kqp/ut/olap/combinatory
    ydb/core/tx/datashard/ut_common
    ydb/public/sdk/cpp/src/client/operation
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    indexes
    types
    operations
    statistics
    storage
    reading
)
