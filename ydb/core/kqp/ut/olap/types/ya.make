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
    bool_ut.cpp
    datatime64_ut.cpp
    decimal_ut.cpp
    json_ut.cpp
)

PEERDIR(
    ydb/core/testlib
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

GENERATE_ENUM_SERIALIZATION(bool_test_enums.h)

END()
