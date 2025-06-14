UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/double-conversion
    library/cpp/string_utils/quote
    ydb/core/kqp/ut/common
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/util
    ydb/core/wrappers/ut_helpers
    ydb/core/ydb_convert
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    ydb/core/testlib/audit_helpers
)

SRCS(
    ut_restore.cpp
)

YQL_LAST_ABI_VERSION()

END()
