UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(80)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/persqueue/writer
    yql/essentials/sql/pg_dummy
    yql/essentials/sql/v1_dummy
)

SRCS(
    ut_cdc_stream_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
