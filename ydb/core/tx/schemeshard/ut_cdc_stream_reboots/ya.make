UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(80)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/persqueue/writer
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_cdc_stream_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
