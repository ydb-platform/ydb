UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SPLIT_FACTOR(10)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/tx/schemeshard/ut_helpers
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    ut_cdc_stream_reboots.cpp
)

YQL_LAST_ABI_VERSION()

END()
