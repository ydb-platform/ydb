UNITTEST_FOR(ydb/core/tx/schemeshard)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/json
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/persqueue/public/write_meta
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/core/tx/scheme_cache
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/ydb_convert
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/src/client/draft
    yql/essentials/public/udf
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_replication.cpp
)

YQL_LAST_ABI_VERSION()

END()
