UNITTEST_FOR(ydb/core/tx/schemeshard)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/protos
    ydb/core/tx/replication/service
    ydb/core/tx/replication/ut_helpers
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/ydb_convert
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/src/client/draft
    yql/essentials/public/udf
)

SRCS(
    ut_rebuild_replication.cpp
    ut_replication.cpp
)

YQL_LAST_ABI_VERSION()

END()
