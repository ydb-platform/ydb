UNITTEST_FOR(ydb/core/kqp/script_executions/table_queries)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    kqp_script_executions_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    ydb/core/kqp/script_executions/common
    ydb/core/kqp/script_executions/finalization
    ydb/core/kqp/script_executions/table_queries
    ydb/core/kqp/ut/common
    ydb/library/yql/providers/common/http_gateway/ut_helpers
    ydb/public/lib/ut_helpers
    ydb/public/sdk/cpp/src/client/driver
    ydb/services/ydb
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
