UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_document_api_ut.cpp
    kqp_qs_queries_ut.cpp
    kqp_qs_scripts_ut.cpp
    kqp_service_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/tx/columnshard/hooks/testing
    ydb/library/yql/sql/pg
    ydb/library/yql/parser/pg_wrapper
    ydb/public/lib/ut_helpers
    ydb/public/sdk/cpp/client/ydb_operation
    ydb/public/sdk/cpp/client/ydb_types/operation
)

YQL_LAST_ABI_VERSION()

END()
