UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    # kqp_executer_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/common
    ydb/core/kqp/host
    ydb/core/kqp/ut/common
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
