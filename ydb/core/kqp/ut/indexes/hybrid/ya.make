UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

SRCS(
    kqp_hybrid_search_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/udfs/common/knn
    ydb/library/yql/udfs/common/hybrid_search
    yql/essentials/sql/pg_dummy
    ydb/public/sdk/cpp/adapters/issue
)

YQL_LAST_ABI_VERSION()

END()
