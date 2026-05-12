UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

REQUIREMENTS(cpu:2)

SIZE(MEDIUM)

SRCS(
    kqp_indexes_json_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/udfs/common/knn
    yql/essentials/sql/pg_dummy
    ydb/public/sdk/cpp/adapters/issue
)

YQL_LAST_ABI_VERSION()

END()
