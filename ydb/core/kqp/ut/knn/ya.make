UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

SIZE(SMALL)

SRCS(
    kqp_knn_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/udfs/common/knn
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

