UNITTEST_FOR(ydb/core/tx/schemeshard)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/tx/tx_proxy
    ydb/services/ydb
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    yql/essentials/minikql/comp_nodes/llvm20
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yt/yql/providers/yt/comp_nodes/dq/llvm20
    yt/yql/providers/yt/comp_nodes/llvm20
)

SRCS(
    ut_ru_calculator.cpp
)

YQL_LAST_ABI_VERSION()

END()
