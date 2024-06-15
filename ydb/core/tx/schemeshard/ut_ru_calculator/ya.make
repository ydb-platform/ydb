UNITTEST_FOR(ydb/core/tx/schemeshard)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/tx/tx_proxy
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
    ydb/services/ydb

    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    ydb/library/yql/minikql/comp_nodes/llvm14

)

SRCS(
    ut_ru_calculator.cpp
)

YQL_LAST_ABI_VERSION()

END()
