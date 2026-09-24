UNITTEST_FOR(ydb/core/tx/schemeshard)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    library/cpp/testing/unittest
    ydb/core/tx/tx_proxy
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    ydb/services/ydb

    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    yql/essentials/minikql/comp_nodes/llvm16

    yql/essentials/sql/v1_dummy
)

SRCS(
    ut_ru_calculator.cpp
)

YQL_LAST_ABI_VERSION()

END()
