UNITTEST_FOR(ydb/core/tx/columnshard/splitter)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/arrow_kernels

    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/data_sharing
    ydb/core/kqp/common
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/public/udf
    ydb/core/persqueue
    ydb/core/kqp/session_actor
    ydb/core/tx/tx_proxy
    ydb/core/tx/columnshard/engines/storage/chunks
    ydb/core/tx
    ydb/core/mind
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    ydb/services/ydb
)

ADDINCL(
    ydb/library/arrow_clickhouse
)

YQL_LAST_ABI_VERSION()

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    ut_splitter.cpp
    batch_slice.cpp
)

END()
