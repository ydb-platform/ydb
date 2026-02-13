UNITTEST_FOR(ydb/core/tx/columnshard/splitter)

SIZE(SMALL)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/kqp/common
    ydb/core/kqp/session_actor
    ydb/core/mind
    ydb/core/tx
    ydb/core/tx/columnshard/counters
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/common
    ydb/core/tx/columnshard/blobs_action
    ydb/core/tx/columnshard/data_sharing
    ydb/core/tx/columnshard/engines/reader/common_reader/iterator
    ydb/core/tx/columnshard/engines/storage/chunks
    ydb/core/tx/columnshard/engines/storage/indexes/max
    ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch
    ydb/core/tx/columnshard/data_accessor
    ydb/core/tx/tx_proxy
    ydb/library/arrow_kernels
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    ydb/services/ydb
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
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
