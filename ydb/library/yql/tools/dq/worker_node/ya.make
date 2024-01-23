IF (NOT OS_WINDOWS)
    PROGRAM()

    PEERDIR(
        ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
        library/cpp/getopt
        yt/cpp/mapreduce/client
        ydb/library/yql/dq/actors/compute
        ydb/library/yql/dq/actors/spilling
        ydb/library/yql/dq/comp_nodes
        ydb/library/yql/dq/integration/transform
        ydb/library/yql/dq/transform
        ydb/library/yql/minikql/comp_nodes/llvm14
        ydb/library/yql/providers/clickhouse/actors
        ydb/library/yql/providers/common/comp_nodes
        ydb/library/yql/providers/dq/runtime
        ydb/library/yql/providers/dq/service
        ydb/library/yql/providers/dq/metrics
        ydb/library/yql/providers/dq/stats_collector
        ydb/library/yql/providers/dq/task_runner
        ydb/library/yql/providers/pq/async_io
        ydb/library/yql/providers/pq/proto
        ydb/library/yql/providers/s3/actors
        ydb/library/yql/providers/ydb/actors
        ydb/library/yql/providers/ydb/comp_nodes
        ydb/library/yql/public/udf/service/exception_policy
        ydb/library/yql/utils
        ydb/library/yql/utils/log
        ydb/library/yql/utils/log/proto
        ydb/library/yql/utils/failure_injector
        ydb/library/yql/utils/backtrace
        ydb/library/yql/providers/yt/comp_nodes/dq
        ydb/library/yql/providers/yt/comp_nodes/llvm14
        ydb/library/yql/providers/yt/codec/codegen
        ydb/library/yql/providers/yt/mkql_dq
        ydb/library/yql/providers/dq/actors/yt
        ydb/library/yql/providers/dq/global_worker_manager
        ydb/library/yql/sql/pg
        ydb/library/yql/parser/pg_wrapper
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        main.cpp
    )

    END()
ENDIF()
