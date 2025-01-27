IF (NOT OS_WINDOWS)
    PROGRAM()

    PEERDIR(
        ydb/public/sdk/cpp/src/client/persqueue_public/codecs
        library/cpp/getopt
        yt/cpp/mapreduce/client
        ydb/library/yql/dq/actors/compute
        ydb/library/yql/dq/actors/spilling
        ydb/library/yql/dq/comp_nodes
        yql/essentials/core/dq_integration/transform
        ydb/library/yql/dq/transform
        yql/essentials/minikql/comp_nodes/llvm14
        ydb/library/yql/providers/clickhouse/actors
        yql/essentials/providers/common/comp_nodes
        ydb/library/yql/providers/dq/runtime
        ydb/library/yql/providers/dq/service
        ydb/library/yql/providers/dq/metrics
        ydb/library/yql/providers/dq/stats_collector
        ydb/library/yql/providers/dq/task_runner
        ydb/library/yql/providers/pq/async_io
        ydb/library/yql/providers/pq/gateway/native
        ydb/library/yql/providers/pq/proto
        ydb/library/yql/providers/s3/actors
        ydb/library/yql/providers/ydb/actors
        ydb/library/yql/providers/ydb/comp_nodes
        yql/essentials/public/udf/service/exception_policy
        yql/essentials/utils
        yql/essentials/utils/log
        yql/essentials/utils/log/proto
        yql/essentials/utils/failure_injector
        yql/essentials/utils/backtrace
        yql/essentials/utils/network
        yt/yql/providers/yt/comp_nodes/dq/llvm14
        yt/yql/providers/yt/comp_nodes/llvm14
        yt/yql/providers/yt/codec/codegen
        yt/yql/providers/yt/mkql_dq
        ydb/library/yql/providers/dq/actors/yt
        ydb/library/yql/providers/dq/global_worker_manager
        yql/essentials/sql/pg
        yql/essentials/parser/pg_wrapper
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        main.cpp
    )

    END()
ENDIF()
