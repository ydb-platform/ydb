IF (NOT OS_WINDOWS)
    PROGRAM()

    PEERDIR(
        library/cpp/getopt
        yt/cpp/mapreduce/client
        ydb/library/yql/sql/pg
        ydb/library/yql/parser/pg_wrapper
        ydb/library/yql/public/udf/service/exception_policy
        ydb/library/yql/utils/failure_injector
        ydb/library/yql/utils/log
        ydb/library/yql/utils/log/proto
        ydb/library/yql/providers/dq/provider
        ydb/library/yql/providers/dq/worker_manager/interface
        ydb/library/yql/minikql/invoke_builtins/llvm14
        ydb/library/yql/utils/backtrace
        ydb/library/yql/providers/dq/service
        ydb/library/yql/providers/dq/metrics
        ydb/library/yql/providers/dq/stats_collector
        ydb/library/yql/providers/yt/dq_task_preprocessor
        ydb/library/yql/providers/dq/global_worker_manager
        ydb/library/yql/providers/dq/actors/yt
        ydb/library/yql/providers/yt/comp_nodes/llvm14
        ydb/library/yql/providers/yt/codec/codegen
        yt/yt/client
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        main.cpp
    )

    END()
ENDIF()
