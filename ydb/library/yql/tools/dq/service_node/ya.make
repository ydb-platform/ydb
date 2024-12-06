IF (NOT OS_WINDOWS)
    PROGRAM()

    PEERDIR(
        library/cpp/getopt
        yt/cpp/mapreduce/client
        yql/essentials/sql/pg
        yql/essentials/parser/pg_wrapper
        yql/essentials/public/udf/service/exception_policy
        yql/essentials/utils/failure_injector
        yql/essentials/utils/log
        yql/essentials/utils/log/proto
        ydb/library/yql/providers/dq/provider
        ydb/library/yql/providers/dq/worker_manager/interface
        yql/essentials/minikql/invoke_builtins/llvm14
        yql/essentials/utils/backtrace
        ydb/library/yql/providers/dq/service
        ydb/library/yql/providers/dq/metrics
        ydb/library/yql/providers/dq/stats_collector
        ydb/library/yql/providers/yt/dq_task_preprocessor
        ydb/library/yql/providers/dq/global_worker_manager
        ydb/library/yql/providers/dq/actors/yt
        yt/yql/providers/yt/comp_nodes/llvm14
        yt/yql/providers/yt/codec/codegen
        yt/yt/client
    )

    YQL_LAST_ABI_VERSION()

    SRCS(
        main.cpp
    )

    END()
ENDIF()
