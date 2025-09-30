IF (NOT OS_WINDOWS)
    PROGRAM()

    IF (PROFILE_MEMORY_ALLOCATIONS)
        ALLOCATOR(LF_DBG)
        CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
    ELSE()
        IF (OS_LINUX AND NOT DISABLE_TCMALLOC)
            ALLOCATOR(TCMALLOC_256K)
        ELSE()
            ALLOCATOR(J)
        ENDIF()
    ENDIF()


    IF (OOM_HELPER)
        PEERDIR(yql/essentials/utils/oom_helper)
    ENDIF()

    SRCS(
        dqrun.cpp
    )

    PEERDIR(
        ydb/library/yql/tools/dqrun/lib

        yt/yql/providers/yt/codec/codegen
        yt/yql/providers/yt/comp_nodes/llvm16
        yt/yql/providers/yt/comp_nodes/dq/llvm16
        yql/essentials/minikql/invoke_builtins/llvm16
        yql/essentials/minikql/comp_nodes/llvm16
        yql/essentials/parser/pg_wrapper
        yql/essentials/public/udf/service/exception_policy
        yql/essentials/sql/pg

        library/cpp/lfalloc/alloc_profiler

        ydb/library/yql/udfs/common/clickhouse/client
        ydb/library/yql/dq/comp_nodes/llvm16
        ydb/public/sdk/cpp/src/client/persqueue_public/codecs
    )

    YQL_LAST_ABI_VERSION()

    END()
ELSE()
    LIBRARY()

    END()
ENDIF()
