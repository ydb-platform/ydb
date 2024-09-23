IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            mrjob
            EXECUTABLE
    )

    END()
ELSE()
    PROGRAM(mrjob)

    ALLOCATOR(J)

    SRCS(
        mrjob.cpp
    )

    IF (OS_LINUX)
        # prevent external python extensions to lookup protobuf symbols (and maybe
        # other common stuff) in main binary
        EXPORTS_SCRIPT(${ARCADIA_ROOT}/ydb/library/yql/tools/exports.symlist)
    ENDIF()

    PEERDIR(
        yt/cpp/mapreduce/client
        ydb/library/yql/public/udf/service/terminate_policy
        ydb/library/yql/providers/common/gateway
        ydb/library/yql/utils/backtrace
        ydb/library/yql/parser/pg_wrapper
        ydb/library/yql/sql/pg
        ydb/library/yql/providers/yt/job
        ydb/library/yql/providers/yt/codec/codegen
        ydb/library/yql/providers/yt/comp_nodes/llvm14
        ydb/library/yql/minikql/computation/llvm14
        ydb/library/yql/minikql/invoke_builtins/llvm14
        ydb/library/yql/minikql/comp_nodes/llvm14
    )

    YQL_LAST_ABI_VERSION()

    END()
ENDIF()

RECURSE_FOR_TESTS(
    test
)
