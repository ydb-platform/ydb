PROGRAM(mrjob)

ALLOCATOR(J)

SRCS(
    mrjob.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    yt/cpp/mapreduce/client
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/providers/common/gateway
    yql/essentials/utils/backtrace
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    yt/yql/providers/yt/job
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    test
)
