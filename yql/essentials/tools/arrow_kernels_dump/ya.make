PROGRAM()

SRCS(
    arrow_kernels_dump.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    yql/essentials/minikql/invoke_builtins
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/public/udf
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
