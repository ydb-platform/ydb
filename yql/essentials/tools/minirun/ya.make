PROGRAM()

SUBSCRIBER(g:yql)

ALLOCATOR(J)

SRCS(
    minirun.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    yql/essentials/tools/yql_facade_run
    yql/essentials/providers/pure
    yql/essentials/providers/common/provider
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    yql/essentials/cfg/tests/gateways.conf gateways.conf
    yql/essentials/cfg/tests/fs.conf fs.conf
    yql/essentials/cfg/tests/fs_arc.conf fs_arc.conf
    yql/essentials/cfg/tests/fs_http.conf fs_http.conf
)

END()
