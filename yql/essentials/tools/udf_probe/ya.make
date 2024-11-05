PROGRAM()

SRCS(
    udf_probe.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    contrib/ydb/library/yql/minikql
    yql/essentials/public/udf/service/terminate_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
