PROGRAM()

SRCS(
    main.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
    PEERDIR(
        contrib/libs/libc_compat
    )
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/string_utils/base64
    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/minikql/runtime_settings
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/public/langver
    yql/essentials/parser/pg_wrapper
    yql/essentials/utils/backtrace
)

YQL_LAST_ABI_VERSION()

END()
