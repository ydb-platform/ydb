PROGRAM()

SRCS(
    udf_resolver.cpp
    discover.cpp
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
    library/cpp/protobuf/util
    yql/essentials/minikql
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/core
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/schema/mkql
    yql/essentials/utils/backtrace
    yql/essentials/utils/sys
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
