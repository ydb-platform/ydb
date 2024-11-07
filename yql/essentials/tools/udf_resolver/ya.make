PROGRAM()

SRCS(
    udf_resolver.cpp
    discover.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/exports.symlist)
    PEERDIR(
        contrib/libs/libc_compat
    )
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/protobuf/util
    yql/essentials/minikql
    yql/essentials/public/udf/service/terminate_policy
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/schema/mkql
    yql/essentials/utils/backtrace
    yql/essentials/utils/sys
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
