IF (NOT OPENSOURCE)

PROGRAM(purebench)

ALLOCATOR(J)

SRCS(
    purebench.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    library/cpp/skiff
    library/cpp/yson
    yt/yql/purecalc/io_specs/mkql
    yql/essentials/public/purecalc/io_specs/arrow
    yql/essentials/public/purecalc
)

YQL_LAST_ABI_VERSION()

END()

ENDIF()

