PROGRAM(purebench)

ALLOCATOR(J)

SRCS(
    purebench.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/library/yql/utils/backtrace
    ydb/library/yql/utils/log
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    library/cpp/skiff
    library/cpp/yson
    ydb/library/yql/public/purecalc/io_specs/mkql
    ydb/library/yql/public/purecalc/io_specs/arrow
    ydb/library/yql/public/purecalc
)

YQL_LAST_ABI_VERSION()

END()
