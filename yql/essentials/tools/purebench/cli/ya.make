IF (NOT OPENSOURCE)

PROGRAM(purebench)

ALLOCATOR(J)

SRCS(
    main.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    yql/essentials/public/udf
    yql/essentials/tools/purebench/lib
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
)

YQL_CURRENT_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    test
)

ENDIF()

