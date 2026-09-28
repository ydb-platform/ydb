PROGRAM()

ALLOCATOR(J)

INCLUDE(
    ${ARCADIA_ROOT}/yql/essentials/udfs/common/python/sanitizer_suppressions.inc
)

SRCS(
    minirun.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    yql/essentials/tools/minirun/lib
)

YQL_LAST_ABI_VERSION()

END()
