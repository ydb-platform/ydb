YQL_PREBUILT_PROGRAM(yqlrun)

IF (NOT OPENSOURCE AND NOT BUILD_YQL_FROM_SOURCES)
    INCLUDE(${ARCADIA_ROOT}/yql/resources.inc)
ENDIF()

SRCS(
    yqlrun.cpp
)

INCLUDE(ya.make.inc)

YQL_CURRENT_ABI_VERSION()

FILES(
    ui.sh
    uig.sh
)

END()
