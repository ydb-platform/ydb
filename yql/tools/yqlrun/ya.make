PROGRAM(yqlrun)

SRCS(
    yqlrun.cpp
)

INCLUDE(ya.make.inc)

IF (NOT OPENSOURCE)
    PEERDIR(yql/spark/tools/tool_lib)
ELSE()
    CFLAGS(-DDONT_ADD_SPARK)
ENDIF()

YQL_LAST_ABI_VERSION()

FILES(
    ui.sh
    uig.sh
)

END()
