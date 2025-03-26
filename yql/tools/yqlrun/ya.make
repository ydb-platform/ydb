PROGRAM(yqlrun)

SRCS(
    yqlrun.cpp
)

INCLUDE(ya.make.inc)

YQL_LAST_ABI_VERSION()

FILES(
    ui.sh
    uig.sh
)

END()
