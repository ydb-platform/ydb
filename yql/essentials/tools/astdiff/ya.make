PROGRAM(astdiff)

SRCS(
    astdiff.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    yql/essentials/ast
    yql/essentials/utils/backtrace

    contrib/libs/dtl
)

END()
