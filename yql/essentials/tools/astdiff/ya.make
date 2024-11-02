PROGRAM(astdiff)

SRCS(
    astdiff.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/library/yql/ast
    yql/essentials/utils/backtrace

    contrib/libs/dtl
)

END()
