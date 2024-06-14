PROGRAM(astdiff)

SRCS(
    astdiff.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    ydb/library/yql/ast
    ydb/library/yql/utils/backtrace

    contrib/libs/dtl
)

END()
