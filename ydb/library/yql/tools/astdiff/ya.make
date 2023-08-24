PROGRAM(astdiff)

SRCS(
    astdiff.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/library/yql/ast
    ydb/library/yql/utils/backtrace
)

END()
