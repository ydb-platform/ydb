PROGRAM(astdiff)

SRCS(
    astdiff.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/resource
    library/cpp/svnversion
    ydb/library/yql/ast
    ydb/library/yql/utils/backtrace
)

BUNDLE(
    ydb/library/yql/tools/astdiff/differ NAME differ_bundled
)

RESOURCE(differ_bundled /differ)

END()
