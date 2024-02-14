PROGRAM(astdiff)

SRCS(
    astdiff.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/resource
    library/cpp/svnversion
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils/backtrace
)

BUNDLE(
    contrib/ydb/library/yql/tools/astdiff/differ NAME differ_bundled
)

RESOURCE(differ_bundled /differ)

END()
