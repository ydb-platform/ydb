YQL_LIBRARY()

SRCS(
    register.cpp
)

PEERDIR(
    library/cpp/testing/common
    library/cpp/testing/gbenchmark
    library/cpp/yt/misc
    yql/essentials/tools/purebench/gbenchmark/suites
    yql/essentials/tools/purebench/lib
)

END()

RECURSE(
    suites
)

RECURSE_FOR_TESTS(
    ut
)
