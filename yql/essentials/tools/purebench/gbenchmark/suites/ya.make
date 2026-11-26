YQL_LIBRARY()

SRCS(
    suite.cpp
)

PEERDIR(
    library/cpp/string_utils/quote
    library/cpp/testing/common
    yql/essentials/tools/purebench/lib
)

END()

RECURSE_FOR_TESTS(
    ut
)
