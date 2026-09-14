LIBRARY()
SRCS(layers.cpp)
PEERDIR(
    yql/essentials/ast
    library/cpp/threading/future
    yql/essentials/utils/fetch
)
END()

RECURSE_FOR_TESTS(
    ut
)
