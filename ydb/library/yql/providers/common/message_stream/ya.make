YQL_LIBRARY()

SRCS(provider.cpp)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/core
    yql/essentials/providers/common/structured_token
)

END()

RECURSE_FOR_TESTS(
    async_io
    ut
)
