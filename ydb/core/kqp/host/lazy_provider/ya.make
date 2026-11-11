LIBRARY()

SRCS(kqp_lazy_provider.cpp)

PEERDIR(
    yql/essentials/core
    yql/essentials/providers/common/provider
)

END()

RECURSE_FOR_TESTS(ut)
