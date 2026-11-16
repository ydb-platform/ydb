YQL_LIBRARY()
SRCS(
    layers.cpp
    layers_fwd.cpp
    layers_integration.cpp
    remote_layer_provider.cpp
    utils.cpp
)
PEERDIR(
    yql/essentials/ast
    library/cpp/threading/future
    yql/essentials/utils/fetch
)
END()

RECURSE_FOR_TESTS(
    ut
)
