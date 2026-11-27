YQL_LIBRARY()

SRCS(
    dynamic_function_registry.cpp
)

PEERDIR(
    library/cpp/threading/hot_swap
    yql/essentials/minikql
)

END()

RECURSE_FOR_TESTS(
    ut
)
