YQL_LIBRARY()

SRCS(
    compute.cpp
    lowering.cpp
    services.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/minikql/async
    yql/essentials/minikql/computation
)

END()

RECURSE_FOR_TESTS(
    ut
)
