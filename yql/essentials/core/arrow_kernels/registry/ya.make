YQL_LIBRARY()

SRCS(
    registry.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/minikql/computation
    yql/essentials/public/langver
)

END()

RECURSE_FOR_TESTS(
    ut
)
