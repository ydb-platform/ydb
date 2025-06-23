LIBRARY()

SRCS(
    registry.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/minikql/computation
    yql/essentials/public/langver
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
