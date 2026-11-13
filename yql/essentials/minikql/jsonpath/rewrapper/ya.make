LIBRARY()

PEERDIR(
    yql/essentials/minikql/jsonpath/rewrapper/proto
)

SRCS(
    dispatcher.cpp
    re.cpp
    registrator.cpp
)

END()

RECURSE(
    hyperscan
    proto
    re2
)

RECURSE_FOR_TESTS(
    ut
)
