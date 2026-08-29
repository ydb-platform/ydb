LIBRARY()

SRCS(
    plan2svg.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/yson
)

END()

RECURSE_FOR_TESTS(
    ut
)
