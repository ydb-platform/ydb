LIBRARY()

SRCS(
    config.cpp
)

PEERDIR(
    contrib/libs/yaml-cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
