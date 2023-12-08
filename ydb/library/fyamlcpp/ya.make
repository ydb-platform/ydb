LIBRARY()

SRCS(
    fyamlcpp.cpp
    fyamlcpp.h
)

PEERDIR(
    contrib/libs/libfyaml
)

END()

RECURSE_FOR_TESTS(
    ut
)
