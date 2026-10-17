LIBRARY()

SRCS(
    fyamlcpp.cpp
    fyamlcpp.h
)

PEERDIR(
    contrib/libs/libfyaml  # gn: public dep
)

END()

RECURSE_FOR_TESTS(
    ut
)
