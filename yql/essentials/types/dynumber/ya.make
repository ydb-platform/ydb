LIBRARY()

ENABLE(YQL_STYLE_CPP)

PEERDIR(
    library/cpp/containers/stack_vector
)

SRCS(
    cast.h
    dynumber.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
