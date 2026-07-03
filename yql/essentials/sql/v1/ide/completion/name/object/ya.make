LIBRARY()

SRCS(
    schema.cpp
)

PEERDIR(
    library/cpp/threading/future
)

END()

RECURSE(
    simple
)
