LIBRARY()

SRCS(
    schema_gateway.cpp
)

PEERDIR(
    library/cpp/threading/future
)

END()

RECURSE(
    simple
    static
)
