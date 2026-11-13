LIBRARY()

PEERDIR(
    library/cpp/threading/future
)

SRCS(
    discovery.cpp
)

END()

RECURSE(
    static
)
