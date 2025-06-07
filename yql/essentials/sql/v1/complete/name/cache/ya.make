LIBRARY()

SRCS(
    byte_size.cpp
    cache.cpp
    cached.cpp
)

PEERDIR(
    library/cpp/threading/future
)

END()

RECURSE(
    local
)

RECURSE_FOR_TESTS(
    ut
)
