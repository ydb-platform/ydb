LIBRARY()

SRCS(
    block_range_map.cpp
    block_range.cpp
)

PEERDIR(
    library/cpp/lwtrace
    util
)

END()

RECURSE_FOR_TESTS(
    ut
)
