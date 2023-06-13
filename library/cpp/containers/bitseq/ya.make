LIBRARY()

PEERDIR(
    util/draft
    library/cpp/pop_count
)

SRCS(
    bitvector.cpp
    readonly_bitvector.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
