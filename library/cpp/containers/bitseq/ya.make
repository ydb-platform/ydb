LIBRARY()

PEERDIR(
    util/draft
)

SRCS(
    bitvector.cpp
    readonly_bitvector.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
