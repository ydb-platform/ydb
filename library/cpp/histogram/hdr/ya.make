LIBRARY()

SRCS(
    histogram.cpp
    histogram_iter.cpp
)

PEERDIR(
    contrib/libs/hdr_histogram
)

END()

RECURSE_FOR_TESTS(
    ut
)
