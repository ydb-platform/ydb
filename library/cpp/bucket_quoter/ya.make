LIBRARY()

SRCS(
    bucket_quoter.cpp
)

PEERDIR(
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
