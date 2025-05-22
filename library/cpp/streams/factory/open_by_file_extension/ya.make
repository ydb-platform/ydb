LIBRARY()

PEERDIR(
    library/cpp/streams/bzip2
    library/cpp/streams/factory/open_common
)

SRCS(
    factory.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
