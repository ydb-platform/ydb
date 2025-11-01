LIBRARY()

SRCS(
    utils.cpp
)

PEERDIR(
    library/cpp/string_utils/url
)

END()

RECURSE_FOR_TESTS(ut)
