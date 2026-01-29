LIBRARY()

SRCS(
    arn.cpp
    consumer.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/string_utils/url
)

END()

RECURSE_FOR_TESTS(ut)
RECURSE(holder)
