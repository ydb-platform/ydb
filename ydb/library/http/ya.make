LIBRARY()

SRCS(
    rfc7239_forwarded.cpp
)

PEERDIR(
    library/cpp/string_utils/url
)

END()

RECURSE_FOR_TESTS(
    ut
)
