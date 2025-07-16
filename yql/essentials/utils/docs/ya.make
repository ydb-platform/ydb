LIBRARY()

SRCS(
    markdown.cpp
)

PEERDIR(
    yql/essentials/utils
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    ut
)
