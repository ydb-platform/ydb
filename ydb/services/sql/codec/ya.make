LIBRARY()

SRCS(
    dispatch.cpp
    ints.cpp
    text.cpp
)

PEERDIR(
    ydb/public/lib/scheme_types
)

END()

RECURSE_FOR_TESTS(
    ut
)
