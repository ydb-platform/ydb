LIBRARY()

SRCS(
    decimal.cpp
    dispatch.cpp
    ints.cpp
    text.cpp
    time.cpp
)

PEERDIR(
    ydb/public/lib/scheme_types
)

END()

RECURSE_FOR_TESTS(
    ut
)
