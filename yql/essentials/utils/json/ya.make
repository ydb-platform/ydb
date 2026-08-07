LIBRARY()

PEERDIR(
    library/cpp/json
)

SRCS(
    expected.cpp
    from.cpp
    meta.cpp
    to.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
