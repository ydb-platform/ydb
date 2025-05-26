LIBRARY()

SRCS(
    schema.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/object
)

END()

RECURSE(
    static
)

RECURSE_FOR_TESTS(
    ut
)
