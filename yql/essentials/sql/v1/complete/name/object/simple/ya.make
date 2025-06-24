LIBRARY()

SRCS(
    schema.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/object
    library/cpp/case_insensitive_string
)

END()

RECURSE(
    static
)

RECURSE_FOR_TESTS(
    ut
)
