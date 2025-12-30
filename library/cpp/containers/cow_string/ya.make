LIBRARY()

SRCS(
    cow_string.cpp
    output.cpp
    reverse.cpp
    subst.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_medium
)
