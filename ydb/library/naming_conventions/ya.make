LIBRARY()

OWNER(g:kikimr)

SRCS(
    naming_conventions.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
