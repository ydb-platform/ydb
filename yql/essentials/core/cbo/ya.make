LIBRARY()

SRCS(
    cbo_optimizer_new.cpp
    cbo_interesting_orderings.cpp
    cbo_hints.cpp
)

GENERATE_ENUM_SERIALIZATION(cbo_optimizer_new.h)

END()

RECURSE(
    simple
)

RECURSE_FOR_TESTS(
    ut
)
