LIBRARY()

SRCS(
    cbo_optimizer_new.cpp
    cbo_hints.cpp
)

GENERATE_ENUM_SERIALIZATION(cbo_optimizer_new.h)

END()

RECURSE_FOR_TESTS(
    ut
)

