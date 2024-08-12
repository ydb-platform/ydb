LIBRARY()

SRCS(
    cbo_optimizer_new.cpp
)

GENERATE_ENUM_SERIALIZATION(cbo_optimizer_new.h)

END()

RECURSE_FOR_TESTS(
    ut
)

