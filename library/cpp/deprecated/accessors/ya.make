LIBRARY()

SRCS(
    accessors.cpp
    accessors_impl.cpp
    memory_traits.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
