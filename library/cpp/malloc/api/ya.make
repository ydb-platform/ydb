LIBRARY()

NO_UTIL()


SRCS(
    malloc.cpp
    malloc.h  # gn: slot allocator
)

END()

RECURSE(
    helpers
    ut
)
