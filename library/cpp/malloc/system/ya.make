# External build system generator for opensource export relies on the name system_allocator.
# Change it carefully.
LIBRARY(system_allocator)

ALLOCATOR_IMPL()

NO_UTIL()

PEERDIR(
    library/cpp/malloc/api
)

SRCS(
    malloc-info.cpp
)

END()
