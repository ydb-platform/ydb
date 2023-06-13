LIBRARY()

NO_UTIL()
ALLOCATOR_IMPL()

PEERDIR(
    library/cpp/malloc/api
    contrib/libs/tcmalloc/malloc_extension
)
SRCS(
    malloc-info.cpp
)

END()
