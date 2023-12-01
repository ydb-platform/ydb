LIBRARY()

NO_UTIL()
ALLOCATOR_IMPL()

PEERDIR(
    library/cpp/malloc/api
    contrib/deprecated/galloc
)

SRCS(
    malloc-info.cpp
)

END()
