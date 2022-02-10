LIBRARY()

NO_UTIL()

OWNER(ayles)

PEERDIR(
    library/cpp/malloc/api
    contrib/libs/tcmalloc/malloc_extension
)
SRCS(
    malloc-info.cpp
)

END()
