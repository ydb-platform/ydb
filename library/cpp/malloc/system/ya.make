LIBRARY()

NO_UTIL()

OWNER(nga)

DISABLE(OPENSOURCE_EXPORT)

PEERDIR(
    library/cpp/malloc/api
)

SRCS(
    malloc-info.cpp
)

END()
