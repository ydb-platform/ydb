LIBRARY()

SRCS(
    tag.cpp
    tcmalloc.cpp
)

PEERDIR(
    contrib/libs/tcmalloc/malloc_extension
    library/cpp/charset/lite
    library/cpp/containers/atomizer
)

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
    PEERDIR(
        library/cpp/malloc/api
        library/cpp/lfalloc/dbg_info
        library/cpp/ytalloc/api
        library/cpp/yt/memory
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
