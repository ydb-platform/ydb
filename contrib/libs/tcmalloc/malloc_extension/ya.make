LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Apache-2.0)

NO_UTIL()

NO_COMPILER_WARNINGS()

VERSION(2025-02-22)

ORIGINAL_SOURCE(https://github.com/google/tcmalloc/archive/7dd049e3367acff457a20cc4fb4c8b366cb2892d.tar.gz)

SRCDIR(contrib/libs/tcmalloc)

SRCS(
    tcmalloc/malloc_extension.cc
)

PEERDIR(
    contrib/restricted/abseil-cpp
)

ADDINCL(
    GLOBAL contrib/libs/tcmalloc
)

CFLAGS(
    -DTCMALLOC_256K_PAGES
)

END()
