LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Apache-2.0)

NO_UTIL()

NO_COMPILER_WARNINGS()

VERSION(2025-01-30)

ORIGINAL_SOURCE(https://github.com/google/tcmalloc/archive/c8dfee3e4c489c5ae0d30c484c92db102a69ec51.tar.gz)

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
