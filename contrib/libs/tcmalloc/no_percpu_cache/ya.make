LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(2025-01-30)

ORIGINAL_SOURCE(https://github.com/google/tcmalloc/archive/c8dfee3e4c489c5ae0d30c484c92db102a69ec51.tar.gz)

LICENSE(Apache-2.0)

ALLOCATOR_IMPL()

SRCDIR(contrib/libs/tcmalloc)

GLOBAL_SRCS(
    # Options
    tcmalloc/want_hpaa.cc
)

INCLUDE(../common.inc)

SRCS(
    aligned_alloc.c
)

CFLAGS(
    -DTCMALLOC_256K_PAGES
    -DTCMALLOC_DEPRECATED_PERTHREAD
)

END()
