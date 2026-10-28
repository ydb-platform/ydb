LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(2025-02-22)

ORIGINAL_SOURCE(https://github.com/google/tcmalloc/archive/7dd049e3367acff457a20cc4fb4c8b366cb2892d.tar.gz)

LICENSE(Apache-2.0)

ALLOCATOR_IMPL()

SRCDIR(contrib/libs/tcmalloc)

INCLUDE(../common.inc)

CFLAGS(
    -DTCMALLOC_INTERNAL_SMALL_BUT_SLOW
)

END()
