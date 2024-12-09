LIBRARY()

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)
ALLOCATOR_IMPL()

VERSION(2024-12-07-253603d9e83f545984e0a593851693a8ae34993f)

ORIGINAL_SOURCE(https://github.com/google/tcmalloc/archive/253603d9e83f545984e0a593851693a8ae34993f.tar.gz)

SRCS(
    # Options
    tcmalloc/want_hpaa.cc
)

INCLUDE(common.inc)

CFLAGS(
    -DTCMALLOC_256K_PAGES
)

END()

IF (NOT DLL_FOR)
    RECURSE(
        default
        dynamic
        malloc_extension
        no_percpu_cache
        numa_256k
        numa_large_pages
        small_but_slow
    )
ENDIF()
