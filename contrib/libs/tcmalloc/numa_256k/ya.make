LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(2021-10-04-45c59ccbc062ac96d83710205033c656e490d376)

LICENSE(Apache-2.0)
ALLOCATOR_IMPL()

SRCDIR(contrib/libs/tcmalloc)

INCLUDE(../common.inc)

GLOBAL_SRCS(
    # Options
    tcmalloc/want_hpaa_subrelease.cc
    tcmalloc/want_numa_aware.cc
)

CFLAGS(
    -DTCMALLOC_256K_PAGES
    -DTCMALLOC_NUMA_AWARE
)

END()
