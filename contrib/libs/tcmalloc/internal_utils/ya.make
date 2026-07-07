LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Apache-2.0)

VERSION(2025-01-30)

NO_UTIL()

NO_COMPILER_WARNINGS()

SRCDIR(contrib/libs/tcmalloc)

IF (OS_LINUX)
    SRCS(
        tcmalloc/internal/allocation_guard.cc
        tcmalloc/internal/environment.cc
        tcmalloc/internal/logging.cc
        tcmalloc/internal/page_size.cc
        tcmalloc/internal/pageflags.cc
        tcmalloc/internal/residency.cc
        tcmalloc/internal/util.cc
    )
    ADDINCL(
        GLOBAL contrib/libs/tcmalloc
    )
    PEERDIR(
        contrib/libs/tcmalloc/malloc_extension
        contrib/restricted/abseil-cpp
    )
ENDIF()

END()
