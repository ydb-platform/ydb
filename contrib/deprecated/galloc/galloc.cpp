#include <util/system/defaults.h>

#ifndef USE_GOOGLE_ALLOCATOR
    #define USE_GOOGLE_ALLOCATOR 1
#endif

#if defined(_MSC_VER) && !defined(__MWERKS__) && !defined (__ICL) && !defined (__COMO__)
    #define USE_VISUALCC
#elif defined(__INTEL_COMPILER)
    #define USE_INTELCC
#elif defined(__GNUC__)
    #define USE_GNUCC
#elif defined(__SUNPRO_C) || defined(__SUNPRO_CC)
    #define USE_SUNCC
#else
    //#error your compiler does not supported
#endif

#if defined(USE_INTELCC)
    #pragma warning(disable 177)
    #pragma warning(disable 869)
    #pragma warning(disable 810)
    #pragma warning(disable 967)
    #pragma warning(disable 1599)
    #pragma warning(disable 1469)
#endif

#if defined(_linux_) || defined(_freebsd_)
    #define GOOGLE_ALLOCATOR_IS_USABLE
#endif

#if defined(GOOGLE_ALLOCATOR_IS_USABLE) && USE_GOOGLE_ALLOCATOR
    #undef NDEBUG
    #define NDEBUG

    #define HAVE_INTTYPES_H 1
    #define HAVE_MMAP 1
    #define HAVE_MUNMAP 1
    #define HAVE_PTHREAD 1
    #define HAVE_SBRK 1
    #define HAVE_UNWIND_H 1

    #if defined(USE_INTELCC) || defined(USE_GNUCC)
        #undef HAVE___ATTRIBUTE__
        #define HAVE___ATTRIBUTE__
    #endif

    #define PRIuS PRISZT
    #define LLU   PRIu64

    #include "malloc_extension.cc"
    #include "internal_logging.cc"
    #include "system-alloc.cc"
    #include "tcmalloc.cc"
#endif
