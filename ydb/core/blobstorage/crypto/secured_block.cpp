#include "secured_block.h"

#include <new>


#if defined(_MSC_VER)
#   if _MSC_VER == 1200
#       include <malloc.h>
#   endif
#   if _MSC_VER > 1200 || defined(_mm_free)
#       define MSVC6PP_OR_LATER       // VC 6 processor pack or later
#   else
#       define MSVC6_NO_PP            // VC 6 without processor pack
#   endif
#endif

// how to allocate 16-byte aligned memory (for SSE2)
#if defined(MSVC6PP_OR_LATER)
#   define MM_MALLOC_AVAILABLE
#elif defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
#   define MALLOC_ALIGNMENT_IS_16
#elif defined(__linux__) || defined(__sun__) || defined(__CYGWIN__)
#   define MEMALIGN_AVAILABLE
#   include <stdlib.h>
#else
#   define NO_ALIGNED_ALLOC
#endif



void CallNewHandler()
{
#if !(defined(_MSC_VER) && (_MSC_VER < 1300))
    using std::new_handler;
    using std::set_new_handler;
#endif

    new_handler newHandler = set_new_handler(nullptr);
    if (newHandler)
        set_new_handler(newHandler);

    if (newHandler)
        newHandler();
    else
        throw std::bad_alloc();
}

ui8* AlignedAllocate(size_t size)
{
    ui8* p;
#ifdef MM_MALLOC_AVAILABLE
    while (!(p = (ui8*)_mm_malloc(size, 16)))
#elif defined(MEMALIGN_AVAILABLE)
    while (posix_memalign((void**)&p, 16, size))
#elif defined(MALLOC_ALIGNMENT_IS_16)
    while (!(p = (ui8*)malloc(size)))
#else
    while (!(p = (ui8*)malloc(size + 16)))
#endif
        CallNewHandler();

#ifdef NO_ALIGNED_ALLOC
    size_t adjustment = 16 - ((size_t)p % 16);
    p += adjustment;
    p[-1] = (ui8)adjustment;
#endif

    Y_ASSERT(IsAlignedOn(p, 16));
    return p;
}

void AlignedDeallocate(ui8* p)
{
#ifdef MM_MALLOC_AVAILABLE
    _mm_free(p);
#elif defined(NO_ALIGNED_ALLOC)
    p = (ui8*)p - ((ui8*)p)[-1];
    free(p);
#else
    free(p);
#endif
}

ui8* UnalignedAllocate(size_t size)
{
    ui8* p;
    while (!(p = (ui8*)malloc(size)))
        CallNewHandler();
    return p;
}

void UnalignedDeallocate(ui8* p)
{
    free(p);
}

