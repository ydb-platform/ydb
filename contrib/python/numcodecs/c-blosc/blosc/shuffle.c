/*********************************************************************
  Blosc - Blocked Shuffling and Compression Library

  Author: Francesc Alted <francesc@blosc.org>
  Creation date: 2009-05-20

  See LICENSE.txt for details about copyright and rights to use.
**********************************************************************/

#include "shuffle.h"
#include "blosc-common.h"
#include "shuffle-generic.h"
#include "bitshuffle-generic.h"
#include "blosc-comp-features.h"
#include <stdio.h>

#if defined(_WIN32)
#include "win32/pthread.h"
#else
#include <pthread.h>
#endif

#if !defined(__clang__) && defined(__GNUC__) && defined(__GNUC_MINOR__) && \
    __GNUC__ >= 5 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 8)
#define HAVE_CPU_FEAT_INTRIN
#endif


/*  Include hardware-accelerated shuffle/unshuffle routines based on
    the target architecture. Note that a target architecture may support
    more than one type of acceleration!*/
#if defined(SHUFFLE_AVX2_ENABLED)
  #include "shuffle-avx2.h"
  #include "bitshuffle-avx2.h"
#endif  /* defined(SHUFFLE_AVX2_ENABLED) */

#if defined(SHUFFLE_SSE2_ENABLED)
  #include "shuffle-sse2.h"
  #include "bitshuffle-sse2.h"
#endif  /* defined(SHUFFLE_SSE2_ENABLED) */


/*  Define function pointer types for shuffle/unshuffle routines. */
typedef void(*shuffle_func)(const size_t, const size_t, const uint8_t*, const uint8_t*);
typedef void(*unshuffle_func)(const size_t, const size_t, const uint8_t*, const uint8_t*);
typedef int64_t(*bitshuffle_func)(void*, void*, const size_t, const size_t, void*);
typedef int64_t(*bitunshuffle_func)(void*, void*, const size_t, const size_t, void*);

/* An implementation of shuffle/unshuffle routines. */
typedef struct shuffle_implementation {
  /* Name of this implementation. */
  const char* name;
  /* Function pointer to the shuffle routine for this implementation. */
  shuffle_func shuffle;
  /* Function pointer to the unshuffle routine for this implementation. */
  unshuffle_func unshuffle;
  /* Function pointer to the bitshuffle routine for this implementation. */
  bitshuffle_func bitshuffle;
  /* Function pointer to the bitunshuffle routine for this implementation. */
  bitunshuffle_func bitunshuffle;
} shuffle_implementation_t;

typedef enum {
  BLOSC_HAVE_NOTHING = 0,
  BLOSC_HAVE_SSE2 = 1,
  BLOSC_HAVE_AVX2 = 2
} blosc_cpu_features;

/*  Detect hardware and set function pointers to the best shuffle/unshuffle
    implementations supported by the host processor for Intel/i686
     */
#if (defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)) \
    && (defined(SHUFFLE_AVX2_ENABLED) || defined(SHUFFLE_SSE2_ENABLED))

/*  Disabled the __builtin_cpu_supports() call, as it has issues with
    new versions of gcc (like 5.3.1 in forthcoming ubuntu/xenial:
      "undefined symbol: __cpu_model"
    For a similar report, see:
    https://lists.fedoraproject.org/archives/list/devel@lists.fedoraproject.org/thread/ZM2L65WIZEEQHHLFERZYD5FAG7QY2OGB/
*/
#if defined(HAVE_CPU_FEAT_INTRIN) && 0
static blosc_cpu_features blosc_get_cpu_features(void) {
  blosc_cpu_features cpu_features = BLOSC_HAVE_NOTHING;
  if (__builtin_cpu_supports("sse2")) {
    cpu_features |= BLOSC_HAVE_SSE2;
  }
  if (__builtin_cpu_supports("avx2")) {
    cpu_features |= BLOSC_HAVE_AVX2;
  }
  return cpu_features;
}
#else

#if defined(_MSC_VER) && !defined(__clang__)
  #include <intrin.h>     /* Needed for __cpuid */

/*  _xgetbv is only supported by VS2010 SP1 and newer versions of VS. */
#if _MSC_FULL_VER >= 160040219
  #include <immintrin.h>  /* Needed for _xgetbv */
  #define blosc_internal_xgetbv _xgetbv
#elif defined(_M_IX86)

/*  Implement _xgetbv for VS2008 and VS2010 RTM with 32-bit (x86) targets. */

static uint64_t blosc_internal_xgetbv(uint32_t xcr) {
    uint32_t xcr0, xcr1;
    __asm {
        mov        ecx, xcr
        _asm _emit 0x0f _asm _emit 0x01 _asm _emit 0xd0
        mov        xcr0, eax
        mov        xcr1, edx
    }
    return ((uint64_t)xcr1 << 32) | xcr0;
}

#elif defined(_M_X64)

/*  Implement _xgetbv for VS2008 and VS2010 RTM with 64-bit (x64) targets.
    These compilers don't support any of the newer acceleration ISAs
    (e.g., AVX2) supported by blosc, and all x64 hardware supports SSE2
    which means we can get away with returning a hard-coded value from
    this implementation of _xgetbv. */

static __inline uint64_t blosc_internal_xgetbv(uint32_t xcr) {
    /* A 64-bit OS must have XMM save support. */
    return (xcr == 0 ? (1UL << 1) : 0UL);
}

#else

/* Hardware detection for any other MSVC targets (e.g., ARM)
   isn't implemented at this time. */
#error This version of c-blosc only supports x86 and x64 targets with MSVC.

#endif /* _MSC_FULL_VER >= 160040219 */

#define blosc_internal_cpuid __cpuid

#else

/*  Implement the __cpuid and __cpuidex intrinsics for GCC, Clang,
    and others using inline assembly. */
__attribute__((always_inline))
static inline void
blosc_internal_cpuidex(int32_t cpuInfo[4], int32_t function_id, int32_t subfunction_id) {
  __asm__ __volatile__ (
# if defined(__i386__) && defined (__PIC__)
  /*  Can't clobber ebx with PIC running under 32-bit, so it needs to be manually restored.
      https://software.intel.com/en-us/articles/how-to-detect-new-instruction-support-in-the-4th-generation-intel-core-processor-family
  */
    "movl %%ebx, %%edi\n\t"
    "cpuid\n\t"
    "xchgl %%ebx, %%edi":
    "=D" (cpuInfo[1]),
#else
    "cpuid":
    "=b" (cpuInfo[1]),
#endif  /* defined(__i386) && defined(__PIC__) */
    "=a" (cpuInfo[0]),
    "=c" (cpuInfo[2]),
    "=d" (cpuInfo[3]) :
    "a" (function_id), "c" (subfunction_id)
    );
}

#define blosc_internal_cpuid(cpuInfo, function_id) blosc_internal_cpuidex(cpuInfo, function_id, 0)

#define _XCR_XFEATURE_ENABLED_MASK 0

/* Reads the content of an extended control register.
   https://software.intel.com/en-us/articles/how-to-detect-new-instruction-support-in-the-4th-generation-intel-core-processor-family
*/
static inline uint64_t
blosc_internal_xgetbv(uint32_t xcr) {
  uint32_t eax, edx;
  __asm__ __volatile__ (
    /* "xgetbv"
       This is specified as raw instruction bytes due to some older compilers
       having issues with the mnemonic form.
    */
    ".byte 0x0f, 0x01, 0xd0":
    "=a" (eax),
    "=d" (edx) :
    "c" (xcr)
    );
  return ((uint64_t)edx << 32) | eax;
}

#endif  /* defined(_MSC_FULL_VER) */

#ifndef _XCR_XFEATURE_ENABLED_MASK
#define _XCR_XFEATURE_ENABLED_MASK 0x0
#endif

static blosc_cpu_features blosc_get_cpu_features(void) {
  blosc_cpu_features result = BLOSC_HAVE_NOTHING;
  int32_t max_basic_function_id;
  /* Holds the values of eax, ebx, ecx, edx set by the `cpuid` instruction */
  int32_t cpu_info[4];
  int sse2_available;
  int sse3_available;
  int ssse3_available;
  int sse41_available;
  int sse42_available;
  int xsave_available;
  int xsave_enabled_by_os;
  int avx2_available = 0;
  int avx512bw_available = 0;
  int xmm_state_enabled = 0;
  int ymm_state_enabled = 0;
  int zmm_state_enabled = 0;
  uint64_t xcr0_contents;
  char* envvar;

  /* Get the number of basic functions available. */
  blosc_internal_cpuid(cpu_info, 0);
  max_basic_function_id = cpu_info[0];

  /* Check for SSE-based features and required OS support */
  blosc_internal_cpuid(cpu_info, 1);
  sse2_available = (cpu_info[3] & (1 << 26)) != 0;
  sse3_available = (cpu_info[2] & (1 << 0)) != 0;
  ssse3_available = (cpu_info[2] & (1 << 9)) != 0;
  sse41_available = (cpu_info[2] & (1 << 19)) != 0;
  sse42_available = (cpu_info[2] & (1 << 20)) != 0;

  xsave_available = (cpu_info[2] & (1 << 26)) != 0;
  xsave_enabled_by_os = (cpu_info[2] & (1 << 27)) != 0;

  /* Check for AVX-based features, if the processor supports extended features. */
  if (max_basic_function_id >= 7) {
    blosc_internal_cpuid(cpu_info, 7);
    avx2_available = (cpu_info[1] & (1 << 5)) != 0;
    avx512bw_available = (cpu_info[1] & (1 << 30)) != 0;
  }

  /*  Even if certain features are supported by the CPU, they may not be supported
      by the OS (in which case using them would crash the process or system).
      If xsave is available and enabled by the OS, check the contents of the
      extended control register XCR0 to see if the CPU features are enabled. */
#if defined(_XCR_XFEATURE_ENABLED_MASK)
  if (xsave_available && xsave_enabled_by_os && (
      sse2_available || sse3_available || ssse3_available
      || sse41_available || sse42_available
      || avx2_available || avx512bw_available)) {
    /* Determine which register states can be restored by the OS. */
    xcr0_contents = blosc_internal_xgetbv(_XCR_XFEATURE_ENABLED_MASK);

    xmm_state_enabled = (xcr0_contents & (1UL << 1)) != 0;
    ymm_state_enabled = (xcr0_contents & (1UL << 2)) != 0;

    /*  Require support for both the upper 256-bits of zmm0-zmm15 to be
        restored as well as all of zmm16-zmm31 and the opmask registers. */
    zmm_state_enabled = (xcr0_contents & 0x70) == 0x70;
  }
#endif /* defined(_XCR_XFEATURE_ENABLED_MASK) */

  envvar = getenv("BLOSC_PRINT_SHUFFLE_ACCEL");
  if (envvar != NULL) {
    printf("Shuffle CPU Information:\n");
    printf("SSE2 available: %s\n", sse2_available ? "True" : "False");
    printf("SSE3 available: %s\n", sse3_available ? "True" : "False");
    printf("SSSE3 available: %s\n", ssse3_available ? "True" : "False");
    printf("SSE4.1 available: %s\n", sse41_available ? "True" : "False");
    printf("SSE4.2 available: %s\n", sse42_available ? "True" : "False");
    printf("AVX2 available: %s\n", avx2_available ? "True" : "False");
    printf("AVX512BW available: %s\n", avx512bw_available ? "True" : "False");
    printf("XSAVE available: %s\n", xsave_available ? "True" : "False");
    printf("XSAVE enabled: %s\n", xsave_enabled_by_os ? "True" : "False");
    printf("XMM state enabled: %s\n", xmm_state_enabled ? "True" : "False");
    printf("YMM state enabled: %s\n", ymm_state_enabled ? "True" : "False");
    printf("ZMM state enabled: %s\n", zmm_state_enabled ? "True" : "False");
  }

  /* Using the gathered CPU information, determine which implementation to use. */
  /* technically could fail on sse2 cpu on os without xmm support, but that
   * shouldn't exist anymore */
  if (sse2_available) {
    result |= BLOSC_HAVE_SSE2;
  }
  if (xmm_state_enabled && ymm_state_enabled && avx2_available) {
    result |= BLOSC_HAVE_AVX2;
  }
  return result;
}
#endif

#else   /* No hardware acceleration supported for the target architecture. */
  #if defined(_MSC_VER)
  #pragma message("Hardware-acceleration detection not implemented for the target architecture. Only the generic shuffle/unshuffle routines will be available.")
  #else
  #warning Hardware-acceleration detection not implemented for the target architecture. Only the generic shuffle/unshuffle routines will be available.
  #endif

static blosc_cpu_features blosc_get_cpu_features(void) {
  return BLOSC_HAVE_NOTHING;
}

#endif

static shuffle_implementation_t get_shuffle_implementation(void) {
  blosc_cpu_features cpu_features = blosc_get_cpu_features();
  shuffle_implementation_t impl_generic;

#if defined(SHUFFLE_AVX2_ENABLED)
  if (cpu_features & BLOSC_HAVE_AVX2) {
    shuffle_implementation_t impl_avx2;
    impl_avx2.name = "avx2";
    impl_avx2.shuffle = (shuffle_func)blosc_internal_shuffle_avx2;
    impl_avx2.unshuffle = (unshuffle_func)blosc_internal_unshuffle_avx2;
    impl_avx2.bitshuffle = (bitshuffle_func)blosc_internal_bshuf_trans_bit_elem_avx2;
    impl_avx2.bitunshuffle = (bitunshuffle_func)blosc_internal_bshuf_untrans_bit_elem_avx2;
    return impl_avx2;
  }
#endif  /* defined(SHUFFLE_AVX2_ENABLED) */

#if defined(SHUFFLE_SSE2_ENABLED)
  if (cpu_features & BLOSC_HAVE_SSE2) {
    shuffle_implementation_t impl_sse2;
    impl_sse2.name = "sse2";
    impl_sse2.shuffle = (shuffle_func)blosc_internal_shuffle_sse2;
    impl_sse2.unshuffle = (unshuffle_func)blosc_internal_unshuffle_sse2;
    impl_sse2.bitshuffle = (bitshuffle_func)blosc_internal_bshuf_trans_bit_elem_sse2;
    impl_sse2.bitunshuffle = (bitunshuffle_func)blosc_internal_bshuf_untrans_bit_elem_sse2;
    return impl_sse2;
  }
#endif  /* defined(SHUFFLE_SSE2_ENABLED) */

  /*  Processor doesn't support any of the hardware-accelerated implementations,
      so use the generic implementation. */
  impl_generic.name = "generic";
  impl_generic.shuffle = (shuffle_func)blosc_internal_shuffle_generic;
  impl_generic.unshuffle = (unshuffle_func)blosc_internal_unshuffle_generic;
  impl_generic.bitshuffle = (bitshuffle_func)blosc_internal_bshuf_trans_bit_elem_scal;
  impl_generic.bitunshuffle = (bitunshuffle_func)blosc_internal_bshuf_untrans_bit_elem_scal;
  return impl_generic;
}


/*  Flag indicating whether the implementation has been initialized. */
static pthread_once_t implementation_initialized = PTHREAD_ONCE_INIT;

/*  The dynamically-chosen shuffle/unshuffle implementation.
    This is only safe to use once `implementation_initialized` is set. */
static shuffle_implementation_t host_implementation;

static void set_host_implementation(void) {
  host_implementation = get_shuffle_implementation();
}

/*  Initialize the shuffle implementation, if necessary. */
#if defined(__GNUC__) || defined(__clang__)
__attribute__((always_inline))
#endif
static
#if defined(_MSC_VER)
__forceinline
#else
BLOSC_INLINE
#endif
void init_shuffle_implementation(void) {
  pthread_once(&implementation_initialized, &set_host_implementation);
}

/*  Shuffle a block by dynamically dispatching to the appropriate
    hardware-accelerated routine at run-time. */
void
blosc_internal_shuffle(const size_t bytesoftype, const size_t blocksize,
                       const uint8_t* _src, const uint8_t* _dest) {
  /* Initialize the shuffle implementation if necessary. */
  init_shuffle_implementation();

  /*  The implementation is initialized.
      Dispatch to it's shuffle routine. */
  (host_implementation.shuffle)(bytesoftype, blocksize, _src, _dest);
}

/*  Unshuffle a block by dynamically dispatching to the appropriate
    hardware-accelerated routine at run-time. */
void
blosc_internal_unshuffle(const size_t bytesoftype, const size_t blocksize,
                         const uint8_t* _src, const uint8_t* _dest) {
  /* Initialize the shuffle implementation if necessary. */
  init_shuffle_implementation();

  /*  The implementation is initialized.
      Dispatch to it's unshuffle routine. */
  (host_implementation.unshuffle)(bytesoftype, blocksize, _src, _dest);
}

/*  Bit-shuffle a block by dynamically dispatching to the appropriate
    hardware-accelerated routine at run-time. */
int
blosc_internal_bitshuffle(const size_t bytesoftype, const size_t blocksize,
                          const uint8_t* const _src, const uint8_t* _dest,
                          const uint8_t* _tmp) {
  int size = blocksize / bytesoftype;
  /* Initialize the shuffle implementation if necessary. */
  init_shuffle_implementation();

  if ((size % 8) == 0) {
    /* The number of elems is a multiple of 8 which is supported by
       bitshuffle. */
    int ret = (int)(host_implementation.bitshuffle)((void *) _src, (void *) _dest,
                                                    blocksize / bytesoftype,
                                                    bytesoftype, (void *) _tmp);
    /* Copy the leftovers */
    size_t offset = size * bytesoftype;
    memcpy((void *) (_dest + offset), (void *) (_src + offset), blocksize - offset);
    return ret;
  }
  else {
    memcpy((void *) _dest, (void *) _src, blocksize);
  }
  return size;
}

/*  Bit-unshuffle a block by dynamically dispatching to the appropriate
    hardware-accelerated routine at run-time. */
int
blosc_internal_bitunshuffle(const size_t bytesoftype, const size_t blocksize,
                            const uint8_t* const _src, const uint8_t* _dest,
                            const uint8_t* _tmp) {
  int size = blocksize / bytesoftype;
  /* Initialize the shuffle implementation if necessary. */
  init_shuffle_implementation();

  if ((size % 8) == 0) {
    /* The number of elems is a multiple of 8 which is supported by
       bitshuffle. */
    int ret = (int) (host_implementation.bitunshuffle)((void *) _src, (void *) _dest,
                                                       blocksize / bytesoftype,
                                                       bytesoftype, (void *) _tmp);
    /* Copy the leftovers */
    size_t offset = size * bytesoftype;
    memcpy((void *) (_dest + offset), (void *) (_src + offset), blocksize - offset);
    return ret;
  }
  else {
    memcpy((void *) _dest, (void *) _src, blocksize);
  }
  return size;
}
