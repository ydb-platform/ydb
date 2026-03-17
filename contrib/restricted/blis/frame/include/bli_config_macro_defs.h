/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin
   Copyright (C) 2019, Advanced Micro Devices, Inc.

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name(s) of the copyright holder(s) nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

#ifndef BLIS_CONFIG_MACRO_DEFS_H
#define BLIS_CONFIG_MACRO_DEFS_H


// -- INTEGER PROPERTIES -------------------------------------------------------

// The bit size of the integer type used to track values such as dimensions,
// strides, diagonal offsets. A value of 32 results in BLIS using 32-bit signed
// integers while 64 results in 64-bit integers. Any other value results in use
// of the C99 type "long int". Note that this ONLY affects integers used
// internally within BLIS as well as those exposed in the native BLAS-like BLIS
// interface.
#ifndef BLIS_INT_TYPE_SIZE
  #ifdef BLIS_ARCH_64
    #define BLIS_INT_TYPE_SIZE 64
  #else
    #define BLIS_INT_TYPE_SIZE 32
  #endif
#endif


// -- FLOATING-POINT PROPERTIES ------------------------------------------------

// Enable use of built-in C99 "float complex" and "double complex" types and
// associated overloaded operations and functions? Disabling results in
// scomplex and dcomplex being defined in terms of simple structs.
// NOTE: AVOID USING THIS FEATURE. IT IS PROBABLY BROKEN.
#ifdef BLIS_ENABLE_C99_COMPLEX
  // No additional definitions needed.
#else
  // Default behavior is disabled.
#endif


// -- MULTITHREADING -----------------------------------------------------------

// Enable multithreading via POSIX threads.
#ifdef BLIS_ENABLE_PTHREADS
  // No additional definitions needed.
#else
  // Default behavior is disabled.
#endif

// Enable multithreading via OpenMP.
#ifdef BLIS_ENABLE_OPENMP
  // No additional definitions needed.
#else
  // Default behavior is disabled.
#endif

// Perform a sanity check to make sure the user doesn't try to enable
// both OpenMP and pthreads.
#if defined ( BLIS_ENABLE_OPENMP ) && \
    defined ( BLIS_ENABLE_PTHREADS )
  #error "BLIS_ENABLE_OPENMP and BLIS_ENABLE_PTHREADS may not be simultaneously defined."
#endif

// Here, we define BLIS_ENABLE_MULTITHREADING if either OpenMP
// or pthreads are enabled. This macro is useful in situations when
// we want to detect use of either OpenMP or pthreads (as opposed
// to neither being used).
#if defined ( BLIS_ENABLE_OPENMP ) || \
    defined ( BLIS_ENABLE_PTHREADS )
  #define BLIS_ENABLE_MULTITHREADING
#endif

// Enable the use of prime numbers of threads when requesting automatic thread
// factorization. When disabled, requesting a prime number of threads will
// result in a reduction (by one) of the number of threads, provided that the
// prime number exceeds a minimum threshold (see below).
#ifdef BLIS_ENABLE_AUTO_PRIME_NUM_THREADS
  #undef BLIS_DISABLE_AUTO_PRIME_NUM_THREADS
#else
  // Default behavior is disabled.
  #undef  BLIS_DISABLE_AUTO_PRIME_NUM_THREADS // In case user explicitly disabled.
  #define BLIS_DISABLE_AUTO_PRIME_NUM_THREADS
#endif

// Set the maximum requested number of threads that BLIS will accept from the
// user that may be prime. If a larger prime number of threads is requested,
// it will be reduced by one to allow for more efficient thread factorizations.
// This value will only be used if BLIS_ENABLE_AUTO_PRIME_NUM_THREADS is defined.
#ifndef BLIS_NT_MAX_PRIME
  #define BLIS_NT_MAX_PRIME 11
#endif


// -- MIXED DATATYPE SUPPORT ---------------------------------------------------

// Enable mixed datatype support?
#ifdef BLIS_DISABLE_MIXED_DT
  #undef BLIS_ENABLE_GEMM_MD
#else
  // Default behavior is enabled.
  #define BLIS_ENABLE_GEMM_MD
#endif

// Enable memory-intensive optimizations for mixed datatype support?
#ifdef BLIS_DISABLE_MIXED_DT_EXTRA_MEM
  #undef BLIS_ENABLE_GEMM_MD_EXTRA_MEM
#else
  // Default behavior is enabled.
  #define BLIS_ENABLE_GEMM_MD_EXTRA_MEM
#endif


// -- MISCELLANEOUS OPTIONS ----------------------------------------------------

// Do NOT require the cross-blocksize constraints. That is, do not enforce
// MC % NR = 0 and NC % MR = 0 in bli_kernel_macro_defs.h. These are ONLY
// needed when implementing trsm_r by allowing the right-hand matrix B to
// be triangular.
#ifndef BLIS_RELAX_MCNR_NCMR_CONSTRAINTS
  #define BLIS_RELAX_MCNR_NCMR_CONSTRAINTS
#endif

// Stay initialized after auto-initialization, unless and until the user
// explicitly calls bli_finalize().
#ifdef BLIS_DISABLE_STAY_AUTO_INITIALIZED
  #undef BLIS_ENABLE_STAY_AUTO_INITIALIZED
#else
  // Default behavior is enabled.
  #undef  BLIS_ENABLE_STAY_AUTO_INITIALIZED // In case user explicitly enabled.
  #define BLIS_ENABLE_STAY_AUTO_INITIALIZED
#endif


// -- BLAS COMPATIBILITY LAYER -------------------------------------------------

// Enable the BLAS compatibility layer?
#ifdef BLIS_DISABLE_BLAS
  #undef BLIS_ENABLE_BLAS
#else
  // Default behavior is enabled.
  #undef  BLIS_ENABLE_BLAS // In case user explicitly enabled.
  #define BLIS_ENABLE_BLAS
#endif

// The bit size of the integer type used to track values such as dimensions and
// leading dimensions (ie: column strides) within the BLAS compatibility layer.
// A value of 32 results in the compatibility layer using 32-bit signed integers
// while 64 results in 64-bit integers. Any other value results in use of the
// C99 type "long int". Note that this ONLY affects integers used within the
// BLAS compatibility layer.
#ifndef BLIS_BLAS_INT_TYPE_SIZE
  #define BLIS_BLAS_INT_TYPE_SIZE 32
#endif

// By default, the level-3 BLAS routines are implemented by directly calling
// the BLIS object API. Alternatively, they may first call the typed BLIS
// API, which will then call the object API.
//#define BLIS_BLAS3_CALLS_TAPI
#ifdef BLIS_BLAS3_CALLS_TAPI
  #undef  BLIS_BLAS3_CALLS_OAPI
#else
  // Default behavior is to call object API directly.
  #undef  BLIS_BLAS3_CALLS_OAPI // In case user explicitly enabled.
  #define BLIS_BLAS3_CALLS_OAPI
#endif


// -- CBLAS COMPATIBILITY LAYER ------------------------------------------------

// Enable the CBLAS compatibility layer?
// NOTE: Enabling CBLAS will automatically enable the BLAS compatibility layer
// regardless of whether or not it was explicitly enabled above. Furthermore,
// the CBLAS compatibility layer will use the integer type size definition
// specified above when defining the size of its own integers (regardless of
// whether the BLAS layer was enabled directly or indirectly).
#ifdef BLIS_ENABLE_CBLAS
  // No additional definitions needed.
#else
  // Default behavior is disabled.
#endif


// -- SHARED LIBRARY SYMBOL EXPORT ---------------------------------------------

// When building shared libraries, we can control which symbols are exported for
// linking by external applications. BLIS annotates all function prototypes that
// are meant to be "public" with BLIS_EXPORT_BLIS (with BLIS_EXPORT_BLAS playing
// a similar role for BLAS compatibility routines). Which symbols are exported
// is controlled by the default symbol visibility, as specifed by the gcc option
// -fvisibility=[default|hidden]. The default for this option is 'default', or,
// "public", which, if allowed to stand, causes all symbols in BLIS to be
// linkable from the outside. But when compiling with -fvisibility=hidden, all
// symbols start out hidden (that is, restricted only for internal use by BLIS),
// with that setting overridden only for function prototypes or variable
// declarations that are annotated with BLIS_EXPORT_BLIS.

#ifndef BLIS_EXPORT
  #if !defined(BLIS_ENABLE_SHARED)
    #define BLIS_EXPORT
  #else
    #if defined(_WIN32) || defined(__CYGWIN__)
      #ifdef BLIS_IS_BUILDING_LIBRARY
        #define BLIS_EXPORT __declspec(dllexport)
      #else
        #define BLIS_EXPORT __declspec(dllimport)
      #endif
    #elif defined(__GNUC__) && __GNUC__ >= 4
      #define BLIS_EXPORT __attribute__ ((visibility ("default")))
    #else
      #define BLIS_EXPORT
    #endif
  #endif
#endif

#define BLIS_EXPORT_BLIS BLIS_EXPORT
#define BLIS_EXPORT_BLAS BLIS_EXPORT


// -- STATIC INLINE FUNCTIONS --------------------------------------------------

// C and C++ have different semantics for defining "inline" functions. In C,
// the keyword phrase "static inline" accomplishes this, though the "inline"
// is optional. In C++, the "inline" keyword is required and obviates "static"
// altogether. Why does this matter? While BLIS is compiled in C99, blis.h may
// be #included by a source file that is compiled with C++.
#ifdef __cplusplus
  #define BLIS_INLINE inline
#else
  //#define BLIS_INLINE static inline
  #define BLIS_INLINE static
#endif


#endif

