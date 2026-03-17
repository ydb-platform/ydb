/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2011-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2018      Intel, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * Atomic operations.
 *
 * This API is patterned after the FreeBSD kernel atomic interface
 * (which is influenced by Intel's ia64 architecture).  The
 * FreeBSD interface is documented at
 *
 * http://www.freebsd.org/cgi/man.cgi?query=atomic&sektion=9
 *
 * Only the necessary subset of functions are implemented here.
 *
 * The following #defines will be true / false based on
 * assembly support:
 *
 *  - \c PMIX_HAVE_ATOMIC_MEM_BARRIER atomic memory barriers
 *  - \c PMIX_HAVE_ATOMIC_SPINLOCKS atomic spinlocks
 *  - \c PMIX_HAVE_ATOMIC_MATH_32 if 32 bit add/sub/compare-exchange can be done "atomicly"
 *  - \c PMIX_HAVE_ATOMIC_MATH_64 if 64 bit add/sub/compare-exchange can be done "atomicly"
 *
 * Note that for the Atomic math, atomic add/sub may be implemented as
 * C code using pmix_atomic_compare_exchange.  The appearance of atomic
 * operation will be upheld in these cases.
 */

#ifndef PMIX_SYS_ATOMIC_H
#define PMIX_SYS_ATOMIC_H 1

#include "pmix_config.h"

#include <stdbool.h>

#include "src/atomics/sys/architecture.h"
#include "src/include/pmix_stdatomic.h"

#if PMIX_ASSEMBLY_BUILTIN == PMIX_BUILTIN_C11

#include "atomic_stdc.h"

#else /* !PMIX_C_HAVE__ATOMIC */

/* do some quick #define cleanup in cases where we are doing
   testing... */
#ifdef PMIX_DISABLE_INLINE_ASM
#undef PMIX_C_GCC_INLINE_ASSEMBLY
#define PMIX_C_GCC_INLINE_ASSEMBLY 0
#endif

/* define PMIX_{GCC,DEC,XLC}_INLINE_ASSEMBLY based on the
   PMIX_C_{GCC,DEC,XLC}_INLINE_ASSEMBLY defines and whether we
   are in C or C++ */
#if defined(c_plusplus) || defined(__cplusplus)
/* We no longer support inline assembly for C++ as PMIX is a C-only interface */
#define PMIX_GCC_INLINE_ASSEMBLY 0
#else
#define PMIX_GCC_INLINE_ASSEMBLY PMIX_C_GCC_INLINE_ASSEMBLY
#endif


BEGIN_C_DECLS
/**********************************************************************
 *
 * Data structures for atomic ops
 *
 *********************************************************************/
/**
 * Volatile lock object (with optional padding).
 *
 * \note The internals of the lock are included here, but should be
 * considered private.  The implementation currently in use may choose
 * to use an int or unsigned char as the lock value - the user is not
 * informed either way.
 */
struct pmix_atomic_lock_t {
    union {
        pmix_atomic_int32_t lock;     /**< The lock address (an integer) */
        volatile unsigned char sparc_lock; /**< The lock address on sparc */
        char padding[sizeof(int)]; /**< Array for optional padding */
    } u;
};
typedef struct pmix_atomic_lock_t pmix_atomic_lock_t;

/**********************************************************************
 *
 * Set or unset these macros in the architecture-specific atomic.h
 * files if we need to specify them as inline or non-inline
 *
 *********************************************************************/
#if !PMIX_GCC_INLINE_ASSEMBLY
#define PMIX_HAVE_INLINE_ATOMIC_MEM_BARRIER 0
#define PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32 0
#define PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64 0
#define PMIX_HAVE_INLINE_ATOMIC_ADD_32 0
#define PMIX_HAVE_INLINE_ATOMIC_AND_32 0
#define PMIX_HAVE_INLINE_ATOMIC_OR_32 0
#define PMIX_HAVE_INLINE_ATOMIC_XOR_32 0
#define PMIX_HAVE_INLINE_ATOMIC_SUB_32 0
#define PMIX_HAVE_INLINE_ATOMIC_ADD_64 0
#define PMIX_HAVE_INLINE_ATOMIC_AND_64 0
#define PMIX_HAVE_INLINE_ATOMIC_OR_64 0
#define PMIX_HAVE_INLINE_ATOMIC_XOR_64 0
#define PMIX_HAVE_INLINE_ATOMIC_SUB_64 0
#define PMIX_HAVE_INLINE_ATOMIC_SWAP_32 0
#define PMIX_HAVE_INLINE_ATOMIC_SWAP_64 0
#else
#define PMIX_HAVE_INLINE_ATOMIC_MEM_BARRIER 1
#define PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32 1
#define PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64 1
#define PMIX_HAVE_INLINE_ATOMIC_ADD_32 1
#define PMIX_HAVE_INLINE_ATOMIC_AND_32 1
#define PMIX_HAVE_INLINE_ATOMIC_OR_32 1
#define PMIX_HAVE_INLINE_ATOMIC_XOR_32 1
#define PMIX_HAVE_INLINE_ATOMIC_SUB_32 1
#define PMIX_HAVE_INLINE_ATOMIC_ADD_64 1
#define PMIX_HAVE_INLINE_ATOMIC_AND_64 1
#define PMIX_HAVE_INLINE_ATOMIC_OR_64 1
#define PMIX_HAVE_INLINE_ATOMIC_XOR_64 1
#define PMIX_HAVE_INLINE_ATOMIC_SUB_64 1
#define PMIX_HAVE_INLINE_ATOMIC_SWAP_32 1
#define PMIX_HAVE_INLINE_ATOMIC_SWAP_64 1
#endif

/**
 * Enumeration of lock states
 */
enum {
    PMIX_ATOMIC_LOCK_UNLOCKED = 0,
    PMIX_ATOMIC_LOCK_LOCKED = 1
};

#define PMIX_ATOMIC_LOCK_INIT {.u = {.lock = PMIX_ATOMIC_LOCK_UNLOCKED}}

/**********************************************************************
 *
 * Load the appropriate architecture files and set some reasonable
 * default values for our support
 *
 *********************************************************************/
#if defined(DOXYGEN)
/* don't include system-level gorp when generating doxygen files */
#elif PMIX_ASSEMBLY_BUILTIN == PMIX_BUILTIN_SYNC
#include "src/atomics/sys/sync_builtin/atomic.h"
#elif PMIX_ASSEMBLY_BUILTIN == PMIX_BUILTIN_GCC
#include "src/atomics/sys/gcc_builtin/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_X86_64
#include "src/atomics/sys/x86_64/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_ARM
#include "src/atomics/sys/arm/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_ARM64
#include "src/atomics/sys/arm64/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_IA32
#include "src/atomics/sys/ia32/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_IA64
#error #include "src/atomics/sys/ia64/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_MIPS
#error #include "src/atomics/sys/mips/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_POWERPC32
#include "src/atomics/sys/powerpc/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_POWERPC64
#include "src/atomics/sys/powerpc/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_SPARC
#error #include "src/atomics/sys/sparc/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_SPARCV9_32
#include "src/atomics/sys/sparcv9/atomic.h"
#elif PMIX_ASSEMBLY_ARCH == PMIX_SPARCV9_64
#include "src/atomics/sys/sparcv9/atomic.h"
#endif

#ifndef DOXYGEN
/* compare and set operations can't really be emulated from software,
   so if these defines aren't already set, they should be set to 0
   now */
#ifndef PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32
#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 0
#endif
#ifndef PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64
#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64 0
#endif
#ifndef PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_128
#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_128 0
#endif
#ifndef PMIX_HAVE_ATOMIC_LLSC_32
#define PMIX_HAVE_ATOMIC_LLSC_32 0
#endif
#ifndef PMIX_HAVE_ATOMIC_LLSC_64
#define PMIX_HAVE_ATOMIC_LLSC_64 0
#endif
#endif /* DOXYGEN */

/**********************************************************************
 *
 * Memory Barriers - defined here if running doxygen or have barriers
 *                   but can't inline
 *
 *********************************************************************/
#if !defined(PMIX_HAVE_ATOMIC_MEM_BARRIER) && !defined(DOXYGEN)
/* no way to emulate in C code */
#define PMIX_HAVE_ATOMIC_MEM_BARRIER 0
#endif

#if defined(DOXYGEN) || PMIX_HAVE_ATOMIC_MEM_BARRIER
/**
 * Memory barrier
 *
 * Will use system-specific features to instruct the processor and
 * memory controller that all writes and reads that have been posted
 * before the call to \c pmix_atomic_mb() must appear to have
 * completed before the next read or write.
 *
 * \note This can have some expensive side effects, including flushing
 * the pipeline, preventing the cpu from reordering instructions, and
 * generally grinding the memory controller's performance.  Use only
 * if you need *both* read and write barriers.
 */

#if PMIX_HAVE_INLINE_ATOMIC_MEM_BARRIER
static inline
#endif
void pmix_atomic_mb(void);

/**
 * Read memory barrier
 *
 * Use system-specific features to instruct the processor and memory
 * conrtoller that all reads that have been posted before the call to
 * \c pmix_atomic_rmb() must appear to have been completed before the
 * next read.  Nothing is said about the ordering of writes when using
 * \c pmix_atomic_rmb().
 */

#if PMIX_HAVE_INLINE_ATOMIC_MEM_BARRIER
static inline
#endif
void pmix_atomic_rmb(void);

/**
 * Write memory barrier.
 *
 * Use system-specific features to instruct the processor and memory
 * conrtoller that all writes that have been posted before the call to
 * \c pmix_atomic_wmb() must appear to have been completed before the
 * next write.  Nothing is said about the ordering of reads when using
 * \c pmix_atomic_wmb().
 */

#if PMIX_HAVE_INLINE_ATOMIC_MEM_BARRIER
static inline
#endif
void pmix_atomic_wmb(void);

#endif /* defined(DOXYGEN) || PMIX_HAVE_ATOMIC_MEM_BARRIER */


/**********************************************************************
 *
 * Atomic spinlocks - always inlined, if have atomic compare-and-swap
 *
 *********************************************************************/

#if !defined(PMIX_HAVE_ATOMIC_SPINLOCKS) && !defined(DOXYGEN)
/* 0 is more like "pending" - we'll fix up at the end after all
   the static inline functions are declared */
#define PMIX_HAVE_ATOMIC_SPINLOCKS 0
#endif

#if defined(DOXYGEN) || PMIX_HAVE_ATOMIC_SPINLOCKS || (PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64)

/**
 * Initialize a lock to value
 *
 * @param lock         Address of the lock
 * @param value        Initial value to set lock to
 */
#if PMIX_HAVE_ATOMIC_SPINLOCKS == 0
static inline
#endif
void pmix_atomic_lock_init(pmix_atomic_lock_t* lock, int32_t value);


/**
 * Try to acquire a lock.
 *
 * @param lock          Address of the lock.
 * @return              0 if the lock was acquired, 1 otherwise.
 */
#if PMIX_HAVE_ATOMIC_SPINLOCKS == 0
static inline
#endif
int pmix_atomic_trylock(pmix_atomic_lock_t *lock);


/**
 * Acquire a lock by spinning.
 *
 * @param lock          Address of the lock.
 */
#if PMIX_HAVE_ATOMIC_SPINLOCKS == 0
static inline
#endif
void pmix_atomic_lock(pmix_atomic_lock_t *lock);


/**
 * Release a lock.
 *
 * @param lock          Address of the lock.
 */
#if PMIX_HAVE_ATOMIC_SPINLOCKS == 0
static inline
#endif
void pmix_atomic_unlock(pmix_atomic_lock_t *lock);


#if PMIX_HAVE_ATOMIC_SPINLOCKS == 0
#undef PMIX_HAVE_ATOMIC_SPINLOCKS
#define PMIX_HAVE_ATOMIC_SPINLOCKS (PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64)
#define PMIX_NEED_INLINE_ATOMIC_SPINLOCKS 1
#endif

#endif /* PMIX_HAVE_ATOMIC_SPINLOCKS */


/**********************************************************************
 *
 * Atomic math operations
 *
 *********************************************************************/
#if !defined(PMIX_HAVE_ATOMIC_CMPSET_32) && !defined(DOXYGEN)
#define PMIX_HAVE_ATOMIC_CMPSET_32 0
#endif
#if defined(DOXYGEN) || PMIX_HAVE_ATOMIC_CMPSET_32

#if PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32
static inline
#endif
bool pmix_atomic_compare_exchange_strong_32 (pmix_atomic_int32_t *addr, int32_t *oldval,
                                             int32_t newval);

#if PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32
static inline
#endif
bool pmix_atomic_compare_exchange_strong_acq_32 (pmix_atomic_int32_t *addr, int32_t *oldval,
                                                 int32_t newval);

#if PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32
static inline
#endif
bool pmix_atomic_compare_exchange_strong_rel_32 (pmix_atomic_int32_t *addr, int32_t *oldval,
                                                 int32_t newval);
#endif


#if !defined(PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64) && !defined(DOXYGEN)
#define PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64 0
#endif
#if defined(DOXYGEN) || PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64

#if PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64
static inline
#endif
bool pmix_atomic_compare_exchange_strong_64 (pmix_atomic_int64_t *addr, int64_t *oldval,
                                             int64_t newval);

#if PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64
static inline
#endif
bool pmix_atomic_compare_exchange_strong_acq_64 (pmix_atomic_int64_t *addr, int64_t *oldval,
                                                 int64_t newval);

#if PMIX_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64
static inline
#endif
bool pmix_atomic_compare_exchange_strong_rel_64 (pmix_atomic_int64_t *addr, int64_t *oldval,
                                                 int64_t newval);

#endif

#if !defined(PMIX_HAVE_ATOMIC_MATH_32) && !defined(DOXYGEN)
  /* define to 0 for these tests.  WIll fix up later. */
  #define PMIX_HAVE_ATOMIC_MATH_32 0
#endif

#if defined(DOXYGEN) || PMIX_HAVE_ATOMIC_MATH_32 || PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32

static inline int32_t pmix_atomic_add_fetch_32(pmix_atomic_int32_t *addr, int delta);
static inline int32_t pmix_atomic_fetch_add_32(pmix_atomic_int32_t *addr, int delta);
static inline int32_t pmix_atomic_and_fetch_32(pmix_atomic_int32_t *addr, int32_t value);
static inline int32_t pmix_atomic_fetch_and_32(pmix_atomic_int32_t *addr, int32_t value);
static inline int32_t pmix_atomic_or_fetch_32(pmix_atomic_int32_t *addr, int32_t value);
static inline int32_t pmix_atomic_fetch_or_32(pmix_atomic_int32_t *addr, int32_t value);
static inline int32_t pmix_atomic_xor_fetch_32(pmix_atomic_int32_t *addr, int32_t value);
static inline int32_t pmix_atomic_fetch_xor_32(pmix_atomic_int32_t *addr, int32_t value);
static inline int32_t pmix_atomic_sub_fetch_32(pmix_atomic_int32_t *addr, int delta);
static inline int32_t pmix_atomic_fetch_sub_32(pmix_atomic_int32_t *addr, int delta);
static inline int32_t pmix_atomic_min_fetch_32 (pmix_atomic_int32_t *addr, int32_t value);
static inline int32_t pmix_atomic_fetch_min_32 (pmix_atomic_int32_t *addr, int32_t value);
static inline int32_t pmix_atomic_max_fetch_32 (pmix_atomic_int32_t *addr, int32_t value);
static inline int32_t pmix_atomic_fetch_max_32 (pmix_atomic_int32_t *addr, int32_t value);

#endif /* PMIX_HAVE_ATOMIC_MATH_32 */

#if ! PMIX_HAVE_ATOMIC_MATH_32
/* fix up the value of pmix_have_atomic_math_32 to allow for C versions */
#undef PMIX_HAVE_ATOMIC_MATH_32
#define PMIX_HAVE_ATOMIC_MATH_32 PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32
#endif

#ifndef PMIX_HAVE_ATOMIC_MATH_64
/* define to 0 for these tests.  WIll fix up later. */
#define PMIX_HAVE_ATOMIC_MATH_64 0
#endif

#if defined(DOXYGEN) || PMIX_HAVE_ATOMIC_MATH_64 || PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64

static inline int64_t pmix_atomic_add_fetch_64(pmix_atomic_int64_t *addr, int64_t delta);
static inline int64_t pmix_atomic_fetch_add_64(pmix_atomic_int64_t *addr, int64_t delta);
static inline int64_t pmix_atomic_and_fetch_64(pmix_atomic_int64_t *addr, int64_t value);
static inline int64_t pmix_atomic_fetch_and_64(pmix_atomic_int64_t *addr, int64_t value);
static inline int64_t pmix_atomic_or_fetch_64(pmix_atomic_int64_t *addr, int64_t value);
static inline int64_t pmix_atomic_fetch_or_64(pmix_atomic_int64_t *addr, int64_t value);
static inline int64_t pmix_atomic_fetch_xor_64(pmix_atomic_int64_t *addr, int64_t value);
static inline int64_t pmix_atomic_sub_fetch_64(pmix_atomic_int64_t *addr, int64_t delta);
static inline int64_t pmix_atomic_fetch_sub_64(pmix_atomic_int64_t *addr, int64_t delta);
static inline int64_t pmix_atomic_min_fetch_64 (pmix_atomic_int64_t *addr, int64_t value);
static inline int64_t pmix_atomic_fetch_min_64 (pmix_atomic_int64_t *addr, int64_t value);
static inline int64_t pmix_atomic_max_fetch_64 (pmix_atomic_int64_t *addr, int64_t value);
static inline int64_t pmix_atomic_fetch_max_64 (pmix_atomic_int64_t *addr, int64_t value);

#endif /* PMIX_HAVE_ATOMIC_MATH_64 */

#if ! PMIX_HAVE_ATOMIC_MATH_64
/* fix up the value of pmix_have_atomic_math_64 to allow for C versions */
#undef PMIX_HAVE_ATOMIC_MATH_64
#define PMIX_HAVE_ATOMIC_MATH_64 PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64
#endif

/* provide a size_t add/subtract.  When in debug mode, make it an
 * inline function so that we don't have any casts in the
 *  interface and can catch type errors.  When not in debug mode,
 * just make it a macro, so that there's no performance penalty
 */
#if defined(DOXYGEN) || PMIX_ENABLE_DEBUG
static inline size_t
pmix_atomic_add_fetch_size_t(pmix_atomic_size_t *addr, size_t delta)
{
#if SIZEOF_SIZE_T == 4
    return (size_t) pmix_atomic_add_fetch_32((int32_t*) addr, delta);
#elif SIZEOF_SIZE_T == 8
    return (size_t) pmix_atomic_add_fetch_64((int64_t*) addr, delta);
#else
#error "Unknown size_t size"
#endif
}

static inline size_t
pmix_atomic_fetch_add_size_t(pmix_atomic_size_t *addr, size_t delta)
{
#if SIZEOF_SIZE_T == 4
    return (size_t) pmix_atomic_fetch_add_32((int32_t*) addr, delta);
#elif SIZEOF_SIZE_T == 8
    return (size_t) pmix_atomic_fetch_add_64((int64_t*) addr, delta);
#else
#error "Unknown size_t size"
#endif
}

static inline size_t
pmix_atomic_sub_fetch_size_t(pmix_atomic_size_t *addr, size_t delta)
{
#if SIZEOF_SIZE_T == 4
    return (size_t) pmix_atomic_sub_fetch_32((int32_t*) addr, delta);
#elif SIZEOF_SIZE_T == 8
    return (size_t) pmix_atomic_sub_fetch_64((int64_t*) addr, delta);
#else
#error "Unknown size_t size"
#endif
}

static inline size_t
pmix_atomic_fetch_sub_size_t(pmix_atomic_size_t *addr, size_t delta)
{
#if SIZEOF_SIZE_T == 4
    return (size_t) pmix_atomic_fetch_sub_32((int32_t*) addr, delta);
#elif SIZEOF_SIZE_T == 8
    return (size_t) pmix_atomic_fetch_sub_64((int64_t*) addr, delta);
#else
#error "Unknown size_t size"
#endif
}

#else
#if SIZEOF_SIZE_T == 4
#define pmix_atomic_add_fetch_size_t(addr, delta) ((size_t) pmix_atomic_add_fetch_32((pmix_atomic_int32_t *) addr, delta))
#define pmix_atomic_fetch_add_size_t(addr, delta) ((size_t) pmix_atomic_fetch_add_32((pmix_atomic_int32_t *) addr, delta))
#define pmix_atomic_sub_fetch_size_t(addr, delta) ((size_t) pmix_atomic_sub_fetch_32((pmix_atomic_int32_t *) addr, delta))
#define pmix_atomic_fetch_sub_size_t(addr, delta) ((size_t) pmix_atomic_fetch_sub_32((pmix_atomic_int32_t *) addr, delta))
#elif SIZEOF_SIZE_T == 8
#define pmix_atomic_add_fetch_size_t(addr, delta) ((size_t) pmix_atomic_add_fetch_64((pmix_atomic_int64_t *) addr, delta))
#define pmix_atomic_fetch_add_size_t(addr, delta) ((size_t) pmix_atomic_fetch_add_64((pmix_atomic_int64_t *) addr, delta))
#define pmix_atomic_sub_fetch_size_t(addr, delta) ((size_t) pmix_atomic_sub_fetch_64((pmix_atomic_int64_t *) addr, delta))
#define pmix_atomic_fetch_sub_size_t(addr, delta) ((size_t) pmix_atomic_fetch_sub_64((pmix_atomic_int64_t *) addr, delta))
#else
#error "Unknown size_t size"
#endif
#endif

#if defined(DOXYGEN) || (PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64)
/* these are always done with inline functions, so always mark as
   static inline */

static inline bool pmix_atomic_compare_exchange_strong_xx (pmix_atomic_intptr_t *addr, intptr_t *oldval,
                                                           int64_t newval, size_t length);
static inline bool pmix_atomic_compare_exchange_strong_acq_xx (pmix_atomic_intptr_t *addr, intptr_t *oldval,
                                                               int64_t newval, size_t length);
static inline bool pmix_atomic_compare_exchange_strong_rel_xx (pmix_atomic_intptr_t *addr, intptr_t *oldval,
                                                               int64_t newval, size_t length);


static inline bool pmix_atomic_compare_exchange_strong_ptr (pmix_atomic_intptr_t* addr, intptr_t *oldval,
                                                            intptr_t newval);
static inline bool pmix_atomic_compare_exchange_strong_acq_ptr (pmix_atomic_intptr_t* addr, intptr_t *oldval,
                                                                intptr_t newval);
static inline bool pmix_atomic_compare_exchange_strong_rel_ptr (pmix_atomic_intptr_t* addr, intptr_t *oldval,
                                                                intptr_t newval);

/**
 * Atomic compare and set of generic type with relaxed semantics. This
 * macro detect at compile time the type of the first argument and
 * choose the correct function to be called.
 *
 * \note This macro should only be used for integer types.
 *
 * @param addr          Address of <TYPE>.
 * @param oldval        Comparison value address of <TYPE>.
 * @param newval        New value to set if comparision is true <TYPE>.
 *
 * See pmix_atomic_compare_exchange_* for pseudo-code.
 */
#define pmix_atomic_compare_exchange_strong( ADDR, OLDVAL, NEWVAL )                  \
    pmix_atomic_compare_exchange_strong_xx( (pmix_atomic_intptr_t*)(ADDR), (intptr_t *)(OLDVAL), \
                                            (intptr_t)(NEWVAL), sizeof(*(ADDR)) )

/**
 * Atomic compare and set of generic type with acquire semantics. This
 * macro detect at compile time the type of the first argument and
 * choose the correct function to be called.
 *
 * \note This macro should only be used for integer types.
 *
 * @param addr          Address of <TYPE>.
 * @param oldval        Comparison value address of <TYPE>.
 * @param newval        New value to set if comparision is true <TYPE>.
 *
 * See pmix_atomic_compare_exchange_acq_* for pseudo-code.
 */
#define pmix_atomic_compare_exchange_strong_acq( ADDR, OLDVAL, NEWVAL )                  \
    pmix_atomic_compare_exchange_strong_acq_xx( (pmix_atomic_intptr_t*)(ADDR), (intptr_t *)(OLDVAL), \
                                                (intptr_t)(NEWVAL), sizeof(*(ADDR)) )

/**
 * Atomic compare and set of generic type with release semantics. This
 * macro detect at compile time the type of the first argument and
 * choose the correct function to be called.
 *
 * \note This macro should only be used for integer types.
 *
 * @param addr          Address of <TYPE>.
 * @param oldval        Comparison value address of <TYPE>.
 * @param newval        New value to set if comparision is true <TYPE>.
 *
 * See pmix_atomic_compare_exchange_rel_* for pseudo-code.
 */
#define pmix_atomic_compare_exchange_strong_rel( ADDR, OLDVAL, NEWVAL ) \
    pmix_atomic_compare_exchange_strong_rel_xx( (pmix_atomic_intptr_t*)(ADDR), (intptr_t *)(OLDVAL), \
                                                (intptr_t)(NEWVAL), sizeof(*(ADDR)) )


#endif /* (PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || PMIX_HAVE_ATOMIC_COMPARE_EXCHANGE_64) */

#if defined(DOXYGEN) || (PMIX_HAVE_ATOMIC_MATH_32 || PMIX_HAVE_ATOMIC_MATH_64)

static inline void pmix_atomic_add_xx(pmix_atomic_intptr_t* addr,
                                      int32_t value, size_t length);
static inline void pmix_atomic_sub_xx(pmix_atomic_intptr_t* addr,
                                      int32_t value, size_t length);

static inline intptr_t pmix_atomic_add_fetch_ptr( pmix_atomic_intptr_t* addr, void* delta );
static inline intptr_t pmix_atomic_fetch_add_ptr( pmix_atomic_intptr_t* addr, void* delta );
static inline intptr_t pmix_atomic_sub_fetch_ptr( pmix_atomic_intptr_t* addr, void* delta );
static inline intptr_t pmix_atomic_fetch_sub_ptr( pmix_atomic_intptr_t* addr, void* delta );

/**
 * Atomically increment the content depending on the type. This
 * macro detect at compile time the type of the first argument
 * and choose the correct function to be called.
 *
 * \note This macro should only be used for integer types.
 *
 * @param addr          Address of <TYPE>
 * @param delta         Value to add (converted to <TYPE>).
 */
#define pmix_atomic_add( ADDR, VALUE )                                  \
   pmix_atomic_add_xx( (pmix_atomic_intptr_t*)(ADDR), (int32_t)(VALUE), \
                       sizeof(*(ADDR)) )

/**
 * Atomically decrement the content depending on the type. This
 * macro detect at compile time the type of the first argument
 * and choose the correct function to be called.
 *
 * \note This macro should only be used for integer types.
 *
 * @param addr          Address of <TYPE>
 * @param delta         Value to substract (converted to <TYPE>).
 */
#define pmix_atomic_sub( ADDR, VALUE )                                  \
   pmix_atomic_sub_xx( (pmix_atomic_intptr_t*)(ADDR), (int32_t)(VALUE),        \
                      sizeof(*(ADDR)) )

#endif /* PMIX_HAVE_ATOMIC_MATH_32 || PMIX_HAVE_ATOMIC_MATH_64 */


/*
 * Include inline implementations of everything not defined directly
 * in assembly
 */
#include "src/atomics/sys/atomic_impl.h"

#endif /* !PMIX_C_HAVE__ATOMIC */

END_C_DECLS

#endif /* PMIX_SYS_ATOMIC_H */
