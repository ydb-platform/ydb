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
 *  - \c OPAL_HAVE_ATOMIC_MEM_BARRIER atomic memory barriers
 *  - \c OPAL_HAVE_ATOMIC_SPINLOCKS atomic spinlocks
 *  - \c OPAL_HAVE_ATOMIC_MATH_32 if 32 bit add/sub/compare-exchange can be done "atomicly"
 *  - \c OPAL_HAVE_ATOMIC_MATH_64 if 64 bit add/sub/compare-exchange can be done "atomicly"
 *
 * Note that for the Atomic math, atomic add/sub may be implemented as
 * C code using opal_atomic_compare_exchange.  The appearance of atomic
 * operation will be upheld in these cases.
 */

#ifndef OPAL_SYS_ATOMIC_H
#define OPAL_SYS_ATOMIC_H 1

#include "opal_config.h"

#include <stdbool.h>

#include "opal/sys/architecture.h"
#include "opal_stdint.h"

/* do some quick #define cleanup in cases where we are doing
   testing... */
#ifdef OPAL_DISABLE_INLINE_ASM
#undef OPAL_C_GCC_INLINE_ASSEMBLY
#define OPAL_C_GCC_INLINE_ASSEMBLY 0
#endif

/* define OPAL_{GCC,DEC,XLC}_INLINE_ASSEMBLY based on the
   OPAL_C_{GCC,DEC,XLC}_INLINE_ASSEMBLY defines and whether we
   are in C or C++ */
#if defined(c_plusplus) || defined(__cplusplus)
/* We no longer support inline assembly for C++ as OPAL is a C-only interface */
#define OPAL_GCC_INLINE_ASSEMBLY 0
#else
#define OPAL_GCC_INLINE_ASSEMBLY OPAL_C_GCC_INLINE_ASSEMBLY
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
struct opal_atomic_lock_t {
    union {
        volatile int32_t lock;     /**< The lock address (an integer) */
        volatile unsigned char sparc_lock; /**< The lock address on sparc */
        char padding[sizeof(int)]; /**< Array for optional padding */
    } u;
};
typedef struct opal_atomic_lock_t opal_atomic_lock_t;

/**********************************************************************
 *
 * Set or unset these macros in the architecture-specific atomic.h
 * files if we need to specify them as inline or non-inline
 *
 *********************************************************************/
#if !OPAL_GCC_INLINE_ASSEMBLY
#define OPAL_HAVE_INLINE_ATOMIC_MEM_BARRIER 0
#define OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32 0
#define OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64 0
#define OPAL_HAVE_INLINE_ATOMIC_ADD_32 0
#define OPAL_HAVE_INLINE_ATOMIC_AND_32 0
#define OPAL_HAVE_INLINE_ATOMIC_OR_32 0
#define OPAL_HAVE_INLINE_ATOMIC_XOR_32 0
#define OPAL_HAVE_INLINE_ATOMIC_SUB_32 0
#define OPAL_HAVE_INLINE_ATOMIC_ADD_64 0
#define OPAL_HAVE_INLINE_ATOMIC_AND_64 0
#define OPAL_HAVE_INLINE_ATOMIC_OR_64 0
#define OPAL_HAVE_INLINE_ATOMIC_XOR_64 0
#define OPAL_HAVE_INLINE_ATOMIC_SUB_64 0
#define OPAL_HAVE_INLINE_ATOMIC_SWAP_32 0
#define OPAL_HAVE_INLINE_ATOMIC_SWAP_64 0
#else
#define OPAL_HAVE_INLINE_ATOMIC_MEM_BARRIER 1
#define OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32 1
#define OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64 1
#define OPAL_HAVE_INLINE_ATOMIC_ADD_32 1
#define OPAL_HAVE_INLINE_ATOMIC_AND_32 1
#define OPAL_HAVE_INLINE_ATOMIC_OR_32 1
#define OPAL_HAVE_INLINE_ATOMIC_XOR_32 1
#define OPAL_HAVE_INLINE_ATOMIC_SUB_32 1
#define OPAL_HAVE_INLINE_ATOMIC_ADD_64 1
#define OPAL_HAVE_INLINE_ATOMIC_AND_64 1
#define OPAL_HAVE_INLINE_ATOMIC_OR_64 1
#define OPAL_HAVE_INLINE_ATOMIC_XOR_64 1
#define OPAL_HAVE_INLINE_ATOMIC_SUB_64 1
#define OPAL_HAVE_INLINE_ATOMIC_SWAP_32 1
#define OPAL_HAVE_INLINE_ATOMIC_SWAP_64 1
#endif

/**
 * Enumeration of lock states
 */
enum {
    OPAL_ATOMIC_LOCK_UNLOCKED = 0,
    OPAL_ATOMIC_LOCK_LOCKED = 1
};

/**********************************************************************
 *
 * Load the appropriate architecture files and set some reasonable
 * default values for our support
 *
 *********************************************************************/
#if defined(DOXYGEN)
/* don't include system-level gorp when generating doxygen files */
#elif OPAL_ASSEMBLY_BUILTIN == OPAL_BUILTIN_SYNC
#include "opal/sys/sync_builtin/atomic.h"
#elif OPAL_ASSEMBLY_BUILTIN == OPAL_BUILTIN_GCC
#include "opal/sys/gcc_builtin/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_X86_64
#include "opal/sys/x86_64/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_ARM
#include "opal/sys/arm/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_ARM64
#include "opal/sys/arm64/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_IA32
#include "opal/sys/ia32/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_IA64
#error #include "opal/sys/ia64/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_MIPS
#error #include "opal/sys/mips/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_POWERPC32
#include "opal/sys/powerpc/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_POWERPC64
#include "opal/sys/powerpc/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_SPARC
#error #include "opal/sys/sparc/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_SPARCV9_32
#include "opal/sys/sparcv9/atomic.h"
#elif OPAL_ASSEMBLY_ARCH == OPAL_SPARCV9_64
#include "opal/sys/sparcv9/atomic.h"
#endif

#ifndef DOXYGEN
/* compare and set operations can't really be emulated from software,
   so if these defines aren't already set, they should be set to 0
   now */
#ifndef OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32
#define OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32 0
#endif
#ifndef OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64
#define OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64 0
#endif
#ifndef OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_128
#define OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_128 0
#endif
#ifndef OPAL_HAVE_ATOMIC_LLSC_32
#define OPAL_HAVE_ATOMIC_LLSC_32 0
#endif
#ifndef OPAL_HAVE_ATOMIC_LLSC_64
#define OPAL_HAVE_ATOMIC_LLSC_64 0
#endif
#endif /* DOXYGEN */

/**********************************************************************
 *
 * Memory Barriers - defined here if running doxygen or have barriers
 *                   but can't inline
 *
 *********************************************************************/
#if !defined(OPAL_HAVE_ATOMIC_MEM_BARRIER) && !defined(DOXYGEN)
/* no way to emulate in C code */
#define OPAL_HAVE_ATOMIC_MEM_BARRIER 0
#endif

#if defined(DOXYGEN) || OPAL_HAVE_ATOMIC_MEM_BARRIER
/**
 * Memory barrier
 *
 * Will use system-specific features to instruct the processor and
 * memory controller that all writes and reads that have been posted
 * before the call to \c opal_atomic_mb() must appear to have
 * completed before the next read or write.
 *
 * \note This can have some expensive side effects, including flushing
 * the pipeline, preventing the cpu from reordering instructions, and
 * generally grinding the memory controller's performance.  Use only
 * if you need *both* read and write barriers.
 */

#if OPAL_HAVE_INLINE_ATOMIC_MEM_BARRIER
static inline
#endif
void opal_atomic_mb(void);

/**
 * Read memory barrier
 *
 * Use system-specific features to instruct the processor and memory
 * conrtoller that all reads that have been posted before the call to
 * \c opal_atomic_rmb() must appear to have been completed before the
 * next read.  Nothing is said about the ordering of writes when using
 * \c opal_atomic_rmb().
 */

#if OPAL_HAVE_INLINE_ATOMIC_MEM_BARRIER
static inline
#endif
void opal_atomic_rmb(void);

/**
 * Write memory barrier.
 *
 * Use system-specific features to instruct the processor and memory
 * conrtoller that all writes that have been posted before the call to
 * \c opal_atomic_wmb() must appear to have been completed before the
 * next write.  Nothing is said about the ordering of reads when using
 * \c opal_atomic_wmb().
 */

#if OPAL_HAVE_INLINE_ATOMIC_MEM_BARRIER
static inline
#endif
void opal_atomic_wmb(void);

#endif /* defined(DOXYGEN) || OPAL_HAVE_ATOMIC_MEM_BARRIER */


/**********************************************************************
 *
 * Atomic spinlocks - always inlined, if have atomic compare-and-swap
 *
 *********************************************************************/

#if !defined(OPAL_HAVE_ATOMIC_SPINLOCKS) && !defined(DOXYGEN)
/* 0 is more like "pending" - we'll fix up at the end after all
   the static inline functions are declared */
#define OPAL_HAVE_ATOMIC_SPINLOCKS 0
#endif

#if defined(DOXYGEN) || OPAL_HAVE_ATOMIC_SPINLOCKS || (OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64)

/**
 * Initialize a lock to value
 *
 * @param lock         Address of the lock
 * @param value        Initial value to set lock to
 */
#if OPAL_HAVE_ATOMIC_SPINLOCKS == 0
static inline
#endif
void opal_atomic_lock_init(opal_atomic_lock_t* lock, int32_t value);


/**
 * Try to acquire a lock.
 *
 * @param lock          Address of the lock.
 * @return              0 if the lock was acquired, 1 otherwise.
 */
#if OPAL_HAVE_ATOMIC_SPINLOCKS == 0
static inline
#endif
int opal_atomic_trylock(opal_atomic_lock_t *lock);


/**
 * Acquire a lock by spinning.
 *
 * @param lock          Address of the lock.
 */
#if OPAL_HAVE_ATOMIC_SPINLOCKS == 0
static inline
#endif
void opal_atomic_lock(opal_atomic_lock_t *lock);


/**
 * Release a lock.
 *
 * @param lock          Address of the lock.
 */
#if OPAL_HAVE_ATOMIC_SPINLOCKS == 0
static inline
#endif
void opal_atomic_unlock(opal_atomic_lock_t *lock);


#if OPAL_HAVE_ATOMIC_SPINLOCKS == 0
#undef OPAL_HAVE_ATOMIC_SPINLOCKS
#define OPAL_HAVE_ATOMIC_SPINLOCKS (OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64)
#define OPAL_NEED_INLINE_ATOMIC_SPINLOCKS 1
#endif

#endif /* OPAL_HAVE_ATOMIC_SPINLOCKS */


/**********************************************************************
 *
 * Atomic math operations
 *
 *********************************************************************/
#if !defined(OPAL_HAVE_ATOMIC_CMPSET_32) && !defined(DOXYGEN)
#define OPAL_HAVE_ATOMIC_CMPSET_32 0
#endif
#if defined(DOXYGEN) || OPAL_HAVE_ATOMIC_CMPSET_32

#if OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32
static inline
#endif
bool opal_atomic_compare_exchange_strong_32 (volatile int32_t *addr, int32_t *oldval,
                                             int32_t newval);

#if OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32
static inline
#endif
bool opal_atomic_compare_exchange_strong_acq_32 (volatile int32_t *addr, int32_t *oldval,
                                                 int32_t newval);

#if OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_32
static inline
#endif
bool opal_atomic_compare_exchange_strong_rel_32 (volatile int32_t *addr, int32_t *oldval,
                                                 int32_t newval);
#endif


#if !defined(OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64) && !defined(DOXYGEN)
#define OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64 0
#endif
#if defined(DOXYGEN) || OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64

#if OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64
static inline
#endif
bool opal_atomic_compare_exchange_strong_64 (volatile int64_t *addr, int64_t *oldval,
                                             int64_t newval);

#if OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64
static inline
#endif
bool opal_atomic_compare_exchange_strong_acq_64 (volatile int64_t *addr, int64_t *oldval,
                                                 int64_t newval);

#if OPAL_HAVE_INLINE_ATOMIC_COMPARE_EXCHANGE_64
static inline
#endif
bool opal_atomic_compare_exchange_strong_rel_64 (volatile int64_t *addr, int64_t *oldval,
                                                 int64_t newval);

#endif

#if !defined(OPAL_HAVE_ATOMIC_MATH_32) && !defined(DOXYGEN)
  /* define to 0 for these tests.  WIll fix up later. */
  #define OPAL_HAVE_ATOMIC_MATH_32 0
#endif

#if defined(DOXYGEN) || OPAL_HAVE_ATOMIC_MATH_32 || OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32

static inline int32_t opal_atomic_add_fetch_32(volatile int32_t *addr, int delta);
static inline int32_t opal_atomic_fetch_add_32(volatile int32_t *addr, int delta);
static inline int32_t opal_atomic_and_fetch_32(volatile int32_t *addr, int32_t value);
static inline int32_t opal_atomic_fetch_and_32(volatile int32_t *addr, int32_t value);
static inline int32_t opal_atomic_or_fetch_32(volatile int32_t *addr, int32_t value);
static inline int32_t opal_atomic_fetch_or_32(volatile int32_t *addr, int32_t value);
static inline int32_t opal_atomic_xor_fetch_32(volatile int32_t *addr, int32_t value);
static inline int32_t opal_atomic_fetch_xor_32(volatile int32_t *addr, int32_t value);
static inline int32_t opal_atomic_sub_fetch_32(volatile int32_t *addr, int delta);
static inline int32_t opal_atomic_fetch_sub_32(volatile int32_t *addr, int delta);
static inline int32_t opal_atomic_min_fetch_32 (volatile int32_t *addr, int32_t value);
static inline int32_t opal_atomic_fetch_min_32 (volatile int32_t *addr, int32_t value);
static inline int32_t opal_atomic_max_fetch_32 (volatile int32_t *addr, int32_t value);
static inline int32_t opal_atomic_fetch_max_32 (volatile int32_t *addr, int32_t value);

#endif /* OPAL_HAVE_ATOMIC_MATH_32 */

#if ! OPAL_HAVE_ATOMIC_MATH_32
/* fix up the value of opal_have_atomic_math_32 to allow for C versions */
#undef OPAL_HAVE_ATOMIC_MATH_32
#define OPAL_HAVE_ATOMIC_MATH_32 OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32
#endif

#ifndef OPAL_HAVE_ATOMIC_MATH_64
/* define to 0 for these tests.  WIll fix up later. */
#define OPAL_HAVE_ATOMIC_MATH_64 0
#endif

#if defined(DOXYGEN) || OPAL_HAVE_ATOMIC_MATH_64 || OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64

static inline int64_t opal_atomic_add_fetch_64(volatile int64_t *addr, int64_t delta);
static inline int64_t opal_atomic_fetch_add_64(volatile int64_t *addr, int64_t delta);
static inline int64_t opal_atomic_and_fetch_64(volatile int64_t *addr, int64_t value);
static inline int64_t opal_atomic_fetch_and_64(volatile int64_t *addr, int64_t value);
static inline int64_t opal_atomic_or_fetch_64(volatile int64_t *addr, int64_t value);
static inline int64_t opal_atomic_fetch_or_64(volatile int64_t *addr, int64_t value);
static inline int64_t opal_atomic_fetch_xor_64(volatile int64_t *addr, int64_t value);
static inline int64_t opal_atomic_sub_fetch_64(volatile int64_t *addr, int64_t delta);
static inline int64_t opal_atomic_fetch_sub_64(volatile int64_t *addr, int64_t delta);
static inline int64_t opal_atomic_min_fetch_64 (volatile int64_t *addr, int64_t value);
static inline int64_t opal_atomic_fetch_min_64 (volatile int64_t *addr, int64_t value);
static inline int64_t opal_atomic_max_fetch_64 (volatile int64_t *addr, int64_t value);
static inline int64_t opal_atomic_fetch_max_64 (volatile int64_t *addr, int64_t value);

#endif /* OPAL_HAVE_ATOMIC_MATH_64 */

#if ! OPAL_HAVE_ATOMIC_MATH_64
/* fix up the value of opal_have_atomic_math_64 to allow for C versions */
#undef OPAL_HAVE_ATOMIC_MATH_64
#define OPAL_HAVE_ATOMIC_MATH_64 OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64
#endif

/* provide a size_t add/subtract.  When in debug mode, make it an
 * inline function so that we don't have any casts in the
 *  interface and can catch type errors.  When not in debug mode,
 * just make it a macro, so that there's no performance penalty
 */
#if defined(DOXYGEN) || OPAL_ENABLE_DEBUG
static inline size_t
opal_atomic_add_fetch_size_t(volatile size_t *addr, size_t delta)
{
#if SIZEOF_SIZE_T == 4
    return (size_t) opal_atomic_add_fetch_32((int32_t*) addr, delta);
#elif SIZEOF_SIZE_T == 8
    return (size_t) opal_atomic_add_fetch_64((int64_t*) addr, delta);
#else
#error "Unknown size_t size"
#endif
}

static inline size_t
opal_atomic_fetch_add_size_t(volatile size_t *addr, size_t delta)
{
#if SIZEOF_SIZE_T == 4
    return (size_t) opal_atomic_fetch_add_32((int32_t*) addr, delta);
#elif SIZEOF_SIZE_T == 8
    return (size_t) opal_atomic_fetch_add_64((int64_t*) addr, delta);
#else
#error "Unknown size_t size"
#endif
}

static inline size_t
opal_atomic_sub_fetch_size_t(volatile size_t *addr, size_t delta)
{
#if SIZEOF_SIZE_T == 4
    return (size_t) opal_atomic_sub_fetch_32((int32_t*) addr, delta);
#elif SIZEOF_SIZE_T == 8
    return (size_t) opal_atomic_sub_fetch_64((int64_t*) addr, delta);
#else
#error "Unknown size_t size"
#endif
}

static inline size_t
opal_atomic_fetch_sub_size_t(volatile size_t *addr, size_t delta)
{
#if SIZEOF_SIZE_T == 4
    return (size_t) opal_atomic_fetch_sub_32((int32_t*) addr, delta);
#elif SIZEOF_SIZE_T == 8
    return (size_t) opal_atomic_fetch_sub_64((int64_t*) addr, delta);
#else
#error "Unknown size_t size"
#endif
}

#else
#if SIZEOF_SIZE_T == 4
#define opal_atomic_add_fetch_size_t(addr, delta) ((size_t) opal_atomic_add_fetch_32((volatile int32_t *) addr, delta))
#define opal_atomic_fetch_add_size_t(addr, delta) ((size_t) opal_atomic_fetch_add_32((volatile int32_t *) addr, delta))
#define opal_atomic_sub_fetch_size_t(addr, delta) ((size_t) opal_atomic_sub_fetch_32((volatile int32_t *) addr, delta))
#define opal_atomic_fetch_sub_size_t(addr, delta) ((size_t) opal_atomic_fetch_sub_32((volatile int32_t *) addr, delta))
#elif SIZEOF_SIZE_T == 8
#define opal_atomic_add_fetch_size_t(addr, delta) ((size_t) opal_atomic_add_fetch_64((volatile int64_t *) addr, delta))
#define opal_atomic_fetch_add_size_t(addr, delta) ((size_t) opal_atomic_fetch_add_64((volatile int64_t *) addr, delta))
#define opal_atomic_sub_fetch_size_t(addr, delta) ((size_t) opal_atomic_sub_fetch_64((volatile int64_t *) addr, delta))
#define opal_atomic_fetch_sub_size_t(addr, delta) ((size_t) opal_atomic_fetch_sub_64((volatile int64_t *) addr, delta))
#else
#error "Unknown size_t size"
#endif
#endif

#if defined(DOXYGEN) || (OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64)
/* these are always done with inline functions, so always mark as
   static inline */

static inline bool opal_atomic_compare_exchange_strong_xx (volatile void *addr, void *oldval,
                                                           int64_t newval, size_t length);
static inline bool opal_atomic_compare_exchange_strong_acq_xx (volatile void *addr, void *oldval,
                                                               int64_t newval, size_t length);
static inline bool opal_atomic_compare_exchange_strong_rel_xx (volatile void *addr, void *oldval,
                                                               int64_t newval, size_t length);


static inline bool opal_atomic_compare_exchange_strong_ptr (volatile void* addr, void *oldval,
                                                            void *newval);
static inline bool opal_atomic_compare_exchange_strong_acq_ptr (volatile void* addr, void *oldval,
                                                                void *newval);
static inline bool opal_atomic_compare_exchange_strong_rel_ptr (volatile void* addr, void *oldval,
                                                                void *newval);

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
 * See opal_atomic_compare_exchange_* for pseudo-code.
 */
#define opal_atomic_compare_exchange_strong( ADDR, OLDVAL, NEWVAL )                  \
    opal_atomic_compare_exchange_strong_xx( (volatile void*)(ADDR), (void *)(OLDVAL), \
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
 * See opal_atomic_compare_exchange_acq_* for pseudo-code.
 */
#define opal_atomic_compare_exchange_strong_acq( ADDR, OLDVAL, NEWVAL )                  \
    opal_atomic_compare_exchange_strong_acq_xx( (volatile void*)(ADDR), (void *)(OLDVAL), \
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
 * See opal_atomic_compare_exchange_rel_* for pseudo-code.
 */
#define opal_atomic_compare_exchange_strong_rel( ADDR, OLDVAL, NEWVAL ) \
    opal_atomic_compare_exchange_strong_rel_xx( (volatile void*)(ADDR), (void *)(OLDVAL), \
                                                (intptr_t)(NEWVAL), sizeof(*(ADDR)) )


#endif /* (OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_32 || OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_64) */

#if defined(DOXYGEN) || (OPAL_HAVE_ATOMIC_MATH_32 || OPAL_HAVE_ATOMIC_MATH_64)

static inline void opal_atomic_add_xx(volatile void* addr,
                                      int32_t value, size_t length);
static inline void opal_atomic_sub_xx(volatile void* addr,
                                      int32_t value, size_t length);

static inline intptr_t opal_atomic_add_fetch_ptr( volatile void* addr, void* delta );
static inline intptr_t opal_atomic_fetch_add_ptr( volatile void* addr, void* delta );
static inline intptr_t opal_atomic_sub_fetch_ptr( volatile void* addr, void* delta );
static inline intptr_t opal_atomic_fetch_sub_ptr( volatile void* addr, void* delta );

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
#define opal_atomic_add( ADDR, VALUE )                                  \
   opal_atomic_add_xx( (volatile void*)(ADDR), (int32_t)(VALUE), \
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
#define opal_atomic_sub( ADDR, VALUE )                                  \
   opal_atomic_sub_xx( (volatile void*)(ADDR), (int32_t)(VALUE),        \
                      sizeof(*(ADDR)) )

#endif /* OPAL_HAVE_ATOMIC_MATH_32 || OPAL_HAVE_ATOMIC_MATH_64 */


/*
 * Include inline implementations of everything not defined directly
 * in assembly
 */
#include "opal/sys/atomic_impl.h"

END_C_DECLS

#endif /* OPAL_SYS_ATOMIC_H */
