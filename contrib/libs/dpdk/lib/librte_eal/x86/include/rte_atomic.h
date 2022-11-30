/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_ATOMIC_X86_H_
#define _RTE_ATOMIC_X86_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <rte_common.h>
#include <rte_config.h>
#include <emmintrin.h>
#include "generic/rte_atomic.h"

#if RTE_MAX_LCORE == 1
#define MPLOCKED                        /**< No need to insert MP lock prefix. */
#else
#define MPLOCKED        "lock ; "       /**< Insert MP lock prefix. */
#endif

#define	rte_mb() _mm_mfence()

#define	rte_wmb() _mm_sfence()

#define	rte_rmb() _mm_lfence()

#define rte_smp_wmb() rte_compiler_barrier()

#define rte_smp_rmb() rte_compiler_barrier()

/*
 * From Intel Software Development Manual; Vol 3;
 * 8.2.2 Memory Ordering in P6 and More Recent Processor Families:
 * ...
 * . Reads are not reordered with other reads.
 * . Writes are not reordered with older reads.
 * . Writes to memory are not reordered with other writes,
 *   with the following exceptions:
 *   . streaming stores (writes) executed with the non-temporal move
 *     instructions (MOVNTI, MOVNTQ, MOVNTDQ, MOVNTPS, and MOVNTPD); and
 *   . string operations (see Section 8.2.4.1).
 *  ...
 * . Reads may be reordered with older writes to different locations but not
 * with older writes to the same location.
 * . Reads or writes cannot be reordered with I/O instructions,
 * locked instructions, or serializing instructions.
 * . Reads cannot pass earlier LFENCE and MFENCE instructions.
 * . Writes ... cannot pass earlier LFENCE, SFENCE, and MFENCE instructions.
 * . LFENCE instructions cannot pass earlier reads.
 * . SFENCE instructions cannot pass earlier writes ...
 * . MFENCE instructions cannot pass earlier reads, writes ...
 *
 * As pointed by Java guys, that makes possible to use lock-prefixed
 * instructions to get the same effect as mfence and on most modern HW
 * that gives a better performance then using mfence:
 * https://shipilev.net/blog/2014/on-the-fence-with-dependencies/
 * Basic idea is to use lock prefixed add with some dummy memory location
 * as the destination. From their experiments 128B(2 cache lines) below
 * current stack pointer looks like a good candidate.
 * So below we use that techinque for rte_smp_mb() implementation.
 */

static __rte_always_inline void
rte_smp_mb(void)
{
#ifdef RTE_ARCH_I686
	asm volatile("lock addl $0, -128(%%esp); " ::: "memory");
#else
	asm volatile("lock addl $0, -128(%%rsp); " ::: "memory");
#endif
}

#define rte_io_mb() rte_mb()

#define rte_io_wmb() rte_compiler_barrier()

#define rte_io_rmb() rte_compiler_barrier()

/**
 * Synchronization fence between threads based on the specified memory order.
 *
 * On x86 the __atomic_thread_fence(__ATOMIC_SEQ_CST) generates full 'mfence'
 * which is quite expensive. The optimized implementation of rte_smp_mb is
 * used instead.
 */
static __rte_always_inline void
rte_atomic_thread_fence(int memorder)
{
	if (memorder == __ATOMIC_SEQ_CST)
		rte_smp_mb();
	else
		__atomic_thread_fence(memorder);
}

/*------------------------- 16 bit atomic operations -------------------------*/

#ifndef RTE_FORCE_INTRINSICS
static inline int
rte_atomic16_cmpset(volatile uint16_t *dst, uint16_t exp, uint16_t src)
{
	uint8_t res;

	asm volatile(
			MPLOCKED
			"cmpxchgw %[src], %[dst];"
			"sete %[res];"
			: [res] "=a" (res),     /* output */
			  [dst] "=m" (*dst)
			: [src] "r" (src),      /* input */
			  "a" (exp),
			  "m" (*dst)
			: "memory");            /* no-clobber list */
	return res;
}

static inline uint16_t
rte_atomic16_exchange(volatile uint16_t *dst, uint16_t val)
{
	asm volatile(
			MPLOCKED
			"xchgw %0, %1;"
			: "=r" (val), "=m" (*dst)
			: "0" (val),  "m" (*dst)
			: "memory");         /* no-clobber list */
	return val;
}

static inline int rte_atomic16_test_and_set(rte_atomic16_t *v)
{
	return rte_atomic16_cmpset((volatile uint16_t *)&v->cnt, 0, 1);
}

static inline void
rte_atomic16_inc(rte_atomic16_t *v)
{
	asm volatile(
			MPLOCKED
			"incw %[cnt]"
			: [cnt] "=m" (v->cnt)   /* output */
			: "m" (v->cnt)          /* input */
			);
}

static inline void
rte_atomic16_dec(rte_atomic16_t *v)
{
	asm volatile(
			MPLOCKED
			"decw %[cnt]"
			: [cnt] "=m" (v->cnt)   /* output */
			: "m" (v->cnt)          /* input */
			);
}

static inline int rte_atomic16_inc_and_test(rte_atomic16_t *v)
{
	uint8_t ret;

	asm volatile(
			MPLOCKED
			"incw %[cnt] ; "
			"sete %[ret]"
			: [cnt] "+m" (v->cnt),  /* output */
			  [ret] "=qm" (ret)
			);
	return ret != 0;
}

static inline int rte_atomic16_dec_and_test(rte_atomic16_t *v)
{
	uint8_t ret;

	asm volatile(MPLOCKED
			"decw %[cnt] ; "
			"sete %[ret]"
			: [cnt] "+m" (v->cnt),  /* output */
			  [ret] "=qm" (ret)
			);
	return ret != 0;
}

/*------------------------- 32 bit atomic operations -------------------------*/

static inline int
rte_atomic32_cmpset(volatile uint32_t *dst, uint32_t exp, uint32_t src)
{
	uint8_t res;

	asm volatile(
			MPLOCKED
			"cmpxchgl %[src], %[dst];"
			"sete %[res];"
			: [res] "=a" (res),     /* output */
			  [dst] "=m" (*dst)
			: [src] "r" (src),      /* input */
			  "a" (exp),
			  "m" (*dst)
			: "memory");            /* no-clobber list */
	return res;
}

static inline uint32_t
rte_atomic32_exchange(volatile uint32_t *dst, uint32_t val)
{
	asm volatile(
			MPLOCKED
			"xchgl %0, %1;"
			: "=r" (val), "=m" (*dst)
			: "0" (val),  "m" (*dst)
			: "memory");         /* no-clobber list */
	return val;
}

static inline int rte_atomic32_test_and_set(rte_atomic32_t *v)
{
	return rte_atomic32_cmpset((volatile uint32_t *)&v->cnt, 0, 1);
}

static inline void
rte_atomic32_inc(rte_atomic32_t *v)
{
	asm volatile(
			MPLOCKED
			"incl %[cnt]"
			: [cnt] "=m" (v->cnt)   /* output */
			: "m" (v->cnt)          /* input */
			);
}

static inline void
rte_atomic32_dec(rte_atomic32_t *v)
{
	asm volatile(
			MPLOCKED
			"decl %[cnt]"
			: [cnt] "=m" (v->cnt)   /* output */
			: "m" (v->cnt)          /* input */
			);
}

static inline int rte_atomic32_inc_and_test(rte_atomic32_t *v)
{
	uint8_t ret;

	asm volatile(
			MPLOCKED
			"incl %[cnt] ; "
			"sete %[ret]"
			: [cnt] "+m" (v->cnt),  /* output */
			  [ret] "=qm" (ret)
			);
	return ret != 0;
}

static inline int rte_atomic32_dec_and_test(rte_atomic32_t *v)
{
	uint8_t ret;

	asm volatile(MPLOCKED
			"decl %[cnt] ; "
			"sete %[ret]"
			: [cnt] "+m" (v->cnt),  /* output */
			  [ret] "=qm" (ret)
			);
	return ret != 0;
}
#endif

#ifdef RTE_ARCH_I686
#error #include "rte_atomic_32.h"
#else
#include "rte_atomic_64.h"
#endif

#ifdef __cplusplus
}
#endif

#endif /* _RTE_ATOMIC_X86_H_ */
