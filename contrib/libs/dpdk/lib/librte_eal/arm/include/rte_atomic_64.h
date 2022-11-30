/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 Cavium, Inc
 * Copyright(c) 2020 Arm Limited
 */

#ifndef _RTE_ATOMIC_ARM64_H_
#define _RTE_ATOMIC_ARM64_H_

#ifndef RTE_FORCE_INTRINSICS
#  error Platform must be built with RTE_FORCE_INTRINSICS
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include "generic/rte_atomic.h"
#include <rte_branch_prediction.h>
#include <rte_compat.h>
#include <rte_debug.h>

#define rte_mb() asm volatile("dmb osh" : : : "memory")

#define rte_wmb() asm volatile("dmb oshst" : : : "memory")

#define rte_rmb() asm volatile("dmb oshld" : : : "memory")

#define rte_smp_mb() asm volatile("dmb ish" : : : "memory")

#define rte_smp_wmb() asm volatile("dmb ishst" : : : "memory")

#define rte_smp_rmb() asm volatile("dmb ishld" : : : "memory")

#define rte_io_mb() rte_mb()

#define rte_io_wmb() rte_wmb()

#define rte_io_rmb() rte_rmb()

static __rte_always_inline void
rte_atomic_thread_fence(int memorder)
{
	__atomic_thread_fence(memorder);
}

/*------------------------ 128 bit atomic operations -------------------------*/

#if defined(__ARM_FEATURE_ATOMICS) || defined(RTE_ARM_FEATURE_ATOMICS)
#if defined(RTE_CC_CLANG)
#define __LSE_PREAMBLE	".arch armv8-a+lse\n"
#else
#define __LSE_PREAMBLE	""
#endif

#define __ATOMIC128_CAS_OP(cas_op_name, op_string)                          \
static __rte_noinline rte_int128_t                                          \
cas_op_name(rte_int128_t *dst, rte_int128_t old, rte_int128_t updated)      \
{                                                                           \
	/* caspX instructions register pair must start from even-numbered
	 * register at operand 1.
	 * So, specify registers for local variables here.
	 */                                                                 \
	register uint64_t x0 __asm("x0") = (uint64_t)old.val[0];            \
	register uint64_t x1 __asm("x1") = (uint64_t)old.val[1];            \
	register uint64_t x2 __asm("x2") = (uint64_t)updated.val[0];        \
	register uint64_t x3 __asm("x3") = (uint64_t)updated.val[1];        \
	asm volatile(                                                       \
		__LSE_PREAMBLE						    \
		op_string " %[old0], %[old1], %[upd0], %[upd1], [%[dst]]"   \
		: [old0] "+r" (x0),                                         \
		[old1] "+r" (x1)                                            \
		: [upd0] "r" (x2),                                          \
		[upd1] "r" (x3),                                            \
		[dst] "r" (dst)                                             \
		: "memory");                                                \
	old.val[0] = x0;                                                    \
	old.val[1] = x1;                                                    \
	return old;                                                         \
}

__ATOMIC128_CAS_OP(__cas_128_relaxed, "casp")
__ATOMIC128_CAS_OP(__cas_128_acquire, "caspa")
__ATOMIC128_CAS_OP(__cas_128_release, "caspl")
__ATOMIC128_CAS_OP(__cas_128_acq_rel, "caspal")

#undef __LSE_PREAMBLE
#undef __ATOMIC128_CAS_OP

#endif

__rte_experimental
static inline int
rte_atomic128_cmp_exchange(rte_int128_t *dst, rte_int128_t *exp,
		const rte_int128_t *src, unsigned int weak, int success,
		int failure)
{
	/* Always do strong CAS */
	RTE_SET_USED(weak);
	/* Ignore memory ordering for failure, memory order for
	 * success must be stronger or equal
	 */
	RTE_SET_USED(failure);
	/* Find invalid memory order */
	RTE_ASSERT(success == __ATOMIC_RELAXED ||
		success == __ATOMIC_ACQUIRE ||
		success == __ATOMIC_RELEASE ||
		success == __ATOMIC_ACQ_REL ||
		success == __ATOMIC_SEQ_CST);

	rte_int128_t expected = *exp;
	rte_int128_t desired = *src;
	rte_int128_t old;

#if defined(__ARM_FEATURE_ATOMICS) || defined(RTE_ARM_FEATURE_ATOMICS)
	if (success == __ATOMIC_RELAXED)
		old = __cas_128_relaxed(dst, expected, desired);
	else if (success == __ATOMIC_ACQUIRE)
		old = __cas_128_acquire(dst, expected, desired);
	else if (success == __ATOMIC_RELEASE)
		old = __cas_128_release(dst, expected, desired);
	else
		old = __cas_128_acq_rel(dst, expected, desired);
#else
#define __HAS_ACQ(mo) ((mo) != __ATOMIC_RELAXED && (mo) != __ATOMIC_RELEASE)
#define __HAS_RLS(mo) ((mo) == __ATOMIC_RELEASE || (mo) == __ATOMIC_ACQ_REL || \
		(mo) == __ATOMIC_SEQ_CST)

	int ldx_mo = __HAS_ACQ(success) ? __ATOMIC_ACQUIRE : __ATOMIC_RELAXED;
	int stx_mo = __HAS_RLS(success) ? __ATOMIC_RELEASE : __ATOMIC_RELAXED;

#undef __HAS_ACQ
#undef __HAS_RLS

	uint32_t ret = 1;

	/* ldx128 can not guarantee atomic,
	 * Must write back src or old to verify atomicity of ldx128;
	 */
	do {

#define __LOAD_128(op_string, src, dst) { \
	asm volatile(                     \
		op_string " %0, %1, %2"   \
		: "=&r" (dst.val[0]),     \
		  "=&r" (dst.val[1])      \
		: "Q" (src->val[0])       \
		: "memory"); }

		if (ldx_mo == __ATOMIC_RELAXED)
			__LOAD_128("ldxp", dst, old)
		else
			__LOAD_128("ldaxp", dst, old)

#undef __LOAD_128

#define __STORE_128(op_string, dst, src, ret) { \
	asm volatile(                           \
		op_string " %w0, %1, %2, %3"    \
		: "=&r" (ret)                   \
		: "r" (src.val[0]),             \
		  "r" (src.val[1]),             \
		  "Q" (dst->val[0])             \
		: "memory"); }

		if (likely(old.int128 == expected.int128)) {
			if (stx_mo == __ATOMIC_RELAXED)
				__STORE_128("stxp", dst, desired, ret)
			else
				__STORE_128("stlxp", dst, desired, ret)
		} else {
			/* In the failure case (since 'weak' is ignored and only
			 * weak == 0 is implemented), expected should contain
			 * the atomically read value of dst. This means, 'old'
			 * needs to be stored back to ensure it was read
			 * atomically.
			 */
			if (stx_mo == __ATOMIC_RELAXED)
				__STORE_128("stxp", dst, old, ret)
			else
				__STORE_128("stlxp", dst, old, ret)
		}

#undef __STORE_128

	} while (unlikely(ret));
#endif

	/* Unconditionally updating expected removes an 'if' statement.
	 * expected should already be in register if not in the cache.
	 */
	*exp = old;

	return (old.int128 == expected.int128);
}

#ifdef __cplusplus
}
#endif

#endif /* _RTE_ATOMIC_ARM64_H_ */
