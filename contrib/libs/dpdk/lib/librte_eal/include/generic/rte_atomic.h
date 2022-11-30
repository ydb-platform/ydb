/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_ATOMIC_H_
#define _RTE_ATOMIC_H_

/**
 * @file
 * Atomic Operations
 *
 * This file defines a generic API for atomic operations.
 */

#include <stdint.h>
#include <rte_common.h>

#ifdef __DOXYGEN__

/** @name Memory Barrier
 */
///@{
/**
 * General memory barrier.
 *
 * Guarantees that the LOAD and STORE operations generated before the
 * barrier occur before the LOAD and STORE operations generated after.
 */
static inline void rte_mb(void);

/**
 * Write memory barrier.
 *
 * Guarantees that the STORE operations generated before the barrier
 * occur before the STORE operations generated after.
 */
static inline void rte_wmb(void);

/**
 * Read memory barrier.
 *
 * Guarantees that the LOAD operations generated before the barrier
 * occur before the LOAD operations generated after.
 */
static inline void rte_rmb(void);
///@}

/** @name SMP Memory Barrier
 */
///@{
/**
 * General memory barrier between lcores
 *
 * Guarantees that the LOAD and STORE operations that precede the
 * rte_smp_mb() call are globally visible across the lcores
 * before the LOAD and STORE operations that follows it.
 */
static inline void rte_smp_mb(void);

/**
 * Write memory barrier between lcores
 *
 * Guarantees that the STORE operations that precede the
 * rte_smp_wmb() call are globally visible across the lcores
 * before the STORE operations that follows it.
 */
static inline void rte_smp_wmb(void);

/**
 * Read memory barrier between lcores
 *
 * Guarantees that the LOAD operations that precede the
 * rte_smp_rmb() call are globally visible across the lcores
 * before the LOAD operations that follows it.
 */
static inline void rte_smp_rmb(void);
///@}

/** @name I/O Memory Barrier
 */
///@{
/**
 * General memory barrier for I/O device
 *
 * Guarantees that the LOAD and STORE operations that precede the
 * rte_io_mb() call are visible to I/O device or CPU before the
 * LOAD and STORE operations that follow it.
 */
static inline void rte_io_mb(void);

/**
 * Write memory barrier for I/O device
 *
 * Guarantees that the STORE operations that precede the
 * rte_io_wmb() call are visible to I/O device before the STORE
 * operations that follow it.
 */
static inline void rte_io_wmb(void);

/**
 * Read memory barrier for IO device
 *
 * Guarantees that the LOAD operations on I/O device that precede the
 * rte_io_rmb() call are visible to CPU before the LOAD
 * operations that follow it.
 */
static inline void rte_io_rmb(void);
///@}

#endif /* __DOXYGEN__ */

/**
 * Compiler barrier.
 *
 * Guarantees that operation reordering does not occur at compile time
 * for operations directly before and after the barrier.
 */
#define	rte_compiler_barrier() do {		\
	asm volatile ("" : : : "memory");	\
} while(0)

/**
 * Synchronization fence between threads based on the specified memory order.
 */
static inline void rte_atomic_thread_fence(int memorder);

/*------------------------- 16 bit atomic operations -------------------------*/

/**
 * Atomic compare and set.
 *
 * (atomic) equivalent to:
 *   if (*dst == exp)
 *     *dst = src (all 16-bit words)
 *
 * @param dst
 *   The destination location into which the value will be written.
 * @param exp
 *   The expected value.
 * @param src
 *   The new value.
 * @return
 *   Non-zero on success; 0 on failure.
 */
static inline int
rte_atomic16_cmpset(volatile uint16_t *dst, uint16_t exp, uint16_t src);

#ifdef RTE_FORCE_INTRINSICS
static inline int
rte_atomic16_cmpset(volatile uint16_t *dst, uint16_t exp, uint16_t src)
{
	return __sync_bool_compare_and_swap(dst, exp, src);
}
#endif

/**
 * Atomic exchange.
 *
 * (atomic) equivalent to:
 *   ret = *dst
 *   *dst = val;
 *   return ret;
 *
 * @param dst
 *   The destination location into which the value will be written.
 * @param val
 *   The new value.
 * @return
 *   The original value at that location
 */
static inline uint16_t
rte_atomic16_exchange(volatile uint16_t *dst, uint16_t val);

#ifdef RTE_FORCE_INTRINSICS
static inline uint16_t
rte_atomic16_exchange(volatile uint16_t *dst, uint16_t val)
{
#if defined(__clang__)
	return __atomic_exchange_n(dst, val, __ATOMIC_SEQ_CST);
#else
	return __atomic_exchange_2(dst, val, __ATOMIC_SEQ_CST);
#endif
}
#endif

/**
 * The atomic counter structure.
 */
typedef struct {
	volatile int16_t cnt; /**< An internal counter value. */
} rte_atomic16_t;

/**
 * Static initializer for an atomic counter.
 */
#define RTE_ATOMIC16_INIT(val) { (val) }

/**
 * Initialize an atomic counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void
rte_atomic16_init(rte_atomic16_t *v)
{
	v->cnt = 0;
}

/**
 * Atomically read a 16-bit value from a counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   The value of the counter.
 */
static inline int16_t
rte_atomic16_read(const rte_atomic16_t *v)
{
	return v->cnt;
}

/**
 * Atomically set a counter to a 16-bit value.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param new_value
 *   The new value for the counter.
 */
static inline void
rte_atomic16_set(rte_atomic16_t *v, int16_t new_value)
{
	v->cnt = new_value;
}

/**
 * Atomically add a 16-bit value to an atomic counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param inc
 *   The value to be added to the counter.
 */
static inline void
rte_atomic16_add(rte_atomic16_t *v, int16_t inc)
{
	__sync_fetch_and_add(&v->cnt, inc);
}

/**
 * Atomically subtract a 16-bit value from an atomic counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param dec
 *   The value to be subtracted from the counter.
 */
static inline void
rte_atomic16_sub(rte_atomic16_t *v, int16_t dec)
{
	__sync_fetch_and_sub(&v->cnt, dec);
}

/**
 * Atomically increment a counter by one.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void
rte_atomic16_inc(rte_atomic16_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic16_inc(rte_atomic16_t *v)
{
	rte_atomic16_add(v, 1);
}
#endif

/**
 * Atomically decrement a counter by one.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void
rte_atomic16_dec(rte_atomic16_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic16_dec(rte_atomic16_t *v)
{
	rte_atomic16_sub(v, 1);
}
#endif

/**
 * Atomically add a 16-bit value to a counter and return the result.
 *
 * Atomically adds the 16-bits value (inc) to the atomic counter (v) and
 * returns the value of v after addition.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param inc
 *   The value to be added to the counter.
 * @return
 *   The value of v after the addition.
 */
static inline int16_t
rte_atomic16_add_return(rte_atomic16_t *v, int16_t inc)
{
	return __sync_add_and_fetch(&v->cnt, inc);
}

/**
 * Atomically subtract a 16-bit value from a counter and return
 * the result.
 *
 * Atomically subtracts the 16-bit value (inc) from the atomic counter
 * (v) and returns the value of v after the subtraction.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param dec
 *   The value to be subtracted from the counter.
 * @return
 *   The value of v after the subtraction.
 */
static inline int16_t
rte_atomic16_sub_return(rte_atomic16_t *v, int16_t dec)
{
	return __sync_sub_and_fetch(&v->cnt, dec);
}

/**
 * Atomically increment a 16-bit counter by one and test.
 *
 * Atomically increments the atomic counter (v) by one and returns true if
 * the result is 0, or false in all other cases.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   True if the result after the increment operation is 0; false otherwise.
 */
static inline int rte_atomic16_inc_and_test(rte_atomic16_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int rte_atomic16_inc_and_test(rte_atomic16_t *v)
{
	return __sync_add_and_fetch(&v->cnt, 1) == 0;
}
#endif

/**
 * Atomically decrement a 16-bit counter by one and test.
 *
 * Atomically decrements the atomic counter (v) by one and returns true if
 * the result is 0, or false in all other cases.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   True if the result after the decrement operation is 0; false otherwise.
 */
static inline int rte_atomic16_dec_and_test(rte_atomic16_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int rte_atomic16_dec_and_test(rte_atomic16_t *v)
{
	return __sync_sub_and_fetch(&v->cnt, 1) == 0;
}
#endif

/**
 * Atomically test and set a 16-bit atomic counter.
 *
 * If the counter value is already set, return 0 (failed). Otherwise, set
 * the counter value to 1 and return 1 (success).
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   0 if failed; else 1, success.
 */
static inline int rte_atomic16_test_and_set(rte_atomic16_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int rte_atomic16_test_and_set(rte_atomic16_t *v)
{
	return rte_atomic16_cmpset((volatile uint16_t *)&v->cnt, 0, 1);
}
#endif

/**
 * Atomically set a 16-bit counter to 0.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void rte_atomic16_clear(rte_atomic16_t *v)
{
	v->cnt = 0;
}

/*------------------------- 32 bit atomic operations -------------------------*/

/**
 * Atomic compare and set.
 *
 * (atomic) equivalent to:
 *   if (*dst == exp)
 *     *dst = src (all 32-bit words)
 *
 * @param dst
 *   The destination location into which the value will be written.
 * @param exp
 *   The expected value.
 * @param src
 *   The new value.
 * @return
 *   Non-zero on success; 0 on failure.
 */
static inline int
rte_atomic32_cmpset(volatile uint32_t *dst, uint32_t exp, uint32_t src);

#ifdef RTE_FORCE_INTRINSICS
static inline int
rte_atomic32_cmpset(volatile uint32_t *dst, uint32_t exp, uint32_t src)
{
	return __sync_bool_compare_and_swap(dst, exp, src);
}
#endif

/**
 * Atomic exchange.
 *
 * (atomic) equivalent to:
 *   ret = *dst
 *   *dst = val;
 *   return ret;
 *
 * @param dst
 *   The destination location into which the value will be written.
 * @param val
 *   The new value.
 * @return
 *   The original value at that location
 */
static inline uint32_t
rte_atomic32_exchange(volatile uint32_t *dst, uint32_t val);

#ifdef RTE_FORCE_INTRINSICS
static inline uint32_t
rte_atomic32_exchange(volatile uint32_t *dst, uint32_t val)
{
#if defined(__clang__)
	return __atomic_exchange_n(dst, val, __ATOMIC_SEQ_CST);
#else
	return __atomic_exchange_4(dst, val, __ATOMIC_SEQ_CST);
#endif
}
#endif

/**
 * The atomic counter structure.
 */
typedef struct {
	volatile int32_t cnt; /**< An internal counter value. */
} rte_atomic32_t;

/**
 * Static initializer for an atomic counter.
 */
#define RTE_ATOMIC32_INIT(val) { (val) }

/**
 * Initialize an atomic counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void
rte_atomic32_init(rte_atomic32_t *v)
{
	v->cnt = 0;
}

/**
 * Atomically read a 32-bit value from a counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   The value of the counter.
 */
static inline int32_t
rte_atomic32_read(const rte_atomic32_t *v)
{
	return v->cnt;
}

/**
 * Atomically set a counter to a 32-bit value.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param new_value
 *   The new value for the counter.
 */
static inline void
rte_atomic32_set(rte_atomic32_t *v, int32_t new_value)
{
	v->cnt = new_value;
}

/**
 * Atomically add a 32-bit value to an atomic counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param inc
 *   The value to be added to the counter.
 */
static inline void
rte_atomic32_add(rte_atomic32_t *v, int32_t inc)
{
	__sync_fetch_and_add(&v->cnt, inc);
}

/**
 * Atomically subtract a 32-bit value from an atomic counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param dec
 *   The value to be subtracted from the counter.
 */
static inline void
rte_atomic32_sub(rte_atomic32_t *v, int32_t dec)
{
	__sync_fetch_and_sub(&v->cnt, dec);
}

/**
 * Atomically increment a counter by one.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void
rte_atomic32_inc(rte_atomic32_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic32_inc(rte_atomic32_t *v)
{
	rte_atomic32_add(v, 1);
}
#endif

/**
 * Atomically decrement a counter by one.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void
rte_atomic32_dec(rte_atomic32_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic32_dec(rte_atomic32_t *v)
{
	rte_atomic32_sub(v,1);
}
#endif

/**
 * Atomically add a 32-bit value to a counter and return the result.
 *
 * Atomically adds the 32-bits value (inc) to the atomic counter (v) and
 * returns the value of v after addition.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param inc
 *   The value to be added to the counter.
 * @return
 *   The value of v after the addition.
 */
static inline int32_t
rte_atomic32_add_return(rte_atomic32_t *v, int32_t inc)
{
	return __sync_add_and_fetch(&v->cnt, inc);
}

/**
 * Atomically subtract a 32-bit value from a counter and return
 * the result.
 *
 * Atomically subtracts the 32-bit value (inc) from the atomic counter
 * (v) and returns the value of v after the subtraction.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param dec
 *   The value to be subtracted from the counter.
 * @return
 *   The value of v after the subtraction.
 */
static inline int32_t
rte_atomic32_sub_return(rte_atomic32_t *v, int32_t dec)
{
	return __sync_sub_and_fetch(&v->cnt, dec);
}

/**
 * Atomically increment a 32-bit counter by one and test.
 *
 * Atomically increments the atomic counter (v) by one and returns true if
 * the result is 0, or false in all other cases.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   True if the result after the increment operation is 0; false otherwise.
 */
static inline int rte_atomic32_inc_and_test(rte_atomic32_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int rte_atomic32_inc_and_test(rte_atomic32_t *v)
{
	return __sync_add_and_fetch(&v->cnt, 1) == 0;
}
#endif

/**
 * Atomically decrement a 32-bit counter by one and test.
 *
 * Atomically decrements the atomic counter (v) by one and returns true if
 * the result is 0, or false in all other cases.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   True if the result after the decrement operation is 0; false otherwise.
 */
static inline int rte_atomic32_dec_and_test(rte_atomic32_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int rte_atomic32_dec_and_test(rte_atomic32_t *v)
{
	return __sync_sub_and_fetch(&v->cnt, 1) == 0;
}
#endif

/**
 * Atomically test and set a 32-bit atomic counter.
 *
 * If the counter value is already set, return 0 (failed). Otherwise, set
 * the counter value to 1 and return 1 (success).
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   0 if failed; else 1, success.
 */
static inline int rte_atomic32_test_and_set(rte_atomic32_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int rte_atomic32_test_and_set(rte_atomic32_t *v)
{
	return rte_atomic32_cmpset((volatile uint32_t *)&v->cnt, 0, 1);
}
#endif

/**
 * Atomically set a 32-bit counter to 0.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void rte_atomic32_clear(rte_atomic32_t *v)
{
	v->cnt = 0;
}

/*------------------------- 64 bit atomic operations -------------------------*/

/**
 * An atomic compare and set function used by the mutex functions.
 * (atomic) equivalent to:
 *   if (*dst == exp)
 *     *dst = src (all 64-bit words)
 *
 * @param dst
 *   The destination into which the value will be written.
 * @param exp
 *   The expected value.
 * @param src
 *   The new value.
 * @return
 *   Non-zero on success; 0 on failure.
 */
static inline int
rte_atomic64_cmpset(volatile uint64_t *dst, uint64_t exp, uint64_t src);

#ifdef RTE_FORCE_INTRINSICS
static inline int
rte_atomic64_cmpset(volatile uint64_t *dst, uint64_t exp, uint64_t src)
{
	return __sync_bool_compare_and_swap(dst, exp, src);
}
#endif

/**
 * Atomic exchange.
 *
 * (atomic) equivalent to:
 *   ret = *dst
 *   *dst = val;
 *   return ret;
 *
 * @param dst
 *   The destination location into which the value will be written.
 * @param val
 *   The new value.
 * @return
 *   The original value at that location
 */
static inline uint64_t
rte_atomic64_exchange(volatile uint64_t *dst, uint64_t val);

#ifdef RTE_FORCE_INTRINSICS
static inline uint64_t
rte_atomic64_exchange(volatile uint64_t *dst, uint64_t val)
{
#if defined(__clang__)
	return __atomic_exchange_n(dst, val, __ATOMIC_SEQ_CST);
#else
	return __atomic_exchange_8(dst, val, __ATOMIC_SEQ_CST);
#endif
}
#endif

/**
 * The atomic counter structure.
 */
typedef struct {
	volatile int64_t cnt;  /**< Internal counter value. */
} rte_atomic64_t;

/**
 * Static initializer for an atomic counter.
 */
#define RTE_ATOMIC64_INIT(val) { (val) }

/**
 * Initialize the atomic counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void
rte_atomic64_init(rte_atomic64_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic64_init(rte_atomic64_t *v)
{
#ifdef __LP64__
	v->cnt = 0;
#else
	int success = 0;
	uint64_t tmp;

	while (success == 0) {
		tmp = v->cnt;
		success = rte_atomic64_cmpset((volatile uint64_t *)&v->cnt,
		                              tmp, 0);
	}
#endif
}
#endif

/**
 * Atomically read a 64-bit counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   The value of the counter.
 */
static inline int64_t
rte_atomic64_read(rte_atomic64_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int64_t
rte_atomic64_read(rte_atomic64_t *v)
{
#ifdef __LP64__
	return v->cnt;
#else
	int success = 0;
	uint64_t tmp;

	while (success == 0) {
		tmp = v->cnt;
		/* replace the value by itself */
		success = rte_atomic64_cmpset((volatile uint64_t *)&v->cnt,
		                              tmp, tmp);
	}
	return tmp;
#endif
}
#endif

/**
 * Atomically set a 64-bit counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param new_value
 *   The new value of the counter.
 */
static inline void
rte_atomic64_set(rte_atomic64_t *v, int64_t new_value);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic64_set(rte_atomic64_t *v, int64_t new_value)
{
#ifdef __LP64__
	v->cnt = new_value;
#else
	int success = 0;
	uint64_t tmp;

	while (success == 0) {
		tmp = v->cnt;
		success = rte_atomic64_cmpset((volatile uint64_t *)&v->cnt,
		                              tmp, new_value);
	}
#endif
}
#endif

/**
 * Atomically add a 64-bit value to a counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param inc
 *   The value to be added to the counter.
 */
static inline void
rte_atomic64_add(rte_atomic64_t *v, int64_t inc);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic64_add(rte_atomic64_t *v, int64_t inc)
{
	__sync_fetch_and_add(&v->cnt, inc);
}
#endif

/**
 * Atomically subtract a 64-bit value from a counter.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param dec
 *   The value to be subtracted from the counter.
 */
static inline void
rte_atomic64_sub(rte_atomic64_t *v, int64_t dec);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic64_sub(rte_atomic64_t *v, int64_t dec)
{
	__sync_fetch_and_sub(&v->cnt, dec);
}
#endif

/**
 * Atomically increment a 64-bit counter by one and test.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void
rte_atomic64_inc(rte_atomic64_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic64_inc(rte_atomic64_t *v)
{
	rte_atomic64_add(v, 1);
}
#endif

/**
 * Atomically decrement a 64-bit counter by one and test.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void
rte_atomic64_dec(rte_atomic64_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline void
rte_atomic64_dec(rte_atomic64_t *v)
{
	rte_atomic64_sub(v, 1);
}
#endif

/**
 * Add a 64-bit value to an atomic counter and return the result.
 *
 * Atomically adds the 64-bit value (inc) to the atomic counter (v) and
 * returns the value of v after the addition.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param inc
 *   The value to be added to the counter.
 * @return
 *   The value of v after the addition.
 */
static inline int64_t
rte_atomic64_add_return(rte_atomic64_t *v, int64_t inc);

#ifdef RTE_FORCE_INTRINSICS
static inline int64_t
rte_atomic64_add_return(rte_atomic64_t *v, int64_t inc)
{
	return __sync_add_and_fetch(&v->cnt, inc);
}
#endif

/**
 * Subtract a 64-bit value from an atomic counter and return the result.
 *
 * Atomically subtracts the 64-bit value (dec) from the atomic counter (v)
 * and returns the value of v after the subtraction.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @param dec
 *   The value to be subtracted from the counter.
 * @return
 *   The value of v after the subtraction.
 */
static inline int64_t
rte_atomic64_sub_return(rte_atomic64_t *v, int64_t dec);

#ifdef RTE_FORCE_INTRINSICS
static inline int64_t
rte_atomic64_sub_return(rte_atomic64_t *v, int64_t dec)
{
	return __sync_sub_and_fetch(&v->cnt, dec);
}
#endif

/**
 * Atomically increment a 64-bit counter by one and test.
 *
 * Atomically increments the atomic counter (v) by one and returns
 * true if the result is 0, or false in all other cases.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   True if the result after the addition is 0; false otherwise.
 */
static inline int rte_atomic64_inc_and_test(rte_atomic64_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int rte_atomic64_inc_and_test(rte_atomic64_t *v)
{
	return rte_atomic64_add_return(v, 1) == 0;
}
#endif

/**
 * Atomically decrement a 64-bit counter by one and test.
 *
 * Atomically decrements the atomic counter (v) by one and returns true if
 * the result is 0, or false in all other cases.
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   True if the result after subtraction is 0; false otherwise.
 */
static inline int rte_atomic64_dec_and_test(rte_atomic64_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int rte_atomic64_dec_and_test(rte_atomic64_t *v)
{
	return rte_atomic64_sub_return(v, 1) == 0;
}
#endif

/**
 * Atomically test and set a 64-bit atomic counter.
 *
 * If the counter value is already set, return 0 (failed). Otherwise, set
 * the counter value to 1 and return 1 (success).
 *
 * @param v
 *   A pointer to the atomic counter.
 * @return
 *   0 if failed; else 1, success.
 */
static inline int rte_atomic64_test_and_set(rte_atomic64_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline int rte_atomic64_test_and_set(rte_atomic64_t *v)
{
	return rte_atomic64_cmpset((volatile uint64_t *)&v->cnt, 0, 1);
}
#endif

/**
 * Atomically set a 64-bit counter to 0.
 *
 * @param v
 *   A pointer to the atomic counter.
 */
static inline void rte_atomic64_clear(rte_atomic64_t *v);

#ifdef RTE_FORCE_INTRINSICS
static inline void rte_atomic64_clear(rte_atomic64_t *v)
{
	rte_atomic64_set(v, 0);
}
#endif

/*------------------------ 128 bit atomic operations -------------------------*/

/**
 * 128-bit integer structure.
 */
RTE_STD_C11
typedef struct {
	RTE_STD_C11
	union {
		uint64_t val[2];
#ifdef RTE_ARCH_64
		__extension__ __int128 int128;
#endif
	};
} __rte_aligned(16) rte_int128_t;

#ifdef __DOXYGEN__

/**
 * An atomic compare and set function used by the mutex functions.
 * (Atomically) Equivalent to:
 * @code
 *   if (*dst == *exp)
 *     *dst = *src
 *   else
 *     *exp = *dst
 * @endcode
 *
 * @note This function is currently available for the x86-64 and aarch64
 * platforms.
 *
 * @note The success and failure arguments must be one of the __ATOMIC_* values
 * defined in the C++11 standard. For details on their behavior, refer to the
 * standard.
 *
 * @param dst
 *   The destination into which the value will be written.
 * @param exp
 *   Pointer to the expected value. If the operation fails, this memory is
 *   updated with the actual value.
 * @param src
 *   Pointer to the new value.
 * @param weak
 *   A value of true allows the comparison to spuriously fail and allows the
 *   'exp' update to occur non-atomically (i.e. a torn read may occur).
 *   Implementations may ignore this argument and only implement the strong
 *   variant.
 * @param success
 *   If successful, the operation's memory behavior conforms to this (or a
 *   stronger) model.
 * @param failure
 *   If unsuccessful, the operation's memory behavior conforms to this (or a
 *   stronger) model. This argument cannot be __ATOMIC_RELEASE,
 *   __ATOMIC_ACQ_REL, or a stronger model than success.
 * @return
 *   Non-zero on success; 0 on failure.
 */
__rte_experimental
static inline int
rte_atomic128_cmp_exchange(rte_int128_t *dst,
			   rte_int128_t *exp,
			   const rte_int128_t *src,
			   unsigned int weak,
			   int success,
			   int failure);

#endif /* __DOXYGEN__ */

#endif /* _RTE_ATOMIC_H_ */
