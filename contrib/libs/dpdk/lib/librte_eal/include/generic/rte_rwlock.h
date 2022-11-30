/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_RWLOCK_H_
#define _RTE_RWLOCK_H_

/**
 * @file
 *
 * RTE Read-Write Locks
 *
 * This file defines an API for read-write locks. The lock is used to
 * protect data that allows multiple readers in parallel, but only
 * one writer. All readers are blocked until the writer is finished
 * writing.
 *
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_common.h>
#include <rte_atomic.h>
#include <rte_pause.h>

/**
 * The rte_rwlock_t type.
 *
 * cnt is -1 when write lock is held, and > 0 when read locks are held.
 */
typedef struct {
	volatile int32_t cnt; /**< -1 when W lock held, > 0 when R locks held. */
} rte_rwlock_t;

/**
 * A static rwlock initializer.
 */
#define RTE_RWLOCK_INITIALIZER { 0 }

/**
 * Initialize the rwlock to an unlocked state.
 *
 * @param rwl
 *   A pointer to the rwlock structure.
 */
static inline void
rte_rwlock_init(rte_rwlock_t *rwl)
{
	rwl->cnt = 0;
}

/**
 * Take a read lock. Loop until the lock is held.
 *
 * @param rwl
 *   A pointer to a rwlock structure.
 */
static inline void
rte_rwlock_read_lock(rte_rwlock_t *rwl)
{
	int32_t x;
	int success = 0;

	while (success == 0) {
		x = __atomic_load_n(&rwl->cnt, __ATOMIC_RELAXED);
		/* write lock is held */
		if (x < 0) {
			rte_pause();
			continue;
		}
		success = __atomic_compare_exchange_n(&rwl->cnt, &x, x + 1, 1,
					__ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
	}
}

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice.
 *
 * try to take a read lock.
 *
 * @param rwl
 *   A pointer to a rwlock structure.
 * @return
 *   - zero if the lock is successfully taken
 *   - -EBUSY if lock could not be acquired for reading because a
 *     writer holds the lock
 */
__rte_experimental
static inline int
rte_rwlock_read_trylock(rte_rwlock_t *rwl)
{
	int32_t x;
	int success = 0;

	while (success == 0) {
		x = __atomic_load_n(&rwl->cnt, __ATOMIC_RELAXED);
		/* write lock is held */
		if (x < 0)
			return -EBUSY;
		success = __atomic_compare_exchange_n(&rwl->cnt, &x, x + 1, 1,
					__ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
	}

	return 0;
}

/**
 * Release a read lock.
 *
 * @param rwl
 *   A pointer to the rwlock structure.
 */
static inline void
rte_rwlock_read_unlock(rte_rwlock_t *rwl)
{
	__atomic_fetch_sub(&rwl->cnt, 1, __ATOMIC_RELEASE);
}

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice.
 *
 * try to take a write lock.
 *
 * @param rwl
 *   A pointer to a rwlock structure.
 * @return
 *   - zero if the lock is successfully taken
 *   - -EBUSY if lock could not be acquired for writing because
 *     it was already locked for reading or writing
 */
__rte_experimental
static inline int
rte_rwlock_write_trylock(rte_rwlock_t *rwl)
{
	int32_t x;

	x = __atomic_load_n(&rwl->cnt, __ATOMIC_RELAXED);
	if (x != 0 || __atomic_compare_exchange_n(&rwl->cnt, &x, -1, 1,
			      __ATOMIC_ACQUIRE, __ATOMIC_RELAXED) == 0)
		return -EBUSY;

	return 0;
}

/**
 * Take a write lock. Loop until the lock is held.
 *
 * @param rwl
 *   A pointer to a rwlock structure.
 */
static inline void
rte_rwlock_write_lock(rte_rwlock_t *rwl)
{
	int32_t x;
	int success = 0;

	while (success == 0) {
		x = __atomic_load_n(&rwl->cnt, __ATOMIC_RELAXED);
		/* a lock is held */
		if (x != 0) {
			rte_pause();
			continue;
		}
		success = __atomic_compare_exchange_n(&rwl->cnt, &x, -1, 1,
					__ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
	}
}

/**
 * Release a write lock.
 *
 * @param rwl
 *   A pointer to a rwlock structure.
 */
static inline void
rte_rwlock_write_unlock(rte_rwlock_t *rwl)
{
	__atomic_store_n(&rwl->cnt, 0, __ATOMIC_RELEASE);
}

/**
 * Try to execute critical section in a hardware memory transaction, if it
 * fails or not available take a read lock
 *
 * NOTE: An attempt to perform a HW I/O operation inside a hardware memory
 * transaction always aborts the transaction since the CPU is not able to
 * roll-back should the transaction fail. Therefore, hardware transactional
 * locks are not advised to be used around rte_eth_rx_burst() and
 * rte_eth_tx_burst() calls.
 *
 * @param rwl
 *   A pointer to a rwlock structure.
 */
static inline void
rte_rwlock_read_lock_tm(rte_rwlock_t *rwl);

/**
 * Commit hardware memory transaction or release the read lock if the lock is used as a fall-back
 *
 * @param rwl
 *   A pointer to the rwlock structure.
 */
static inline void
rte_rwlock_read_unlock_tm(rte_rwlock_t *rwl);

/**
 * Try to execute critical section in a hardware memory transaction, if it
 * fails or not available take a write lock
 *
 * NOTE: An attempt to perform a HW I/O operation inside a hardware memory
 * transaction always aborts the transaction since the CPU is not able to
 * roll-back should the transaction fail. Therefore, hardware transactional
 * locks are not advised to be used around rte_eth_rx_burst() and
 * rte_eth_tx_burst() calls.
 *
 * @param rwl
 *   A pointer to a rwlock structure.
 */
static inline void
rte_rwlock_write_lock_tm(rte_rwlock_t *rwl);

/**
 * Commit hardware memory transaction or release the write lock if the lock is used as a fall-back
 *
 * @param rwl
 *   A pointer to a rwlock structure.
 */
static inline void
rte_rwlock_write_unlock_tm(rte_rwlock_t *rwl);

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RWLOCK_H_ */
