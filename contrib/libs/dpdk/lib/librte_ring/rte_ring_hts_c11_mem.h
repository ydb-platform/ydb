/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2010-2020 Intel Corporation
 * Copyright (c) 2007-2009 Kip Macy kmacy@freebsd.org
 * All rights reserved.
 * Derived from FreeBSD's bufring.h
 * Used as BSD-3 Licensed with permission from Kip Macy.
 */

#ifndef _RTE_RING_HTS_C11_MEM_H_
#define _RTE_RING_HTS_C11_MEM_H_

/**
 * @file rte_ring_hts_c11_mem.h
 * It is not recommended to include this file directly,
 * include <rte_ring.h> instead.
 * Contains internal helper functions for head/tail sync (HTS) ring mode.
 * For more information please refer to <rte_ring_hts.h>.
 */

/**
 * @internal update tail with new value.
 */
static __rte_always_inline void
__rte_ring_hts_update_tail(struct rte_ring_hts_headtail *ht, uint32_t old_tail,
	uint32_t num, uint32_t enqueue)
{
	uint32_t tail;

	RTE_SET_USED(enqueue);

	tail = old_tail + num;
	__atomic_store_n(&ht->ht.pos.tail, tail, __ATOMIC_RELEASE);
}

/**
 * @internal waits till tail will become equal to head.
 * Means no writer/reader is active for that ring.
 * Suppose to work as serialization point.
 */
static __rte_always_inline void
__rte_ring_hts_head_wait(const struct rte_ring_hts_headtail *ht,
		union __rte_ring_hts_pos *p)
{
	while (p->pos.head != p->pos.tail) {
		rte_pause();
		p->raw = __atomic_load_n(&ht->ht.raw, __ATOMIC_ACQUIRE);
	}
}

/**
 * @internal This function updates the producer head for enqueue
 */
static __rte_always_inline unsigned int
__rte_ring_hts_move_prod_head(struct rte_ring *r, unsigned int num,
	enum rte_ring_queue_behavior behavior, uint32_t *old_head,
	uint32_t *free_entries)
{
	uint32_t n;
	union __rte_ring_hts_pos np, op;

	const uint32_t capacity = r->capacity;

	op.raw = __atomic_load_n(&r->hts_prod.ht.raw, __ATOMIC_ACQUIRE);

	do {
		/* Reset n to the initial burst count */
		n = num;

		/*
		 * wait for tail to be equal to head,
		 * make sure that we read prod head/tail *before*
		 * reading cons tail.
		 */
		__rte_ring_hts_head_wait(&r->hts_prod, &op);

		/*
		 *  The subtraction is done between two unsigned 32bits value
		 * (the result is always modulo 32 bits even if we have
		 * *old_head > cons_tail). So 'free_entries' is always between 0
		 * and capacity (which is < size).
		 */
		*free_entries = capacity + r->cons.tail - op.pos.head;

		/* check that we have enough room in ring */
		if (unlikely(n > *free_entries))
			n = (behavior == RTE_RING_QUEUE_FIXED) ?
					0 : *free_entries;

		if (n == 0)
			break;

		np.pos.tail = op.pos.tail;
		np.pos.head = op.pos.head + n;

	/*
	 * this CAS(ACQUIRE, ACQUIRE) serves as a hoist barrier to prevent:
	 *  - OOO reads of cons tail value
	 *  - OOO copy of elems from the ring
	 */
	} while (__atomic_compare_exchange_n(&r->hts_prod.ht.raw,
			&op.raw, np.raw,
			0, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE) == 0);

	*old_head = op.pos.head;
	return n;
}

/**
 * @internal This function updates the consumer head for dequeue
 */
static __rte_always_inline unsigned int
__rte_ring_hts_move_cons_head(struct rte_ring *r, unsigned int num,
	enum rte_ring_queue_behavior behavior, uint32_t *old_head,
	uint32_t *entries)
{
	uint32_t n;
	union __rte_ring_hts_pos np, op;

	op.raw = __atomic_load_n(&r->hts_cons.ht.raw, __ATOMIC_ACQUIRE);

	/* move cons.head atomically */
	do {
		/* Restore n as it may change every loop */
		n = num;

		/*
		 * wait for tail to be equal to head,
		 * make sure that we read cons head/tail *before*
		 * reading prod tail.
		 */
		__rte_ring_hts_head_wait(&r->hts_cons, &op);

		/* The subtraction is done between two unsigned 32bits value
		 * (the result is always modulo 32 bits even if we have
		 * cons_head > prod_tail). So 'entries' is always between 0
		 * and size(ring)-1.
		 */
		*entries = r->prod.tail - op.pos.head;

		/* Set the actual entries for dequeue */
		if (n > *entries)
			n = (behavior == RTE_RING_QUEUE_FIXED) ? 0 : *entries;

		if (unlikely(n == 0))
			break;

		np.pos.tail = op.pos.tail;
		np.pos.head = op.pos.head + n;

	/*
	 * this CAS(ACQUIRE, ACQUIRE) serves as a hoist barrier to prevent:
	 *  - OOO reads of prod tail value
	 *  - OOO copy of elems from the ring
	 */
	} while (__atomic_compare_exchange_n(&r->hts_cons.ht.raw,
			&op.raw, np.raw,
			0, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE) == 0);

	*old_head = op.pos.head;
	return n;
}

#endif /* _RTE_RING_HTS_C11_MEM_H_ */
