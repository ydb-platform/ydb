/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2010-2020 Intel Corporation
 * Copyright (c) 2007-2009 Kip Macy kmacy@freebsd.org
 * All rights reserved.
 * Derived from FreeBSD's bufring.h
 * Used as BSD-3 Licensed with permission from Kip Macy.
 */

#ifndef _RTE_RING_RTS_C11_MEM_H_
#define _RTE_RING_RTS_C11_MEM_H_

/**
 * @file rte_ring_rts_c11_mem.h
 * It is not recommended to include this file directly,
 * include <rte_ring.h> instead.
 * Contains internal helper functions for Relaxed Tail Sync (RTS) ring mode.
 * For more information please refer to <rte_ring_rts.h>.
 */

/**
 * @internal This function updates tail values.
 */
static __rte_always_inline void
__rte_ring_rts_update_tail(struct rte_ring_rts_headtail *ht)
{
	union __rte_ring_rts_poscnt h, ot, nt;

	/*
	 * If there are other enqueues/dequeues in progress that
	 * might preceded us, then don't update tail with new value.
	 */

	ot.raw = __atomic_load_n(&ht->tail.raw, __ATOMIC_ACQUIRE);

	do {
		/* on 32-bit systems we have to do atomic read here */
		h.raw = __atomic_load_n(&ht->head.raw, __ATOMIC_RELAXED);

		nt.raw = ot.raw;
		if (++nt.val.cnt == h.val.cnt)
			nt.val.pos = h.val.pos;

	} while (__atomic_compare_exchange_n(&ht->tail.raw, &ot.raw, nt.raw,
			0, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE) == 0);
}

/**
 * @internal This function waits till head/tail distance wouldn't
 * exceed pre-defined max value.
 */
static __rte_always_inline void
__rte_ring_rts_head_wait(const struct rte_ring_rts_headtail *ht,
	union __rte_ring_rts_poscnt *h)
{
	uint32_t max;

	max = ht->htd_max;

	while (h->val.pos - ht->tail.val.pos > max) {
		rte_pause();
		h->raw = __atomic_load_n(&ht->head.raw, __ATOMIC_ACQUIRE);
	}
}

/**
 * @internal This function updates the producer head for enqueue.
 */
static __rte_always_inline uint32_t
__rte_ring_rts_move_prod_head(struct rte_ring *r, uint32_t num,
	enum rte_ring_queue_behavior behavior, uint32_t *old_head,
	uint32_t *free_entries)
{
	uint32_t n;
	union __rte_ring_rts_poscnt nh, oh;

	const uint32_t capacity = r->capacity;

	oh.raw = __atomic_load_n(&r->rts_prod.head.raw, __ATOMIC_ACQUIRE);

	do {
		/* Reset n to the initial burst count */
		n = num;

		/*
		 * wait for prod head/tail distance,
		 * make sure that we read prod head *before*
		 * reading cons tail.
		 */
		__rte_ring_rts_head_wait(&r->rts_prod, &oh);

		/*
		 *  The subtraction is done between two unsigned 32bits value
		 * (the result is always modulo 32 bits even if we have
		 * *old_head > cons_tail). So 'free_entries' is always between 0
		 * and capacity (which is < size).
		 */
		*free_entries = capacity + r->cons.tail - oh.val.pos;

		/* check that we have enough room in ring */
		if (unlikely(n > *free_entries))
			n = (behavior == RTE_RING_QUEUE_FIXED) ?
					0 : *free_entries;

		if (n == 0)
			break;

		nh.val.pos = oh.val.pos + n;
		nh.val.cnt = oh.val.cnt + 1;

	/*
	 * this CAS(ACQUIRE, ACQUIRE) serves as a hoist barrier to prevent:
	 *  - OOO reads of cons tail value
	 *  - OOO copy of elems to the ring
	 */
	} while (__atomic_compare_exchange_n(&r->rts_prod.head.raw,
			&oh.raw, nh.raw,
			0, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE) == 0);

	*old_head = oh.val.pos;
	return n;
}

/**
 * @internal This function updates the consumer head for dequeue
 */
static __rte_always_inline unsigned int
__rte_ring_rts_move_cons_head(struct rte_ring *r, uint32_t num,
	enum rte_ring_queue_behavior behavior, uint32_t *old_head,
	uint32_t *entries)
{
	uint32_t n;
	union __rte_ring_rts_poscnt nh, oh;

	oh.raw = __atomic_load_n(&r->rts_cons.head.raw, __ATOMIC_ACQUIRE);

	/* move cons.head atomically */
	do {
		/* Restore n as it may change every loop */
		n = num;

		/*
		 * wait for cons head/tail distance,
		 * make sure that we read cons head *before*
		 * reading prod tail.
		 */
		__rte_ring_rts_head_wait(&r->rts_cons, &oh);

		/* The subtraction is done between two unsigned 32bits value
		 * (the result is always modulo 32 bits even if we have
		 * cons_head > prod_tail). So 'entries' is always between 0
		 * and size(ring)-1.
		 */
		*entries = r->prod.tail - oh.val.pos;

		/* Set the actual entries for dequeue */
		if (n > *entries)
			n = (behavior == RTE_RING_QUEUE_FIXED) ? 0 : *entries;

		if (unlikely(n == 0))
			break;

		nh.val.pos = oh.val.pos + n;
		nh.val.cnt = oh.val.cnt + 1;

	/*
	 * this CAS(ACQUIRE, ACQUIRE) serves as a hoist barrier to prevent:
	 *  - OOO reads of prod tail value
	 *  - OOO copy of elems from the ring
	 */
	} while (__atomic_compare_exchange_n(&r->rts_cons.head.raw,
			&oh.raw, nh.raw,
			0, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE) == 0);

	*old_head = oh.val.pos;
	return n;
}

#endif /* _RTE_RING_RTS_C11_MEM_H_ */
