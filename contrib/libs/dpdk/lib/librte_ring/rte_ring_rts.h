/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2010-2020 Intel Corporation
 * Copyright (c) 2007-2009 Kip Macy kmacy@freebsd.org
 * All rights reserved.
 * Derived from FreeBSD's bufring.h
 * Used as BSD-3 Licensed with permission from Kip Macy.
 */

#ifndef _RTE_RING_RTS_H_
#define _RTE_RING_RTS_H_

/**
 * @file rte_ring_rts.h
 * @b EXPERIMENTAL: this API may change without prior notice
 * It is not recommended to include this file directly.
 * Please include <rte_ring.h> instead.
 *
 * Contains functions for Relaxed Tail Sync (RTS) ring mode.
 * The main idea remains the same as for our original MP/MC synchronization
 * mechanism.
 * The main difference is that tail value is increased not
 * by every thread that finished enqueue/dequeue,
 * but only by the current last one doing enqueue/dequeue.
 * That allows threads to skip spinning on tail value,
 * leaving actual tail value change to last thread at a given instance.
 * RTS requires 2 64-bit CAS for each enqueue(/dequeue) operation:
 * one for head update, second for tail update.
 * As a gain it allows thread to avoid spinning/waiting on tail value.
 * In comparison original MP/MC algorithm requires one 32-bit CAS
 * for head update and waiting/spinning on tail value.
 *
 * Brief outline:
 *  - introduce update counter (cnt) for both head and tail.
 *  - increment head.cnt for each head.value update
 *  - write head.value and head.cnt atomically (64-bit CAS)
 *  - move tail.value ahead only when tail.cnt + 1 == head.cnt
 *    (indicating that this is the last thread updating the tail)
 *  - increment tail.cnt when each enqueue/dequeue op finishes
 *    (no matter if tail.value going to change or not)
 *  - write tail.value and tail.cnt atomically (64-bit CAS)
 *
 * To avoid producer/consumer starvation:
 *  - limit max allowed distance between head and tail value (HTD_MAX).
 *    I.E. thread is allowed to proceed with changing head.value,
 *    only when:  head.value - tail.value <= HTD_MAX
 * HTD_MAX is an optional parameter.
 * With HTD_MAX == 0 we'll have fully serialized ring -
 * i.e. only one thread at a time will be able to enqueue/dequeue
 * to/from the ring.
 * With HTD_MAX >= ring.capacity - no limitation.
 * By default HTD_MAX == ring.capacity / 8.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_ring_rts_c11_mem.h>

/**
 * @internal Enqueue several objects on the RTS ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of objects.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param behavior
 *   RTE_RING_QUEUE_FIXED:    Enqueue a fixed number of items from a ring
 *   RTE_RING_QUEUE_VARIABLE: Enqueue as many items as possible from ring
 * @param free_space
 *   returns the amount of space after the enqueue operation has finished
 * @return
 *   Actual number of objects enqueued.
 *   If behavior == RTE_RING_QUEUE_FIXED, this will be 0 or n only.
 */
static __rte_always_inline unsigned int
__rte_ring_do_rts_enqueue_elem(struct rte_ring *r, const void *obj_table,
	uint32_t esize, uint32_t n, enum rte_ring_queue_behavior behavior,
	uint32_t *free_space)
{
	uint32_t free, head;

	n =  __rte_ring_rts_move_prod_head(r, n, behavior, &head, &free);

	if (n != 0) {
		__rte_ring_enqueue_elems(r, head, obj_table, esize, n);
		__rte_ring_rts_update_tail(&r->rts_prod);
	}

	if (free_space != NULL)
		*free_space = free - n;
	return n;
}

/**
 * @internal Dequeue several objects from the RTS ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of objects.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @param n
 *   The number of objects to pull from the ring.
 * @param behavior
 *   RTE_RING_QUEUE_FIXED:    Dequeue a fixed number of items from a ring
 *   RTE_RING_QUEUE_VARIABLE: Dequeue as many items as possible from ring
 * @param available
 *   returns the number of remaining ring entries after the dequeue has finished
 * @return
 *   - Actual number of objects dequeued.
 *     If behavior == RTE_RING_QUEUE_FIXED, this will be 0 or n only.
 */
static __rte_always_inline unsigned int
__rte_ring_do_rts_dequeue_elem(struct rte_ring *r, void *obj_table,
	uint32_t esize, uint32_t n, enum rte_ring_queue_behavior behavior,
	uint32_t *available)
{
	uint32_t entries, head;

	n = __rte_ring_rts_move_cons_head(r, n, behavior, &head, &entries);

	if (n != 0) {
		__rte_ring_dequeue_elems(r, head, obj_table, esize, n);
		__rte_ring_rts_update_tail(&r->rts_cons);
	}

	if (available != NULL)
		*available = entries - n;
	return n;
}

/**
 * Enqueue several objects on the RTS ring (multi-producers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of objects.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   The number of objects enqueued, either 0 or n
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_mp_rts_enqueue_bulk_elem(struct rte_ring *r, const void *obj_table,
	unsigned int esize, unsigned int n, unsigned int *free_space)
{
	return __rte_ring_do_rts_enqueue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_FIXED, free_space);
}

/**
 * Dequeue several objects from an RTS ring (multi-consumers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of objects that will be filled.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects dequeued, either 0 or n
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_mc_rts_dequeue_bulk_elem(struct rte_ring *r, void *obj_table,
	unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_rts_dequeue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_FIXED, available);
}

/**
 * Enqueue several objects on the RTS ring (multi-producers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of objects.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   - n: Actual number of objects enqueued.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_mp_rts_enqueue_burst_elem(struct rte_ring *r, const void *obj_table,
	unsigned int esize, unsigned int n, unsigned int *free_space)
{
	return __rte_ring_do_rts_enqueue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_VARIABLE, free_space);
}

/**
 * Dequeue several objects from an RTS  ring (multi-consumers safe).
 * When the requested objects are more than the available objects,
 * only dequeue the actual number of objects.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of objects that will be filled.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   - n: Actual number of objects dequeued, 0 if ring is empty
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_mc_rts_dequeue_burst_elem(struct rte_ring *r, void *obj_table,
	unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_rts_dequeue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_VARIABLE, available);
}

/**
 * Enqueue several objects on the RTS ring (multi-producers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   The number of objects enqueued, either 0 or n
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_mp_rts_enqueue_bulk(struct rte_ring *r, void * const *obj_table,
			 unsigned int n, unsigned int *free_space)
{
	return rte_ring_mp_rts_enqueue_bulk_elem(r, obj_table,
			sizeof(uintptr_t), n, free_space);
}

/**
 * Dequeue several objects from an RTS ring (multi-consumers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects dequeued, either 0 or n
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_mc_rts_dequeue_bulk(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_mc_rts_dequeue_bulk_elem(r, obj_table,
			sizeof(uintptr_t), n, available);
}

/**
 * Enqueue several objects on the RTS ring (multi-producers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   - n: Actual number of objects enqueued.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_mp_rts_enqueue_burst(struct rte_ring *r, void * const *obj_table,
			 unsigned int n, unsigned int *free_space)
{
	return rte_ring_mp_rts_enqueue_burst_elem(r, obj_table,
			sizeof(uintptr_t), n, free_space);
}

/**
 * Dequeue several objects from an RTS  ring (multi-consumers safe).
 * When the requested objects are more than the available objects,
 * only dequeue the actual number of objects.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   - n: Actual number of objects dequeued, 0 if ring is empty
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_mc_rts_dequeue_burst(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_mc_rts_dequeue_burst_elem(r, obj_table,
			sizeof(uintptr_t), n, available);
}

/**
 * Return producer max Head-Tail-Distance (HTD).
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   Producer HTD value, if producer is set in appropriate sync mode,
 *   or UINT32_MAX otherwise.
 */
__rte_experimental
static inline uint32_t
rte_ring_get_prod_htd_max(const struct rte_ring *r)
{
	if (r->prod.sync_type == RTE_RING_SYNC_MT_RTS)
		return r->rts_prod.htd_max;
	return UINT32_MAX;
}

/**
 * Set producer max Head-Tail-Distance (HTD).
 * Note that producer has to use appropriate sync mode (RTS).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param v
 *   new HTD value to setup.
 * @return
 *   Zero on success, or negative error code otherwise.
 */
__rte_experimental
static inline int
rte_ring_set_prod_htd_max(struct rte_ring *r, uint32_t v)
{
	if (r->prod.sync_type != RTE_RING_SYNC_MT_RTS)
		return -ENOTSUP;

	r->rts_prod.htd_max = v;
	return 0;
}

/**
 * Return consumer max Head-Tail-Distance (HTD).
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   Consumer HTD value, if consumer is set in appropriate sync mode,
 *   or UINT32_MAX otherwise.
 */
__rte_experimental
static inline uint32_t
rte_ring_get_cons_htd_max(const struct rte_ring *r)
{
	if (r->cons.sync_type == RTE_RING_SYNC_MT_RTS)
		return r->rts_cons.htd_max;
	return UINT32_MAX;
}

/**
 * Set consumer max Head-Tail-Distance (HTD).
 * Note that consumer has to use appropriate sync mode (RTS).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param v
 *   new HTD value to setup.
 * @return
 *   Zero on success, or negative error code otherwise.
 */
__rte_experimental
static inline int
rte_ring_set_cons_htd_max(struct rte_ring *r, uint32_t v)
{
	if (r->cons.sync_type != RTE_RING_SYNC_MT_RTS)
		return -ENOTSUP;

	r->rts_cons.htd_max = v;
	return 0;
}

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RING_RTS_H_ */
