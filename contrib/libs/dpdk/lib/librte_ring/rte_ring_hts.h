/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2010-2020 Intel Corporation
 * Copyright (c) 2007-2009 Kip Macy kmacy@freebsd.org
 * All rights reserved.
 * Derived from FreeBSD's bufring.h
 * Used as BSD-3 Licensed with permission from Kip Macy.
 */

#ifndef _RTE_RING_HTS_H_
#define _RTE_RING_HTS_H_

/**
 * @file rte_ring_hts.h
 * @b EXPERIMENTAL: this API may change without prior notice
 * It is not recommended to include this file directly.
 * Please include <rte_ring.h> instead.
 *
 * Contains functions for serialized, aka Head-Tail Sync (HTS) ring mode.
 * In that mode enqueue/dequeue operation is fully serialized:
 * at any given moment only one enqueue/dequeue operation can proceed.
 * This is achieved by allowing a thread to proceed with changing head.value
 * only when head.value == tail.value.
 * Both head and tail values are updated atomically (as one 64-bit value).
 * To achieve that 64-bit CAS is used by head update routine.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_ring_hts_c11_mem.h>

/**
 * @internal Enqueue several objects on the HTS ring.
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
__rte_ring_do_hts_enqueue_elem(struct rte_ring *r, const void *obj_table,
	uint32_t esize, uint32_t n, enum rte_ring_queue_behavior behavior,
	uint32_t *free_space)
{
	uint32_t free, head;

	n =  __rte_ring_hts_move_prod_head(r, n, behavior, &head, &free);

	if (n != 0) {
		__rte_ring_enqueue_elems(r, head, obj_table, esize, n);
		__rte_ring_hts_update_tail(&r->hts_prod, head, n, 1);
	}

	if (free_space != NULL)
		*free_space = free - n;
	return n;
}

/**
 * @internal Dequeue several objects from the HTS ring.
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
__rte_ring_do_hts_dequeue_elem(struct rte_ring *r, void *obj_table,
	uint32_t esize, uint32_t n, enum rte_ring_queue_behavior behavior,
	uint32_t *available)
{
	uint32_t entries, head;

	n = __rte_ring_hts_move_cons_head(r, n, behavior, &head, &entries);

	if (n != 0) {
		__rte_ring_dequeue_elems(r, head, obj_table, esize, n);
		__rte_ring_hts_update_tail(&r->hts_cons, head, n, 0);
	}

	if (available != NULL)
		*available = entries - n;
	return n;
}

/**
 * Enqueue several objects on the HTS ring (multi-producers safe).
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
rte_ring_mp_hts_enqueue_bulk_elem(struct rte_ring *r, const void *obj_table,
	unsigned int esize, unsigned int n, unsigned int *free_space)
{
	return __rte_ring_do_hts_enqueue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_FIXED, free_space);
}

/**
 * Dequeue several objects from an HTS ring (multi-consumers safe).
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
rte_ring_mc_hts_dequeue_bulk_elem(struct rte_ring *r, void *obj_table,
	unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_hts_dequeue_elem(r, obj_table, esize, n,
		RTE_RING_QUEUE_FIXED, available);
}

/**
 * Enqueue several objects on the HTS ring (multi-producers safe).
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
rte_ring_mp_hts_enqueue_burst_elem(struct rte_ring *r, const void *obj_table,
	unsigned int esize, unsigned int n, unsigned int *free_space)
{
	return __rte_ring_do_hts_enqueue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_VARIABLE, free_space);
}

/**
 * Dequeue several objects from an HTS  ring (multi-consumers safe).
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
rte_ring_mc_hts_dequeue_burst_elem(struct rte_ring *r, void *obj_table,
	unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_hts_dequeue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_VARIABLE, available);
}

/**
 * Enqueue several objects on the HTS ring (multi-producers safe).
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
rte_ring_mp_hts_enqueue_bulk(struct rte_ring *r, void * const *obj_table,
			 unsigned int n, unsigned int *free_space)
{
	return rte_ring_mp_hts_enqueue_bulk_elem(r, obj_table,
			sizeof(uintptr_t), n, free_space);
}

/**
 * Dequeue several objects from an HTS ring (multi-consumers safe).
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
rte_ring_mc_hts_dequeue_bulk(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_mc_hts_dequeue_bulk_elem(r, obj_table,
			sizeof(uintptr_t), n, available);
}

/**
 * Enqueue several objects on the HTS ring (multi-producers safe).
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
rte_ring_mp_hts_enqueue_burst(struct rte_ring *r, void * const *obj_table,
			 unsigned int n, unsigned int *free_space)
{
	return rte_ring_mp_hts_enqueue_burst_elem(r, obj_table,
			sizeof(uintptr_t), n, free_space);
}

/**
 * Dequeue several objects from an HTS  ring (multi-consumers safe).
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
rte_ring_mc_hts_dequeue_burst(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_mc_hts_dequeue_burst_elem(r, obj_table,
			sizeof(uintptr_t), n, available);
}

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RING_HTS_H_ */
