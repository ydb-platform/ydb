/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2010-2020 Intel Corporation
 * Copyright (c) 2007-2009 Kip Macy kmacy@freebsd.org
 * All rights reserved.
 * Derived from FreeBSD's bufring.h
 * Used as BSD-3 Licensed with permission from Kip Macy.
 */

#ifndef _RTE_RING_PEEK_H_
#define _RTE_RING_PEEK_H_

/**
 * @file
 * @b EXPERIMENTAL: this API may change without prior notice
 * It is not recommended to include this file directly.
 * Please include <rte_ring_elem.h> instead.
 *
 * Ring Peek API
 * Introduction of rte_ring with serialized producer/consumer (HTS sync mode)
 * makes possible to split public enqueue/dequeue API into two phases:
 * - enqueue/dequeue start
 * - enqueue/dequeue finish
 * That allows user to inspect objects in the ring without removing them
 * from it (aka MT safe peek).
 * Note that right now this new API is available only for two sync modes:
 * 1) Single Producer/Single Consumer (RTE_RING_SYNC_ST)
 * 2) Serialized Producer/Serialized Consumer (RTE_RING_SYNC_MT_HTS).
 * It is a user responsibility to create/init ring with appropriate sync
 * modes selected.
 * As an example:
 * // read 1 elem from the ring:
 * n = rte_ring_dequeue_bulk_start(ring, &obj, 1, NULL);
 * if (n != 0) {
 *    //examine object
 *    if (object_examine(obj) == KEEP)
 *       //decided to keep it in the ring.
 *       rte_ring_dequeue_finish(ring, 0);
 *    else
 *       //decided to remove it from the ring.
 *       rte_ring_dequeue_finish(ring, n);
 * }
 * Note that between _start_ and _finish_ none other thread can proceed
 * with enqueue(/dequeue) operation till _finish_ completes.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_ring_peek_c11_mem.h>

/**
 * @internal This function moves prod head value.
 */
static __rte_always_inline unsigned int
__rte_ring_do_enqueue_start(struct rte_ring *r, uint32_t n,
		enum rte_ring_queue_behavior behavior, uint32_t *free_space)
{
	uint32_t free, head, next;

	switch (r->prod.sync_type) {
	case RTE_RING_SYNC_ST:
		n = __rte_ring_move_prod_head(r, RTE_RING_SYNC_ST, n,
			behavior, &head, &next, &free);
		break;
	case RTE_RING_SYNC_MT_HTS:
		n =  __rte_ring_hts_move_prod_head(r, n, behavior,
			&head, &free);
		break;
	case RTE_RING_SYNC_MT:
	case RTE_RING_SYNC_MT_RTS:
	default:
		/* unsupported mode, shouldn't be here */
		RTE_ASSERT(0);
		n = 0;
		free = 0;
	}

	if (free_space != NULL)
		*free_space = free - n;
	return n;
}

/**
 * Start to enqueue several objects on the ring.
 * Note that no actual objects are put in the queue by this function,
 * it just reserves for user such ability.
 * User has to call appropriate enqueue_elem_finish() to copy objects into the
 * queue and complete given enqueue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   The number of objects that can be enqueued, either 0 or n
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_enqueue_bulk_elem_start(struct rte_ring *r, unsigned int n,
		unsigned int *free_space)
{
	return __rte_ring_do_enqueue_start(r, n, RTE_RING_QUEUE_FIXED,
			free_space);
}

/**
 * Start to enqueue several objects on the ring.
 * Note that no actual objects are put in the queue by this function,
 * it just reserves for user such ability.
 * User has to call appropriate enqueue_finish() to copy objects into the
 * queue and complete given enqueue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   The number of objects that can be enqueued, either 0 or n
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_enqueue_bulk_start(struct rte_ring *r, unsigned int n,
		unsigned int *free_space)
{
	return rte_ring_enqueue_bulk_elem_start(r, n, free_space);
}

/**
 * Start to enqueue several objects on the ring.
 * Note that no actual objects are put in the queue by this function,
 * it just reserves for user such ability.
 * User has to call appropriate enqueue_elem_finish() to copy objects into the
 * queue and complete given enqueue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   Actual number of objects that can be enqueued.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_enqueue_burst_elem_start(struct rte_ring *r, unsigned int n,
		unsigned int *free_space)
{
	return __rte_ring_do_enqueue_start(r, n, RTE_RING_QUEUE_VARIABLE,
			free_space);
}

/**
 * Start to enqueue several objects on the ring.
 * Note that no actual objects are put in the queue by this function,
 * it just reserves for user such ability.
 * User has to call appropriate enqueue_finish() to copy objects into the
 * queue and complete given enqueue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   Actual number of objects that can be enqueued.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_enqueue_burst_start(struct rte_ring *r, unsigned int n,
		unsigned int *free_space)
{
	return rte_ring_enqueue_burst_elem_start(r, n, free_space);
}

/**
 * Complete to enqueue several objects on the ring.
 * Note that number of objects to enqueue should not exceed previous
 * enqueue_start return value.
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
 *   The number of objects to add to the ring from the obj_table.
 */
__rte_experimental
static __rte_always_inline void
rte_ring_enqueue_elem_finish(struct rte_ring *r, const void *obj_table,
		unsigned int esize, unsigned int n)
{
	uint32_t tail;

	switch (r->prod.sync_type) {
	case RTE_RING_SYNC_ST:
		n = __rte_ring_st_get_tail(&r->prod, &tail, n);
		if (n != 0)
			__rte_ring_enqueue_elems(r, tail, obj_table, esize, n);
		__rte_ring_st_set_head_tail(&r->prod, tail, n, 1);
		break;
	case RTE_RING_SYNC_MT_HTS:
		n = __rte_ring_hts_get_tail(&r->hts_prod, &tail, n);
		if (n != 0)
			__rte_ring_enqueue_elems(r, tail, obj_table, esize, n);
		__rte_ring_hts_set_head_tail(&r->hts_prod, tail, n, 1);
		break;
	case RTE_RING_SYNC_MT:
	case RTE_RING_SYNC_MT_RTS:
	default:
		/* unsupported mode, shouldn't be here */
		RTE_ASSERT(0);
	}
}

/**
 * Complete to enqueue several objects on the ring.
 * Note that number of objects to enqueue should not exceed previous
 * enqueue_start return value.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of objects.
 * @param n
 *   The number of objects to add to the ring from the obj_table.
 */
__rte_experimental
static __rte_always_inline void
rte_ring_enqueue_finish(struct rte_ring *r, void * const *obj_table,
		unsigned int n)
{
	rte_ring_enqueue_elem_finish(r, obj_table, sizeof(uintptr_t), n);
}

/**
 * @internal This function moves cons head value and copies up to *n*
 * objects from the ring to the user provided obj_table.
 */
static __rte_always_inline unsigned int
__rte_ring_do_dequeue_start(struct rte_ring *r, void *obj_table,
	uint32_t esize, uint32_t n, enum rte_ring_queue_behavior behavior,
	uint32_t *available)
{
	uint32_t avail, head, next;

	switch (r->cons.sync_type) {
	case RTE_RING_SYNC_ST:
		n = __rte_ring_move_cons_head(r, RTE_RING_SYNC_ST, n,
			behavior, &head, &next, &avail);
		break;
	case RTE_RING_SYNC_MT_HTS:
		n =  __rte_ring_hts_move_cons_head(r, n, behavior,
			&head, &avail);
		break;
	case RTE_RING_SYNC_MT:
	case RTE_RING_SYNC_MT_RTS:
	default:
		/* unsupported mode, shouldn't be here */
		RTE_ASSERT(0);
		n = 0;
		avail = 0;
	}

	if (n != 0)
		__rte_ring_dequeue_elems(r, head, obj_table, esize, n);

	if (available != NULL)
		*available = avail - n;
	return n;
}

/**
 * Start to dequeue several objects from the ring.
 * Note that user has to call appropriate dequeue_finish()
 * to complete given dequeue operation and actually remove objects the ring.
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
 *   The number of objects dequeued, either 0 or n.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_dequeue_bulk_elem_start(struct rte_ring *r, void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_dequeue_start(r, obj_table, esize, n,
			RTE_RING_QUEUE_FIXED, available);
}

/**
 * Start to dequeue several objects from the ring.
 * Note that user has to call appropriate dequeue_finish()
 * to complete given dequeue operation and actually remove objects the ring.
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
 *   Actual number of objects dequeued.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_dequeue_bulk_start(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_dequeue_bulk_elem_start(r, obj_table, sizeof(uintptr_t),
		n, available);
}

/**
 * Start to dequeue several objects from the ring.
 * Note that user has to call appropriate dequeue_finish()
 * to complete given dequeue operation and actually remove objects the ring.
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
 *   The actual number of objects dequeued.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_dequeue_burst_elem_start(struct rte_ring *r, void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_dequeue_start(r, obj_table, esize, n,
			RTE_RING_QUEUE_VARIABLE, available);
}

/**
 * Start to dequeue several objects from the ring.
 * Note that user has to call appropriate dequeue_finish()
 * to complete given dequeue operation and actually remove objects the ring.
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
 *   The actual number of objects dequeued.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_dequeue_burst_start(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_dequeue_burst_elem_start(r, obj_table,
		sizeof(uintptr_t), n, available);
}

/**
 * Complete to dequeue several objects from the ring.
 * Note that number of objects to dequeue should not exceed previous
 * dequeue_start return value.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to remove from the ring.
 */
__rte_experimental
static __rte_always_inline void
rte_ring_dequeue_elem_finish(struct rte_ring *r, unsigned int n)
{
	uint32_t tail;

	switch (r->cons.sync_type) {
	case RTE_RING_SYNC_ST:
		n = __rte_ring_st_get_tail(&r->cons, &tail, n);
		__rte_ring_st_set_head_tail(&r->cons, tail, n, 0);
		break;
	case RTE_RING_SYNC_MT_HTS:
		n = __rte_ring_hts_get_tail(&r->hts_cons, &tail, n);
		__rte_ring_hts_set_head_tail(&r->hts_cons, tail, n, 0);
		break;
	case RTE_RING_SYNC_MT:
	case RTE_RING_SYNC_MT_RTS:
	default:
		/* unsupported mode, shouldn't be here */
		RTE_ASSERT(0);
	}
}

/**
 * Complete to dequeue several objects from the ring.
 * Note that number of objects to dequeue should not exceed previous
 * dequeue_start return value.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to remove from the ring.
 */
__rte_experimental
static __rte_always_inline void
rte_ring_dequeue_finish(struct rte_ring *r, unsigned int n)
{
	rte_ring_dequeue_elem_finish(r, n);
}

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RING_PEEK_H_ */
