/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2020 Arm Limited
 * Copyright (c) 2007-2009 Kip Macy kmacy@freebsd.org
 * All rights reserved.
 * Derived from FreeBSD's bufring.h
 * Used as BSD-3 Licensed with permission from Kip Macy.
 */

#ifndef _RTE_RING_PEEK_ZC_H_
#define _RTE_RING_PEEK_ZC_H_

/**
 * @file
 * @b EXPERIMENTAL: this API may change without prior notice
 * It is not recommended to include this file directly.
 * Please include <rte_ring_elem.h> instead.
 *
 * Ring Peek Zero Copy APIs
 * These APIs make it possible to split public enqueue/dequeue API
 * into 3 parts:
 * - enqueue/dequeue start
 * - copy data to/from the ring
 * - enqueue/dequeue finish
 * Along with the advantages of the peek APIs, these APIs provide the ability
 * to avoid copying of the data to temporary area (for ex: array of mbufs
 * on the stack).
 *
 * Note that currently these APIs are available only for two sync modes:
 * 1) Single Producer/Single Consumer (RTE_RING_SYNC_ST)
 * 2) Serialized Producer/Serialized Consumer (RTE_RING_SYNC_MT_HTS).
 * It is user's responsibility to create/init ring with appropriate sync
 * modes selected.
 *
 * Following are some examples showing the API usage.
 * 1)
 * struct elem_obj {uint64_t a; uint32_t b, c;};
 * struct elem_obj *obj;
 *
 * // Create ring with sync type RTE_RING_SYNC_ST or RTE_RING_SYNC_MT_HTS
 * // Reserve space on the ring
 * n = rte_ring_enqueue_zc_bulk_elem_start(r, sizeof(elem_obj), 1, &zcd, NULL);
 *
 * // Produce the data directly on the ring memory
 * obj = (struct elem_obj *)zcd->ptr1;
 * obj->a = rte_get_a();
 * obj->b = rte_get_b();
 * obj->c = rte_get_c();
 * rte_ring_enqueue_zc_elem_finish(ring, n);
 *
 * 2)
 * // Create ring with sync type RTE_RING_SYNC_ST or RTE_RING_SYNC_MT_HTS
 * // Reserve space on the ring
 * n = rte_ring_enqueue_zc_burst_start(r, 32, &zcd, NULL);
 *
 * // Pkt I/O core polls packets from the NIC
 * if (n != 0) {
 *	nb_rx = rte_eth_rx_burst(portid, queueid, zcd->ptr1, zcd->n1);
 *	if (nb_rx == zcd->n1 && n != zcd->n1)
 *		nb_rx = rte_eth_rx_burst(portid, queueid,
 *						zcd->ptr2, n - zcd->n1);
 *
 *	// Provide packets to the packet processing cores
 *	rte_ring_enqueue_zc_finish(r, nb_rx);
 * }
 *
 * Note that between _start_ and _finish_ none other thread can proceed
 * with enqueue/dequeue operation till _finish_ completes.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_ring_peek_c11_mem.h>

/**
 * Ring zero-copy information structure.
 *
 * This structure contains the pointers and length of the space
 * reserved on the ring storage.
 */
struct rte_ring_zc_data {
	/* Pointer to the first space in the ring */
	void *ptr1;
	/* Pointer to the second space in the ring if there is wrap-around.
	 * It contains valid value only if wrap-around happens.
	 */
	void *ptr2;
	/* Number of elements in the first pointer. If this is equal to
	 * the number of elements requested, then ptr2 is NULL.
	 * Otherwise, subtracting n1 from number of elements requested
	 * will give the number of elements available at ptr2.
	 */
	unsigned int n1;
} __rte_cache_aligned;

static __rte_always_inline void
__rte_ring_get_elem_addr(struct rte_ring *r, uint32_t head,
	uint32_t esize, uint32_t num, void **dst1, uint32_t *n1, void **dst2)
{
	uint32_t idx, scale, nr_idx;
	uint32_t *ring = (uint32_t *)&r[1];

	/* Normalize to uint32_t */
	scale = esize / sizeof(uint32_t);
	idx = head & r->mask;
	nr_idx = idx * scale;

	*dst1 = ring + nr_idx;
	*n1 = num;

	if (idx + num > r->size) {
		*n1 = r->size - idx;
		*dst2 = ring;
	} else {
		*dst2 = NULL;
	}
}

/**
 * @internal This function moves prod head value.
 */
static __rte_always_inline unsigned int
__rte_ring_do_enqueue_zc_elem_start(struct rte_ring *r, unsigned int esize,
		uint32_t n, enum rte_ring_queue_behavior behavior,
		struct rte_ring_zc_data *zcd, unsigned int *free_space)
{
	uint32_t free, head, next;

	switch (r->prod.sync_type) {
	case RTE_RING_SYNC_ST:
		n = __rte_ring_move_prod_head(r, RTE_RING_SYNC_ST, n,
			behavior, &head, &next, &free);
		break;
	case RTE_RING_SYNC_MT_HTS:
		n = __rte_ring_hts_move_prod_head(r, n, behavior, &head, &free);
		break;
	case RTE_RING_SYNC_MT:
	case RTE_RING_SYNC_MT_RTS:
	default:
		/* unsupported mode, shouldn't be here */
		RTE_ASSERT(0);
		n = 0;
		free = 0;
		return n;
	}

	__rte_ring_get_elem_addr(r, head, esize, n, &zcd->ptr1,
		&zcd->n1, &zcd->ptr2);

	if (free_space != NULL)
		*free_space = free - n;
	return n;
}

/**
 * Start to enqueue several objects on the ring.
 * Note that no actual objects are put in the queue by this function,
 * it just reserves space for the user on the ring.
 * User has to copy objects into the queue using the returned pointers.
 * User should call rte_ring_enqueue_zc_elem_finish to complete the
 * enqueue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 * @param n
 *   The number of objects to add in the ring.
 * @param zcd
 *   Structure containing the pointers and length of the space
 *   reserved on the ring storage.
 * @param free_space
 *   If non-NULL, returns the amount of space in the ring after the
 *   reservation operation has finished.
 * @return
 *   The number of objects that can be enqueued, either 0 or n
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_enqueue_zc_bulk_elem_start(struct rte_ring *r, unsigned int esize,
	unsigned int n, struct rte_ring_zc_data *zcd, unsigned int *free_space)
{
	return __rte_ring_do_enqueue_zc_elem_start(r, esize, n,
			RTE_RING_QUEUE_FIXED, zcd, free_space);
}

/**
 * Start to enqueue several pointers to objects on the ring.
 * Note that no actual pointers are put in the queue by this function,
 * it just reserves space for the user on the ring.
 * User has to copy pointers to objects into the queue using the
 * returned pointers.
 * User should call rte_ring_enqueue_zc_finish to complete the
 * enqueue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to add in the ring.
 * @param zcd
 *   Structure containing the pointers and length of the space
 *   reserved on the ring storage.
 * @param free_space
 *   If non-NULL, returns the amount of space in the ring after the
 *   reservation operation has finished.
 * @return
 *   The number of objects that can be enqueued, either 0 or n
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_enqueue_zc_bulk_start(struct rte_ring *r, unsigned int n,
	struct rte_ring_zc_data *zcd, unsigned int *free_space)
{
	return rte_ring_enqueue_zc_bulk_elem_start(r, sizeof(uintptr_t), n,
							zcd, free_space);
}

/**
 * Start to enqueue several objects on the ring.
 * Note that no actual objects are put in the queue by this function,
 * it just reserves space for the user on the ring.
 * User has to copy objects into the queue using the returned pointers.
 * User should call rte_ring_enqueue_zc_elem_finish to complete the
 * enqueue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 * @param n
 *   The number of objects to add in the ring.
 * @param zcd
 *   Structure containing the pointers and length of the space
 *   reserved on the ring storage.
 * @param free_space
 *   If non-NULL, returns the amount of space in the ring after the
 *   reservation operation has finished.
 * @return
 *   The number of objects that can be enqueued, either 0 or n
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_enqueue_zc_burst_elem_start(struct rte_ring *r, unsigned int esize,
	unsigned int n, struct rte_ring_zc_data *zcd, unsigned int *free_space)
{
	return __rte_ring_do_enqueue_zc_elem_start(r, esize, n,
			RTE_RING_QUEUE_VARIABLE, zcd, free_space);
}

/**
 * Start to enqueue several pointers to objects on the ring.
 * Note that no actual pointers are put in the queue by this function,
 * it just reserves space for the user on the ring.
 * User has to copy pointers to objects into the queue using the
 * returned pointers.
 * User should call rte_ring_enqueue_zc_finish to complete the
 * enqueue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to add in the ring.
 * @param zcd
 *   Structure containing the pointers and length of the space
 *   reserved on the ring storage.
 * @param free_space
 *   If non-NULL, returns the amount of space in the ring after the
 *   reservation operation has finished.
 * @return
 *   The number of objects that can be enqueued, either 0 or n.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_enqueue_zc_burst_start(struct rte_ring *r, unsigned int n,
	struct rte_ring_zc_data *zcd, unsigned int *free_space)
{
	return rte_ring_enqueue_zc_burst_elem_start(r, sizeof(uintptr_t), n,
							zcd, free_space);
}

/**
 * Complete enqueuing several objects on the ring.
 * Note that number of objects to enqueue should not exceed previous
 * enqueue_start return value.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to add to the ring.
 */
__rte_experimental
static __rte_always_inline void
rte_ring_enqueue_zc_elem_finish(struct rte_ring *r, unsigned int n)
{
	uint32_t tail;

	switch (r->prod.sync_type) {
	case RTE_RING_SYNC_ST:
		n = __rte_ring_st_get_tail(&r->prod, &tail, n);
		__rte_ring_st_set_head_tail(&r->prod, tail, n, 1);
		break;
	case RTE_RING_SYNC_MT_HTS:
		n = __rte_ring_hts_get_tail(&r->hts_prod, &tail, n);
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
 * Complete enqueuing several pointers to objects on the ring.
 * Note that number of objects to enqueue should not exceed previous
 * enqueue_start return value.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of pointers to objects to add to the ring.
 */
__rte_experimental
static __rte_always_inline void
rte_ring_enqueue_zc_finish(struct rte_ring *r, unsigned int n)
{
	rte_ring_enqueue_zc_elem_finish(r, n);
}

/**
 * @internal This function moves cons head value and copies up to *n*
 * objects from the ring to the user provided obj_table.
 */
static __rte_always_inline unsigned int
__rte_ring_do_dequeue_zc_elem_start(struct rte_ring *r,
	uint32_t esize, uint32_t n, enum rte_ring_queue_behavior behavior,
	struct rte_ring_zc_data *zcd, unsigned int *available)
{
	uint32_t avail, head, next;

	switch (r->cons.sync_type) {
	case RTE_RING_SYNC_ST:
		n = __rte_ring_move_cons_head(r, RTE_RING_SYNC_ST, n,
			behavior, &head, &next, &avail);
		break;
	case RTE_RING_SYNC_MT_HTS:
		n = __rte_ring_hts_move_cons_head(r, n, behavior,
			&head, &avail);
		break;
	case RTE_RING_SYNC_MT:
	case RTE_RING_SYNC_MT_RTS:
	default:
		/* unsupported mode, shouldn't be here */
		RTE_ASSERT(0);
		n = 0;
		avail = 0;
		return n;
	}

	__rte_ring_get_elem_addr(r, head, esize, n, &zcd->ptr1,
		&zcd->n1, &zcd->ptr2);

	if (available != NULL)
		*available = avail - n;
	return n;
}

/**
 * Start to dequeue several objects from the ring.
 * Note that no actual objects are copied from the queue by this function.
 * User has to copy objects from the queue using the returned pointers.
 * User should call rte_ring_dequeue_zc_elem_finish to complete the
 * dequeue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 * @param n
 *   The number of objects to remove from the ring.
 * @param zcd
 *   Structure containing the pointers and length of the space
 *   reserved on the ring storage.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects that can be dequeued, either 0 or n.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_dequeue_zc_bulk_elem_start(struct rte_ring *r, unsigned int esize,
	unsigned int n, struct rte_ring_zc_data *zcd, unsigned int *available)
{
	return __rte_ring_do_dequeue_zc_elem_start(r, esize, n,
			RTE_RING_QUEUE_FIXED, zcd, available);
}

/**
 * Start to dequeue several pointers to objects from the ring.
 * Note that no actual pointers are removed from the queue by this function.
 * User has to copy pointers to objects from the queue using the
 * returned pointers.
 * User should call rte_ring_dequeue_zc_finish to complete the
 * dequeue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to remove from the ring.
 * @param zcd
 *   Structure containing the pointers and length of the space
 *   reserved on the ring storage.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects that can be dequeued, either 0 or n.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_dequeue_zc_bulk_start(struct rte_ring *r, unsigned int n,
	struct rte_ring_zc_data *zcd, unsigned int *available)
{
	return rte_ring_dequeue_zc_bulk_elem_start(r, sizeof(uintptr_t),
		n, zcd, available);
}

/**
 * Start to dequeue several objects from the ring.
 * Note that no actual objects are copied from the queue by this function.
 * User has to copy objects from the queue using the returned pointers.
 * User should call rte_ring_dequeue_zc_elem_finish to complete the
 * dequeue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @param n
 *   The number of objects to dequeue from the ring.
 * @param zcd
 *   Structure containing the pointers and length of the space
 *   reserved on the ring storage.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects that can be dequeued, either 0 or n.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_dequeue_zc_burst_elem_start(struct rte_ring *r, unsigned int esize,
	unsigned int n, struct rte_ring_zc_data *zcd, unsigned int *available)
{
	return __rte_ring_do_dequeue_zc_elem_start(r, esize, n,
			RTE_RING_QUEUE_VARIABLE, zcd, available);
}

/**
 * Start to dequeue several pointers to objects from the ring.
 * Note that no actual pointers are removed from the queue by this function.
 * User has to copy pointers to objects from the queue using the
 * returned pointers.
 * User should call rte_ring_dequeue_zc_finish to complete the
 * dequeue operation.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to remove from the ring.
 * @param zcd
 *   Structure containing the pointers and length of the space
 *   reserved on the ring storage.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects that can be dequeued, either 0 or n.
 */
__rte_experimental
static __rte_always_inline unsigned int
rte_ring_dequeue_zc_burst_start(struct rte_ring *r, unsigned int n,
		struct rte_ring_zc_data *zcd, unsigned int *available)
{
	return rte_ring_dequeue_zc_burst_elem_start(r, sizeof(uintptr_t), n,
			zcd, available);
}

/**
 * Complete dequeuing several objects from the ring.
 * Note that number of objects to dequeued should not exceed previous
 * dequeue_start return value.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to remove from the ring.
 */
__rte_experimental
static __rte_always_inline void
rte_ring_dequeue_zc_elem_finish(struct rte_ring *r, unsigned int n)
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
 * Complete dequeuing several objects from the ring.
 * Note that number of objects to dequeued should not exceed previous
 * dequeue_start return value.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param n
 *   The number of objects to remove from the ring.
 */
__rte_experimental
static __rte_always_inline void
rte_ring_dequeue_zc_finish(struct rte_ring *r, unsigned int n)
{
	rte_ring_dequeue_elem_finish(r, n);
}

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RING_PEEK_ZC_H_ */
