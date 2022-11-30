/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2019 Arm Limited
 * Copyright (c) 2010-2017 Intel Corporation
 * Copyright (c) 2007-2009 Kip Macy kmacy@freebsd.org
 * All rights reserved.
 * Derived from FreeBSD's bufring.h
 * Used as BSD-3 Licensed with permission from Kip Macy.
 */

#ifndef _RTE_RING_ELEM_H_
#define _RTE_RING_ELEM_H_

/**
 * @file
 * RTE Ring with user defined element size
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_ring_core.h>

/**
 * Calculate the memory size needed for a ring with given element size
 *
 * This function returns the number of bytes needed for a ring, given
 * the number of elements in it and the size of the element. This value
 * is the sum of the size of the structure rte_ring and the size of the
 * memory needed for storing the elements. The value is aligned to a cache
 * line size.
 *
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 * @param count
 *   The number of elements in the ring (must be a power of 2).
 * @return
 *   - The memory size needed for the ring on success.
 *   - -EINVAL - esize is not a multiple of 4 or count provided is not a
 *		 power of 2.
 */
ssize_t rte_ring_get_memsize_elem(unsigned int esize, unsigned int count);

/**
 * Create a new ring named *name* that stores elements with given size.
 *
 * This function uses ``memzone_reserve()`` to allocate memory. Then it
 * calls rte_ring_init() to initialize an empty ring.
 *
 * The new ring size is set to *count*, which must be a power of
 * two. Water marking is disabled by default. The real usable ring size
 * is *count-1* instead of *count* to differentiate a free ring from an
 * empty ring.
 *
 * The ring is added in RTE_TAILQ_RING list.
 *
 * @param name
 *   The name of the ring.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 * @param count
 *   The number of elements in the ring (must be a power of 2).
 * @param socket_id
 *   The *socket_id* argument is the socket identifier in case of
 *   NUMA. The value can be *SOCKET_ID_ANY* if there is no NUMA
 *   constraint for the reserved zone.
 * @param flags
 *   An OR of the following:
 *   - One of mutually exclusive flags that define producer behavior:
 *      - RING_F_SP_ENQ: If this flag is set, the default behavior when
 *        using ``rte_ring_enqueue()`` or ``rte_ring_enqueue_bulk()``
 *        is "single-producer".
 *      - RING_F_MP_RTS_ENQ: If this flag is set, the default behavior when
 *        using ``rte_ring_enqueue()`` or ``rte_ring_enqueue_bulk()``
 *        is "multi-producer RTS mode".
 *      - RING_F_MP_HTS_ENQ: If this flag is set, the default behavior when
 *        using ``rte_ring_enqueue()`` or ``rte_ring_enqueue_bulk()``
 *        is "multi-producer HTS mode".
 *     If none of these flags is set, then default "multi-producer"
 *     behavior is selected.
 *   - One of mutually exclusive flags that define consumer behavior:
 *      - RING_F_SC_DEQ: If this flag is set, the default behavior when
 *        using ``rte_ring_dequeue()`` or ``rte_ring_dequeue_bulk()``
 *        is "single-consumer". Otherwise, it is "multi-consumers".
 *      - RING_F_MC_RTS_DEQ: If this flag is set, the default behavior when
 *        using ``rte_ring_dequeue()`` or ``rte_ring_dequeue_bulk()``
 *        is "multi-consumer RTS mode".
 *      - RING_F_MC_HTS_DEQ: If this flag is set, the default behavior when
 *        using ``rte_ring_dequeue()`` or ``rte_ring_dequeue_bulk()``
 *        is "multi-consumer HTS mode".
 *     If none of these flags is set, then default "multi-consumer"
 *     behavior is selected.
 * @return
 *   On success, the pointer to the new allocated ring. NULL on error with
 *    rte_errno set appropriately. Possible errno values include:
 *    - E_RTE_NO_CONFIG - function could not get pointer to rte_config structure
 *    - E_RTE_SECONDARY - function was called from a secondary process instance
 *    - EINVAL - esize is not a multiple of 4 or count provided is not a
 *		 power of 2.
 *    - ENOSPC - the maximum number of memzones has already been allocated
 *    - EEXIST - a memzone with the same name already exists
 *    - ENOMEM - no appropriate memory area found in which to create memzone
 */
struct rte_ring *rte_ring_create_elem(const char *name, unsigned int esize,
			unsigned int count, int socket_id, unsigned int flags);

static __rte_always_inline void
__rte_ring_enqueue_elems_32(struct rte_ring *r, const uint32_t size,
		uint32_t idx, const void *obj_table, uint32_t n)
{
	unsigned int i;
	uint32_t *ring = (uint32_t *)&r[1];
	const uint32_t *obj = (const uint32_t *)obj_table;
	if (likely(idx + n < size)) {
		for (i = 0; i < (n & ~0x7); i += 8, idx += 8) {
			ring[idx] = obj[i];
			ring[idx + 1] = obj[i + 1];
			ring[idx + 2] = obj[i + 2];
			ring[idx + 3] = obj[i + 3];
			ring[idx + 4] = obj[i + 4];
			ring[idx + 5] = obj[i + 5];
			ring[idx + 6] = obj[i + 6];
			ring[idx + 7] = obj[i + 7];
		}
		switch (n & 0x7) {
		case 7:
			ring[idx++] = obj[i++]; /* fallthrough */
		case 6:
			ring[idx++] = obj[i++]; /* fallthrough */
		case 5:
			ring[idx++] = obj[i++]; /* fallthrough */
		case 4:
			ring[idx++] = obj[i++]; /* fallthrough */
		case 3:
			ring[idx++] = obj[i++]; /* fallthrough */
		case 2:
			ring[idx++] = obj[i++]; /* fallthrough */
		case 1:
			ring[idx++] = obj[i++]; /* fallthrough */
		}
	} else {
		for (i = 0; idx < size; i++, idx++)
			ring[idx] = obj[i];
		/* Start at the beginning */
		for (idx = 0; i < n; i++, idx++)
			ring[idx] = obj[i];
	}
}

static __rte_always_inline void
__rte_ring_enqueue_elems_64(struct rte_ring *r, uint32_t prod_head,
		const void *obj_table, uint32_t n)
{
	unsigned int i;
	const uint32_t size = r->size;
	uint32_t idx = prod_head & r->mask;
	uint64_t *ring = (uint64_t *)&r[1];
	const unaligned_uint64_t *obj = (const unaligned_uint64_t *)obj_table;
	if (likely(idx + n < size)) {
		for (i = 0; i < (n & ~0x3); i += 4, idx += 4) {
			ring[idx] = obj[i];
			ring[idx + 1] = obj[i + 1];
			ring[idx + 2] = obj[i + 2];
			ring[idx + 3] = obj[i + 3];
		}
		switch (n & 0x3) {
		case 3:
			ring[idx++] = obj[i++]; /* fallthrough */
		case 2:
			ring[idx++] = obj[i++]; /* fallthrough */
		case 1:
			ring[idx++] = obj[i++];
		}
	} else {
		for (i = 0; idx < size; i++, idx++)
			ring[idx] = obj[i];
		/* Start at the beginning */
		for (idx = 0; i < n; i++, idx++)
			ring[idx] = obj[i];
	}
}

static __rte_always_inline void
__rte_ring_enqueue_elems_128(struct rte_ring *r, uint32_t prod_head,
		const void *obj_table, uint32_t n)
{
	unsigned int i;
	const uint32_t size = r->size;
	uint32_t idx = prod_head & r->mask;
	rte_int128_t *ring = (rte_int128_t *)&r[1];
	const rte_int128_t *obj = (const rte_int128_t *)obj_table;
	if (likely(idx + n < size)) {
		for (i = 0; i < (n & ~0x1); i += 2, idx += 2)
			memcpy((void *)(ring + idx),
				(const void *)(obj + i), 32);
		switch (n & 0x1) {
		case 1:
			memcpy((void *)(ring + idx),
				(const void *)(obj + i), 16);
		}
	} else {
		for (i = 0; idx < size; i++, idx++)
			memcpy((void *)(ring + idx),
				(const void *)(obj + i), 16);
		/* Start at the beginning */
		for (idx = 0; i < n; i++, idx++)
			memcpy((void *)(ring + idx),
				(const void *)(obj + i), 16);
	}
}

/* the actual enqueue of elements on the ring.
 * Placed here since identical code needed in both
 * single and multi producer enqueue functions.
 */
static __rte_always_inline void
__rte_ring_enqueue_elems(struct rte_ring *r, uint32_t prod_head,
		const void *obj_table, uint32_t esize, uint32_t num)
{
	/* 8B and 16B copies implemented individually to retain
	 * the current performance.
	 */
	if (esize == 8)
		__rte_ring_enqueue_elems_64(r, prod_head, obj_table, num);
	else if (esize == 16)
		__rte_ring_enqueue_elems_128(r, prod_head, obj_table, num);
	else {
		uint32_t idx, scale, nr_idx, nr_num, nr_size;

		/* Normalize to uint32_t */
		scale = esize / sizeof(uint32_t);
		nr_num = num * scale;
		idx = prod_head & r->mask;
		nr_idx = idx * scale;
		nr_size = r->size * scale;
		__rte_ring_enqueue_elems_32(r, nr_size, nr_idx,
				obj_table, nr_num);
	}
}

static __rte_always_inline void
__rte_ring_dequeue_elems_32(struct rte_ring *r, const uint32_t size,
		uint32_t idx, void *obj_table, uint32_t n)
{
	unsigned int i;
	uint32_t *ring = (uint32_t *)&r[1];
	uint32_t *obj = (uint32_t *)obj_table;
	if (likely(idx + n < size)) {
		for (i = 0; i < (n & ~0x7); i += 8, idx += 8) {
			obj[i] = ring[idx];
			obj[i + 1] = ring[idx + 1];
			obj[i + 2] = ring[idx + 2];
			obj[i + 3] = ring[idx + 3];
			obj[i + 4] = ring[idx + 4];
			obj[i + 5] = ring[idx + 5];
			obj[i + 6] = ring[idx + 6];
			obj[i + 7] = ring[idx + 7];
		}
		switch (n & 0x7) {
		case 7:
			obj[i++] = ring[idx++]; /* fallthrough */
		case 6:
			obj[i++] = ring[idx++]; /* fallthrough */
		case 5:
			obj[i++] = ring[idx++]; /* fallthrough */
		case 4:
			obj[i++] = ring[idx++]; /* fallthrough */
		case 3:
			obj[i++] = ring[idx++]; /* fallthrough */
		case 2:
			obj[i++] = ring[idx++]; /* fallthrough */
		case 1:
			obj[i++] = ring[idx++]; /* fallthrough */
		}
	} else {
		for (i = 0; idx < size; i++, idx++)
			obj[i] = ring[idx];
		/* Start at the beginning */
		for (idx = 0; i < n; i++, idx++)
			obj[i] = ring[idx];
	}
}

static __rte_always_inline void
__rte_ring_dequeue_elems_64(struct rte_ring *r, uint32_t prod_head,
		void *obj_table, uint32_t n)
{
	unsigned int i;
	const uint32_t size = r->size;
	uint32_t idx = prod_head & r->mask;
	uint64_t *ring = (uint64_t *)&r[1];
	unaligned_uint64_t *obj = (unaligned_uint64_t *)obj_table;
	if (likely(idx + n < size)) {
		for (i = 0; i < (n & ~0x3); i += 4, idx += 4) {
			obj[i] = ring[idx];
			obj[i + 1] = ring[idx + 1];
			obj[i + 2] = ring[idx + 2];
			obj[i + 3] = ring[idx + 3];
		}
		switch (n & 0x3) {
		case 3:
			obj[i++] = ring[idx++]; /* fallthrough */
		case 2:
			obj[i++] = ring[idx++]; /* fallthrough */
		case 1:
			obj[i++] = ring[idx++]; /* fallthrough */
		}
	} else {
		for (i = 0; idx < size; i++, idx++)
			obj[i] = ring[idx];
		/* Start at the beginning */
		for (idx = 0; i < n; i++, idx++)
			obj[i] = ring[idx];
	}
}

static __rte_always_inline void
__rte_ring_dequeue_elems_128(struct rte_ring *r, uint32_t prod_head,
		void *obj_table, uint32_t n)
{
	unsigned int i;
	const uint32_t size = r->size;
	uint32_t idx = prod_head & r->mask;
	rte_int128_t *ring = (rte_int128_t *)&r[1];
	rte_int128_t *obj = (rte_int128_t *)obj_table;
	if (likely(idx + n < size)) {
		for (i = 0; i < (n & ~0x1); i += 2, idx += 2)
			memcpy((void *)(obj + i), (void *)(ring + idx), 32);
		switch (n & 0x1) {
		case 1:
			memcpy((void *)(obj + i), (void *)(ring + idx), 16);
		}
	} else {
		for (i = 0; idx < size; i++, idx++)
			memcpy((void *)(obj + i), (void *)(ring + idx), 16);
		/* Start at the beginning */
		for (idx = 0; i < n; i++, idx++)
			memcpy((void *)(obj + i), (void *)(ring + idx), 16);
	}
}

/* the actual dequeue of elements from the ring.
 * Placed here since identical code needed in both
 * single and multi producer enqueue functions.
 */
static __rte_always_inline void
__rte_ring_dequeue_elems(struct rte_ring *r, uint32_t cons_head,
		void *obj_table, uint32_t esize, uint32_t num)
{
	/* 8B and 16B copies implemented individually to retain
	 * the current performance.
	 */
	if (esize == 8)
		__rte_ring_dequeue_elems_64(r, cons_head, obj_table, num);
	else if (esize == 16)
		__rte_ring_dequeue_elems_128(r, cons_head, obj_table, num);
	else {
		uint32_t idx, scale, nr_idx, nr_num, nr_size;

		/* Normalize to uint32_t */
		scale = esize / sizeof(uint32_t);
		nr_num = num * scale;
		idx = cons_head & r->mask;
		nr_idx = idx * scale;
		nr_size = r->size * scale;
		__rte_ring_dequeue_elems_32(r, nr_size, nr_idx,
				obj_table, nr_num);
	}
}

/* Between load and load. there might be cpu reorder in weak model
 * (powerpc/arm).
 * There are 2 choices for the users
 * 1.use rmb() memory barrier
 * 2.use one-direction load_acquire/store_release barrier
 * It depends on performance test results.
 * By default, move common functions to rte_ring_generic.h
 */
#ifdef RTE_USE_C11_MEM_MODEL
#include "rte_ring_c11_mem.h"
#else
#include "rte_ring_generic.h"
#endif

/**
 * @internal Enqueue several objects on the ring
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
 * @param is_sp
 *   Indicates whether to use single producer or multi-producer head update
 * @param free_space
 *   returns the amount of space after the enqueue operation has finished
 * @return
 *   Actual number of objects enqueued.
 *   If behavior == RTE_RING_QUEUE_FIXED, this will be 0 or n only.
 */
static __rte_always_inline unsigned int
__rte_ring_do_enqueue_elem(struct rte_ring *r, const void *obj_table,
		unsigned int esize, unsigned int n,
		enum rte_ring_queue_behavior behavior, unsigned int is_sp,
		unsigned int *free_space)
{
	uint32_t prod_head, prod_next;
	uint32_t free_entries;

	n = __rte_ring_move_prod_head(r, is_sp, n, behavior,
			&prod_head, &prod_next, &free_entries);
	if (n == 0)
		goto end;

	__rte_ring_enqueue_elems(r, prod_head, obj_table, esize, n);

	update_tail(&r->prod, prod_head, prod_next, is_sp, 1);
end:
	if (free_space != NULL)
		*free_space = free_entries - n;
	return n;
}

/**
 * @internal Dequeue several objects from the ring
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
 * @param is_sc
 *   Indicates whether to use single consumer or multi-consumer head update
 * @param available
 *   returns the number of remaining ring entries after the dequeue has finished
 * @return
 *   - Actual number of objects dequeued.
 *     If behavior == RTE_RING_QUEUE_FIXED, this will be 0 or n only.
 */
static __rte_always_inline unsigned int
__rte_ring_do_dequeue_elem(struct rte_ring *r, void *obj_table,
		unsigned int esize, unsigned int n,
		enum rte_ring_queue_behavior behavior, unsigned int is_sc,
		unsigned int *available)
{
	uint32_t cons_head, cons_next;
	uint32_t entries;

	n = __rte_ring_move_cons_head(r, (int)is_sc, n, behavior,
			&cons_head, &cons_next, &entries);
	if (n == 0)
		goto end;

	__rte_ring_dequeue_elems(r, cons_head, obj_table, esize, n);

	update_tail(&r->cons, cons_head, cons_next, is_sc, 0);

end:
	if (available != NULL)
		*available = entries - n;
	return n;
}

/**
 * Enqueue several objects on the ring (multi-producers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * producer index atomically.
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
static __rte_always_inline unsigned int
rte_ring_mp_enqueue_bulk_elem(struct rte_ring *r, const void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *free_space)
{
	return __rte_ring_do_enqueue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_FIXED, RTE_RING_SYNC_MT, free_space);
}

/**
 * Enqueue several objects on a ring
 *
 * @warning This API is NOT multi-producers safe
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
static __rte_always_inline unsigned int
rte_ring_sp_enqueue_bulk_elem(struct rte_ring *r, const void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *free_space)
{
	return __rte_ring_do_enqueue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_FIXED, RTE_RING_SYNC_ST, free_space);
}

#ifdef ALLOW_EXPERIMENTAL_API
#include <rte_ring_hts.h>
#include <rte_ring_rts.h>
#endif

/**
 * Enqueue several objects on a ring.
 *
 * This function calls the multi-producer or the single-producer
 * version depending on the default behavior that was specified at
 * ring creation time (see flags).
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
static __rte_always_inline unsigned int
rte_ring_enqueue_bulk_elem(struct rte_ring *r, const void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *free_space)
{
	switch (r->prod.sync_type) {
	case RTE_RING_SYNC_MT:
		return rte_ring_mp_enqueue_bulk_elem(r, obj_table, esize, n,
			free_space);
	case RTE_RING_SYNC_ST:
		return rte_ring_sp_enqueue_bulk_elem(r, obj_table, esize, n,
			free_space);
#ifdef ALLOW_EXPERIMENTAL_API
	case RTE_RING_SYNC_MT_RTS:
		return rte_ring_mp_rts_enqueue_bulk_elem(r, obj_table, esize, n,
			free_space);
	case RTE_RING_SYNC_MT_HTS:
		return rte_ring_mp_hts_enqueue_bulk_elem(r, obj_table, esize, n,
			free_space);
#endif
	}

	/* valid ring should never reach this point */
	RTE_ASSERT(0);
	if (free_space != NULL)
		*free_space = 0;
	return 0;
}

/**
 * Enqueue one object on a ring (multi-producers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * producer index atomically.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj
 *   A pointer to the object to be added.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @return
 *   - 0: Success; objects enqueued.
 *   - -ENOBUFS: Not enough room in the ring to enqueue; no object is enqueued.
 */
static __rte_always_inline int
rte_ring_mp_enqueue_elem(struct rte_ring *r, void *obj, unsigned int esize)
{
	return rte_ring_mp_enqueue_bulk_elem(r, obj, esize, 1, NULL) ? 0 :
								-ENOBUFS;
}

/**
 * Enqueue one object on a ring
 *
 * @warning This API is NOT multi-producers safe
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj
 *   A pointer to the object to be added.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @return
 *   - 0: Success; objects enqueued.
 *   - -ENOBUFS: Not enough room in the ring to enqueue; no object is enqueued.
 */
static __rte_always_inline int
rte_ring_sp_enqueue_elem(struct rte_ring *r, void *obj, unsigned int esize)
{
	return rte_ring_sp_enqueue_bulk_elem(r, obj, esize, 1, NULL) ? 0 :
								-ENOBUFS;
}

/**
 * Enqueue one object on a ring.
 *
 * This function calls the multi-producer or the single-producer
 * version, depending on the default behaviour that was specified at
 * ring creation time (see flags).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj
 *   A pointer to the object to be added.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @return
 *   - 0: Success; objects enqueued.
 *   - -ENOBUFS: Not enough room in the ring to enqueue; no object is enqueued.
 */
static __rte_always_inline int
rte_ring_enqueue_elem(struct rte_ring *r, void *obj, unsigned int esize)
{
	return rte_ring_enqueue_bulk_elem(r, obj, esize, 1, NULL) ? 0 :
								-ENOBUFS;
}

/**
 * Dequeue several objects from a ring (multi-consumers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * consumer index atomically.
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
static __rte_always_inline unsigned int
rte_ring_mc_dequeue_bulk_elem(struct rte_ring *r, void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_dequeue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_FIXED, RTE_RING_SYNC_MT, available);
}

/**
 * Dequeue several objects from a ring (NOT multi-consumers safe).
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
 *   The number of objects to dequeue from the ring to the obj_table,
 *   must be strictly positive.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects dequeued, either 0 or n
 */
static __rte_always_inline unsigned int
rte_ring_sc_dequeue_bulk_elem(struct rte_ring *r, void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_dequeue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_FIXED, RTE_RING_SYNC_ST, available);
}

/**
 * Dequeue several objects from a ring.
 *
 * This function calls the multi-consumers or the single-consumer
 * version, depending on the default behaviour that was specified at
 * ring creation time (see flags).
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
static __rte_always_inline unsigned int
rte_ring_dequeue_bulk_elem(struct rte_ring *r, void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *available)
{
	switch (r->cons.sync_type) {
	case RTE_RING_SYNC_MT:
		return rte_ring_mc_dequeue_bulk_elem(r, obj_table, esize, n,
			available);
	case RTE_RING_SYNC_ST:
		return rte_ring_sc_dequeue_bulk_elem(r, obj_table, esize, n,
			available);
#ifdef ALLOW_EXPERIMENTAL_API
	case RTE_RING_SYNC_MT_RTS:
		return rte_ring_mc_rts_dequeue_bulk_elem(r, obj_table, esize,
			n, available);
	case RTE_RING_SYNC_MT_HTS:
		return rte_ring_mc_hts_dequeue_bulk_elem(r, obj_table, esize,
			n, available);
#endif
	}

	/* valid ring should never reach this point */
	RTE_ASSERT(0);
	if (available != NULL)
		*available = 0;
	return 0;
}

/**
 * Dequeue one object from a ring (multi-consumers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * consumer index atomically.
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_p
 *   A pointer to the object that will be filled.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @return
 *   - 0: Success; objects dequeued.
 *   - -ENOENT: Not enough entries in the ring to dequeue; no object is
 *     dequeued.
 */
static __rte_always_inline int
rte_ring_mc_dequeue_elem(struct rte_ring *r, void *obj_p,
				unsigned int esize)
{
	return rte_ring_mc_dequeue_bulk_elem(r, obj_p, esize, 1, NULL)  ? 0 :
								-ENOENT;
}

/**
 * Dequeue one object from a ring (NOT multi-consumers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_p
 *   A pointer to the object that will be filled.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @return
 *   - 0: Success; objects dequeued.
 *   - -ENOENT: Not enough entries in the ring to dequeue, no object is
 *     dequeued.
 */
static __rte_always_inline int
rte_ring_sc_dequeue_elem(struct rte_ring *r, void *obj_p,
				unsigned int esize)
{
	return rte_ring_sc_dequeue_bulk_elem(r, obj_p, esize, 1, NULL) ? 0 :
								-ENOENT;
}

/**
 * Dequeue one object from a ring.
 *
 * This function calls the multi-consumers or the single-consumer
 * version depending on the default behaviour that was specified at
 * ring creation time (see flags).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_p
 *   A pointer to the object that will be filled.
 * @param esize
 *   The size of ring element, in bytes. It must be a multiple of 4.
 *   This must be the same value used while creating the ring. Otherwise
 *   the results are undefined.
 * @return
 *   - 0: Success, objects dequeued.
 *   - -ENOENT: Not enough entries in the ring to dequeue, no object is
 *     dequeued.
 */
static __rte_always_inline int
rte_ring_dequeue_elem(struct rte_ring *r, void *obj_p, unsigned int esize)
{
	return rte_ring_dequeue_bulk_elem(r, obj_p, esize, 1, NULL) ? 0 :
								-ENOENT;
}

/**
 * Enqueue several objects on the ring (multi-producers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * producer index atomically.
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
static __rte_always_inline unsigned int
rte_ring_mp_enqueue_burst_elem(struct rte_ring *r, const void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *free_space)
{
	return __rte_ring_do_enqueue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_VARIABLE, RTE_RING_SYNC_MT, free_space);
}

/**
 * Enqueue several objects on a ring
 *
 * @warning This API is NOT multi-producers safe
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
static __rte_always_inline unsigned int
rte_ring_sp_enqueue_burst_elem(struct rte_ring *r, const void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *free_space)
{
	return __rte_ring_do_enqueue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_VARIABLE, RTE_RING_SYNC_ST, free_space);
}

/**
 * Enqueue several objects on a ring.
 *
 * This function calls the multi-producer or the single-producer
 * version depending on the default behavior that was specified at
 * ring creation time (see flags).
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
static __rte_always_inline unsigned int
rte_ring_enqueue_burst_elem(struct rte_ring *r, const void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *free_space)
{
	switch (r->prod.sync_type) {
	case RTE_RING_SYNC_MT:
		return rte_ring_mp_enqueue_burst_elem(r, obj_table, esize, n,
			free_space);
	case RTE_RING_SYNC_ST:
		return rte_ring_sp_enqueue_burst_elem(r, obj_table, esize, n,
			free_space);
#ifdef ALLOW_EXPERIMENTAL_API
	case RTE_RING_SYNC_MT_RTS:
		return rte_ring_mp_rts_enqueue_burst_elem(r, obj_table, esize,
			n, free_space);
	case RTE_RING_SYNC_MT_HTS:
		return rte_ring_mp_hts_enqueue_burst_elem(r, obj_table, esize,
			n, free_space);
#endif
	}

	/* valid ring should never reach this point */
	RTE_ASSERT(0);
	if (free_space != NULL)
		*free_space = 0;
	return 0;
}

/**
 * Dequeue several objects from a ring (multi-consumers safe). When the request
 * objects are more than the available objects, only dequeue the actual number
 * of objects
 *
 * This function uses a "compare and set" instruction to move the
 * consumer index atomically.
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
static __rte_always_inline unsigned int
rte_ring_mc_dequeue_burst_elem(struct rte_ring *r, void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_dequeue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_VARIABLE, RTE_RING_SYNC_MT, available);
}

/**
 * Dequeue several objects from a ring (NOT multi-consumers safe).When the
 * request objects are more than the available objects, only dequeue the
 * actual number of objects
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
static __rte_always_inline unsigned int
rte_ring_sc_dequeue_burst_elem(struct rte_ring *r, void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *available)
{
	return __rte_ring_do_dequeue_elem(r, obj_table, esize, n,
			RTE_RING_QUEUE_VARIABLE, RTE_RING_SYNC_ST, available);
}

/**
 * Dequeue multiple objects from a ring up to a maximum number.
 *
 * This function calls the multi-consumers or the single-consumer
 * version, depending on the default behaviour that was specified at
 * ring creation time (see flags).
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
 *   - Number of objects dequeued
 */
static __rte_always_inline unsigned int
rte_ring_dequeue_burst_elem(struct rte_ring *r, void *obj_table,
		unsigned int esize, unsigned int n, unsigned int *available)
{
	switch (r->cons.sync_type) {
	case RTE_RING_SYNC_MT:
		return rte_ring_mc_dequeue_burst_elem(r, obj_table, esize, n,
			available);
	case RTE_RING_SYNC_ST:
		return rte_ring_sc_dequeue_burst_elem(r, obj_table, esize, n,
			available);
#ifdef ALLOW_EXPERIMENTAL_API
	case RTE_RING_SYNC_MT_RTS:
		return rte_ring_mc_rts_dequeue_burst_elem(r, obj_table, esize,
			n, available);
	case RTE_RING_SYNC_MT_HTS:
		return rte_ring_mc_hts_dequeue_burst_elem(r, obj_table, esize,
			n, available);
#endif
	}

	/* valid ring should never reach this point */
	RTE_ASSERT(0);
	if (available != NULL)
		*available = 0;
	return 0;
}

#ifdef ALLOW_EXPERIMENTAL_API
#include <rte_ring_peek.h>
#include <rte_ring_peek_zc.h>
#endif

#include <rte_ring.h>

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RING_ELEM_H_ */
