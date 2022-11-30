/* SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 2010-2020 Intel Corporation
 * Copyright (c) 2007-2009 Kip Macy kmacy@freebsd.org
 * All rights reserved.
 * Derived from FreeBSD's bufring.h
 * Used as BSD-3 Licensed with permission from Kip Macy.
 */

#ifndef _RTE_RING_H_
#define _RTE_RING_H_

/**
 * @file
 * RTE Ring
 *
 * The Ring Manager is a fixed-size queue, implemented as a table of
 * pointers. Head and tail pointers are modified atomically, allowing
 * concurrent access to it. It has the following features:
 *
 * - FIFO (First In First Out)
 * - Maximum size is fixed; the pointers are stored in a table.
 * - Lockless implementation.
 * - Multi- or single-consumer dequeue.
 * - Multi- or single-producer enqueue.
 * - Bulk dequeue.
 * - Bulk enqueue.
 * - Ability to select different sync modes for producer/consumer.
 * - Dequeue start/finish (depending on consumer sync modes).
 * - Enqueue start/finish (depending on producer sync mode).
 *
 * Note: the ring implementation is not preemptible. Refer to Programmer's
 * guide/Environment Abstraction Layer/Multiple pthread/Known Issues/rte_ring
 * for more information.
 *
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_ring_core.h>
#include <rte_ring_elem.h>

/**
 * Calculate the memory size needed for a ring
 *
 * This function returns the number of bytes needed for a ring, given
 * the number of elements in it. This value is the sum of the size of
 * the structure rte_ring and the size of the memory needed by the
 * objects pointers. The value is aligned to a cache line size.
 *
 * @param count
 *   The number of elements in the ring (must be a power of 2).
 * @return
 *   - The memory size needed for the ring on success.
 *   - -EINVAL if count is not a power of 2.
 */
ssize_t rte_ring_get_memsize(unsigned int count);

/**
 * Initialize a ring structure.
 *
 * Initialize a ring structure in memory pointed by "r". The size of the
 * memory area must be large enough to store the ring structure and the
 * object table. It is advised to use rte_ring_get_memsize() to get the
 * appropriate size.
 *
 * The ring size is set to *count*, which must be a power of two. Water
 * marking is disabled by default. The real usable ring size is
 * *count-1* instead of *count* to differentiate a free ring from an
 * empty ring.
 *
 * The ring is not added in RTE_TAILQ_RING global list. Indeed, the
 * memory given by the caller may not be shareable among dpdk
 * processes.
 *
 * @param r
 *   The pointer to the ring structure followed by the objects table.
 * @param name
 *   The name of the ring.
 * @param count
 *   The number of elements in the ring (must be a power of 2).
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
 *   0 on success, or a negative value on error.
 */
int rte_ring_init(struct rte_ring *r, const char *name, unsigned int count,
	unsigned int flags);

/**
 * Create a new ring named *name* in memory.
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
 * @param count
 *   The size of the ring (must be a power of 2).
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
 *    - EINVAL - count provided is not a power of 2
 *    - ENOSPC - the maximum number of memzones has already been allocated
 *    - EEXIST - a memzone with the same name already exists
 *    - ENOMEM - no appropriate memory area found in which to create memzone
 */
struct rte_ring *rte_ring_create(const char *name, unsigned int count,
				 int socket_id, unsigned int flags);

/**
 * De-allocate all memory used by the ring.
 *
 * @param r
 *   Ring to free
 */
void rte_ring_free(struct rte_ring *r);

/**
 * Dump the status of the ring to a file.
 *
 * @param f
 *   A pointer to a file for output
 * @param r
 *   A pointer to the ring structure.
 */
void rte_ring_dump(FILE *f, const struct rte_ring *r);

/**
 * Enqueue several objects on the ring (multi-producers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * producer index atomically.
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
static __rte_always_inline unsigned int
rte_ring_mp_enqueue_bulk(struct rte_ring *r, void * const *obj_table,
			 unsigned int n, unsigned int *free_space)
{
	return rte_ring_mp_enqueue_bulk_elem(r, obj_table, sizeof(void *),
			n, free_space);
}

/**
 * Enqueue several objects on a ring (NOT multi-producers safe).
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
static __rte_always_inline unsigned int
rte_ring_sp_enqueue_bulk(struct rte_ring *r, void * const *obj_table,
			 unsigned int n, unsigned int *free_space)
{
	return rte_ring_sp_enqueue_bulk_elem(r, obj_table, sizeof(void *),
			n, free_space);
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
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   The number of objects enqueued, either 0 or n
 */
static __rte_always_inline unsigned int
rte_ring_enqueue_bulk(struct rte_ring *r, void * const *obj_table,
		      unsigned int n, unsigned int *free_space)
{
	return rte_ring_enqueue_bulk_elem(r, obj_table, sizeof(void *),
			n, free_space);
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
 * @return
 *   - 0: Success; objects enqueued.
 *   - -ENOBUFS: Not enough room in the ring to enqueue; no object is enqueued.
 */
static __rte_always_inline int
rte_ring_mp_enqueue(struct rte_ring *r, void *obj)
{
	return rte_ring_mp_enqueue_elem(r, &obj, sizeof(void *));
}

/**
 * Enqueue one object on a ring (NOT multi-producers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj
 *   A pointer to the object to be added.
 * @return
 *   - 0: Success; objects enqueued.
 *   - -ENOBUFS: Not enough room in the ring to enqueue; no object is enqueued.
 */
static __rte_always_inline int
rte_ring_sp_enqueue(struct rte_ring *r, void *obj)
{
	return rte_ring_sp_enqueue_elem(r, &obj, sizeof(void *));
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
 * @return
 *   - 0: Success; objects enqueued.
 *   - -ENOBUFS: Not enough room in the ring to enqueue; no object is enqueued.
 */
static __rte_always_inline int
rte_ring_enqueue(struct rte_ring *r, void *obj)
{
	return rte_ring_enqueue_elem(r, &obj, sizeof(void *));
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
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects dequeued, either 0 or n
 */
static __rte_always_inline unsigned int
rte_ring_mc_dequeue_bulk(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_mc_dequeue_bulk_elem(r, obj_table, sizeof(void *),
			n, available);
}

/**
 * Dequeue several objects from a ring (NOT multi-consumers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_table
 *   A pointer to a table of void * pointers (objects) that will be filled.
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
rte_ring_sc_dequeue_bulk(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_sc_dequeue_bulk_elem(r, obj_table, sizeof(void *),
			n, available);
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
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   The number of objects dequeued, either 0 or n
 */
static __rte_always_inline unsigned int
rte_ring_dequeue_bulk(struct rte_ring *r, void **obj_table, unsigned int n,
		unsigned int *available)
{
	return rte_ring_dequeue_bulk_elem(r, obj_table, sizeof(void *),
			n, available);
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
 *   A pointer to a void * pointer (object) that will be filled.
 * @return
 *   - 0: Success; objects dequeued.
 *   - -ENOENT: Not enough entries in the ring to dequeue; no object is
 *     dequeued.
 */
static __rte_always_inline int
rte_ring_mc_dequeue(struct rte_ring *r, void **obj_p)
{
	return rte_ring_mc_dequeue_elem(r, obj_p, sizeof(void *));
}

/**
 * Dequeue one object from a ring (NOT multi-consumers safe).
 *
 * @param r
 *   A pointer to the ring structure.
 * @param obj_p
 *   A pointer to a void * pointer (object) that will be filled.
 * @return
 *   - 0: Success; objects dequeued.
 *   - -ENOENT: Not enough entries in the ring to dequeue, no object is
 *     dequeued.
 */
static __rte_always_inline int
rte_ring_sc_dequeue(struct rte_ring *r, void **obj_p)
{
	return rte_ring_sc_dequeue_elem(r, obj_p, sizeof(void *));
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
 *   A pointer to a void * pointer (object) that will be filled.
 * @return
 *   - 0: Success, objects dequeued.
 *   - -ENOENT: Not enough entries in the ring to dequeue, no object is
 *     dequeued.
 */
static __rte_always_inline int
rte_ring_dequeue(struct rte_ring *r, void **obj_p)
{
	return rte_ring_dequeue_elem(r, obj_p, sizeof(void *));
}

/**
 * Flush a ring.
 *
 * This function flush all the elements in a ring
 *
 * @warning
 * Make sure the ring is not in use while calling this function.
 *
 * @param r
 *   A pointer to the ring structure.
 */
void
rte_ring_reset(struct rte_ring *r);

/**
 * Return the number of entries in a ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   The number of entries in the ring.
 */
static inline unsigned int
rte_ring_count(const struct rte_ring *r)
{
	uint32_t prod_tail = r->prod.tail;
	uint32_t cons_tail = r->cons.tail;
	uint32_t count = (prod_tail - cons_tail) & r->mask;
	return (count > r->capacity) ? r->capacity : count;
}

/**
 * Return the number of free entries in a ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   The number of free entries in the ring.
 */
static inline unsigned int
rte_ring_free_count(const struct rte_ring *r)
{
	return r->capacity - rte_ring_count(r);
}

/**
 * Test if a ring is full.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   - 1: The ring is full.
 *   - 0: The ring is not full.
 */
static inline int
rte_ring_full(const struct rte_ring *r)
{
	return rte_ring_free_count(r) == 0;
}

/**
 * Test if a ring is empty.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   - 1: The ring is empty.
 *   - 0: The ring is not empty.
 */
static inline int
rte_ring_empty(const struct rte_ring *r)
{
	uint32_t prod_tail = r->prod.tail;
	uint32_t cons_tail = r->cons.tail;
	return cons_tail == prod_tail;
}

/**
 * Return the size of the ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   The size of the data store used by the ring.
 *   NOTE: this is not the same as the usable space in the ring. To query that
 *   use ``rte_ring_get_capacity()``.
 */
static inline unsigned int
rte_ring_get_size(const struct rte_ring *r)
{
	return r->size;
}

/**
 * Return the number of elements which can be stored in the ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   The usable size of the ring.
 */
static inline unsigned int
rte_ring_get_capacity(const struct rte_ring *r)
{
	return r->capacity;
}

/**
 * Return sync type used by producer in the ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   Producer sync type value.
 */
static inline enum rte_ring_sync_type
rte_ring_get_prod_sync_type(const struct rte_ring *r)
{
	return r->prod.sync_type;
}

/**
 * Check is the ring for single producer.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   true if ring is SP, zero otherwise.
 */
static inline int
rte_ring_is_prod_single(const struct rte_ring *r)
{
	return (rte_ring_get_prod_sync_type(r) == RTE_RING_SYNC_ST);
}

/**
 * Return sync type used by consumer in the ring.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   Consumer sync type value.
 */
static inline enum rte_ring_sync_type
rte_ring_get_cons_sync_type(const struct rte_ring *r)
{
	return r->cons.sync_type;
}

/**
 * Check is the ring for single consumer.
 *
 * @param r
 *   A pointer to the ring structure.
 * @return
 *   true if ring is SC, zero otherwise.
 */
static inline int
rte_ring_is_cons_single(const struct rte_ring *r)
{
	return (rte_ring_get_cons_sync_type(r) == RTE_RING_SYNC_ST);
}

/**
 * Dump the status of all rings on the console
 *
 * @param f
 *   A pointer to a file for output
 */
void rte_ring_list_dump(FILE *f);

/**
 * Search a ring from its name
 *
 * @param name
 *   The name of the ring.
 * @return
 *   The pointer to the ring matching the name, or NULL if not found,
 *   with rte_errno set appropriately. Possible rte_errno values include:
 *    - ENOENT - required entry not available to return.
 */
struct rte_ring *rte_ring_lookup(const char *name);

/**
 * Enqueue several objects on the ring (multi-producers safe).
 *
 * This function uses a "compare and set" instruction to move the
 * producer index atomically.
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
static __rte_always_inline unsigned int
rte_ring_mp_enqueue_burst(struct rte_ring *r, void * const *obj_table,
			 unsigned int n, unsigned int *free_space)
{
	return rte_ring_mp_enqueue_burst_elem(r, obj_table, sizeof(void *),
			n, free_space);
}

/**
 * Enqueue several objects on a ring (NOT multi-producers safe).
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
static __rte_always_inline unsigned int
rte_ring_sp_enqueue_burst(struct rte_ring *r, void * const *obj_table,
			 unsigned int n, unsigned int *free_space)
{
	return rte_ring_sp_enqueue_burst_elem(r, obj_table, sizeof(void *),
			n, free_space);
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
 *   A pointer to a table of void * pointers (objects).
 * @param n
 *   The number of objects to add in the ring from the obj_table.
 * @param free_space
 *   if non-NULL, returns the amount of space in the ring after the
 *   enqueue operation has finished.
 * @return
 *   - n: Actual number of objects enqueued.
 */
static __rte_always_inline unsigned int
rte_ring_enqueue_burst(struct rte_ring *r, void * const *obj_table,
		      unsigned int n, unsigned int *free_space)
{
	return rte_ring_enqueue_burst_elem(r, obj_table, sizeof(void *),
			n, free_space);
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
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   - n: Actual number of objects dequeued, 0 if ring is empty
 */
static __rte_always_inline unsigned int
rte_ring_mc_dequeue_burst(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_mc_dequeue_burst_elem(r, obj_table, sizeof(void *),
			n, available);
}

/**
 * Dequeue several objects from a ring (NOT multi-consumers safe).When the
 * request objects are more than the available objects, only dequeue the
 * actual number of objects
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
static __rte_always_inline unsigned int
rte_ring_sc_dequeue_burst(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_sc_dequeue_burst_elem(r, obj_table, sizeof(void *),
			n, available);
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
 *   A pointer to a table of void * pointers (objects) that will be filled.
 * @param n
 *   The number of objects to dequeue from the ring to the obj_table.
 * @param available
 *   If non-NULL, returns the number of remaining ring entries after the
 *   dequeue has finished.
 * @return
 *   - Number of objects dequeued
 */
static __rte_always_inline unsigned int
rte_ring_dequeue_burst(struct rte_ring *r, void **obj_table,
		unsigned int n, unsigned int *available)
{
	return rte_ring_dequeue_burst_elem(r, obj_table, sizeof(void *),
			n, available);
}

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RING_H_ */
