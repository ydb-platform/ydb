/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017-2018 Intel Corporation
 */

#ifndef RTE_FBARRAY_H
#define RTE_FBARRAY_H

/**
 * @file
 *
 * File-backed shared indexed array for DPDK.
 *
 * Basic workflow is expected to be the following:
 *  1) Allocate array either using ``rte_fbarray_init()`` or
 *     ``rte_fbarray_attach()`` (depending on whether it's shared between
 *     multiple DPDK processes)
 *  2) find free spots using ``rte_fbarray_find_next_free()``
 *  3) get pointer to data in the free spot using ``rte_fbarray_get()``, and
 *     copy data into the pointer (element size is fixed)
 *  4) mark entry as used using ``rte_fbarray_set_used()``
 *
 * Calls to ``rte_fbarray_init()`` and ``rte_fbarray_destroy()`` will have
 * consequences for all processes, while calls to ``rte_fbarray_attach()`` and
 * ``rte_fbarray_detach()`` will only have consequences within a single process.
 * Therefore, it is safe to call ``rte_fbarray_attach()`` or
 * ``rte_fbarray_detach()`` while another process is using ``rte_fbarray``,
 * provided no other thread within the same process will try to use
 * ``rte_fbarray`` before attaching or after detaching. It is not safe to call
 * ``rte_fbarray_init()`` or ``rte_fbarray_destroy()`` while another thread or
 * another process is using ``rte_fbarray``.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

#include <rte_compat.h>
#include <rte_rwlock.h>

#define RTE_FBARRAY_NAME_LEN 64

struct rte_fbarray {
	char name[RTE_FBARRAY_NAME_LEN]; /**< name associated with an array */
	unsigned int count;              /**< number of entries stored */
	unsigned int len;                /**< current length of the array */
	unsigned int elt_sz;             /**< size of each element */
	void *data;                      /**< data pointer */
	rte_rwlock_t rwlock;             /**< multiprocess lock */
};

/**
 * Set up ``rte_fbarray`` structure and allocate underlying resources.
 *
 * Call this function to correctly set up ``rte_fbarray`` and allocate
 * underlying files that will be backing the data in the current process. Note
 * that in order to use and share ``rte_fbarray`` between multiple processes,
 * data pointed to by ``arr`` pointer must itself be allocated in shared memory.
 *
 * @param arr
 *   Valid pointer to allocated ``rte_fbarray`` structure.
 *
 * @param name
 *   Unique name to be assigned to this array.
 *
 * @param len
 *   Number of elements initially available in the array.
 *
 * @param elt_sz
 *   Size of each element.
 *
 * @return
 *  - 0 on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_init(struct rte_fbarray *arr, const char *name, unsigned int len,
		unsigned int elt_sz);


/**
 * Attach to a file backing an already allocated and correctly set up
 * ``rte_fbarray`` structure.
 *
 * Call this function to attach to file that will be backing the data in the
 * current process. The structure must have been previously correctly set up
 * with a call to ``rte_fbarray_init()``. Calls to ``rte_fbarray_attach()`` are
 * usually meant to be performed in a multiprocessing scenario, with data
 * pointed to by ``arr`` pointer allocated in shared memory.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up rte_fbarray structure.
 *
 * @return
 *  - 0 on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_attach(struct rte_fbarray *arr);


/**
 * Deallocate resources for an already allocated and correctly set up
 * ``rte_fbarray`` structure, and remove the underlying file.
 *
 * Call this function to deallocate all resources associated with an
 * ``rte_fbarray`` structure within the current process. This will also
 * zero-fill data pointed to by ``arr`` pointer and remove the underlying file
 * backing the data, so it is expected that by the time this function is called,
 * all other processes have detached from this ``rte_fbarray``.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @return
 *  - 0 on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_destroy(struct rte_fbarray *arr);


/**
 * Deallocate resources for an already allocated and correctly set up
 * ``rte_fbarray`` structure.
 *
 * Call this function to deallocate all resources associated with an
 * ``rte_fbarray`` structure within current process.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @return
 *  - 0 on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_detach(struct rte_fbarray *arr);


/**
 * Get pointer to element residing at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param idx
 *   Index of an element to get a pointer to.
 *
 * @return
 *  - non-NULL pointer on success.
 *  - NULL on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
void *
rte_fbarray_get(const struct rte_fbarray *arr, unsigned int idx);


/**
 * Find index of a specified element within the array.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param elt
 *   Pointer to element to find index to.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_idx(const struct rte_fbarray *arr, const void *elt);


/**
 * Mark specified element as used.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param idx
 *   Element index to mark as used.
 *
 * @return
 *  - 0 on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_set_used(struct rte_fbarray *arr, unsigned int idx);


/**
 * Mark specified element as free.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param idx
 *   Element index to mark as free.
 *
 * @return
 *  - 0 on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_set_free(struct rte_fbarray *arr, unsigned int idx);


/**
 * Check whether element at specified index is marked as used.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param idx
 *   Element index to check as used.
 *
 * @return
 *  - 1 if element is used.
 *  - 0 if element is unused.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_is_used(struct rte_fbarray *arr, unsigned int idx);


/**
 * Find index of next free element, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_next_free(struct rte_fbarray *arr, unsigned int start);


/**
 * Find index of next used element, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_next_used(struct rte_fbarray *arr, unsigned int start);


/**
 * Find index of next chunk of ``n`` free elements, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @param n
 *   Number of free elements to look for.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_next_n_free(struct rte_fbarray *arr, unsigned int start,
		unsigned int n);


/**
 * Find index of next chunk of ``n`` used elements, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @param n
 *   Number of used elements to look for.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_next_n_used(struct rte_fbarray *arr, unsigned int start,
		unsigned int n);


/**
 * Find how many more free entries there are, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_contig_free(struct rte_fbarray *arr,
		unsigned int start);


/**
 * Find how many more used entries there are, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_contig_used(struct rte_fbarray *arr, unsigned int start);

/**
 * Find index of previous free element, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_prev_free(struct rte_fbarray *arr, unsigned int start);


/**
 * Find index of previous used element, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_prev_used(struct rte_fbarray *arr, unsigned int start);


/**
 * Find lowest start index of chunk of ``n`` free elements, down from specified
 * index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @param n
 *   Number of free elements to look for.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_prev_n_free(struct rte_fbarray *arr, unsigned int start,
		unsigned int n);


/**
 * Find lowest start index of chunk of ``n`` used elements, down from specified
 * index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @param n
 *   Number of used elements to look for.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_prev_n_used(struct rte_fbarray *arr, unsigned int start,
		unsigned int n);


/**
 * Find how many more free entries there are before specified index (like
 * ``rte_fbarray_find_contig_free`` but going in reverse).
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_rev_contig_free(struct rte_fbarray *arr,
		unsigned int start);


/**
 * Find how many more used entries there are before specified index (like
 * ``rte_fbarray_find_contig_used`` but going in reverse).
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_rev_contig_used(struct rte_fbarray *arr, unsigned int start);


/**
 * Find index of biggest chunk of free elements, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_biggest_free(struct rte_fbarray *arr, unsigned int start);


/**
 * Find index of biggest chunk of used elements, starting at specified index.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_biggest_used(struct rte_fbarray *arr, unsigned int start);


/**
 * Find index of biggest chunk of free elements before a specified index (like
 * ``rte_fbarray_find_biggest_free``, but going in reverse).
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_rev_biggest_free(struct rte_fbarray *arr, unsigned int start);


/**
 * Find index of biggest chunk of used elements before a specified index (like
 * ``rte_fbarray_find_biggest_used``, but going in reverse).
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param start
 *   Element index to start search from.
 *
 * @return
 *  - non-negative integer on success.
 *  - -1 on failure, with ``rte_errno`` indicating reason for failure.
 */
__rte_experimental
int
rte_fbarray_find_rev_biggest_used(struct rte_fbarray *arr, unsigned int start);


/**
 * Dump ``rte_fbarray`` metadata.
 *
 * @param arr
 *   Valid pointer to allocated and correctly set up ``rte_fbarray`` structure.
 *
 * @param f
 *   File object to dump information into.
 */
__rte_experimental
void
rte_fbarray_dump_metadata(struct rte_fbarray *arr, FILE *f);

#ifdef __cplusplus
}
#endif

#endif /* RTE_FBARRAY_H */
