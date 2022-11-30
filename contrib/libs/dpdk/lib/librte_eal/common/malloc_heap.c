#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/queue.h>

#include <rte_memory.h>
#include <rte_errno.h>
#include <rte_eal.h>
#include <rte_eal_memconfig.h>
#include <rte_launch.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_common.h>
#include <rte_string_fns.h>
#include <rte_spinlock.h>
#include <rte_memcpy.h>
#include <rte_memzone.h>
#include <rte_atomic.h>
#include <rte_fbarray.h>

#include "eal_internal_cfg.h"
#include "eal_memalloc.h"
#include "eal_memcfg.h"
#include "eal_private.h"
#include "malloc_elem.h"
#include "malloc_heap.h"
#include "malloc_mp.h"

/* start external socket ID's at a very high number */
#define CONST_MAX(a, b) (a > b ? a : b) /* RTE_MAX is not a constant */
#define EXTERNAL_HEAP_MIN_SOCKET_ID (CONST_MAX((1 << 8), RTE_MAX_NUMA_NODES))

static unsigned
check_hugepage_sz(unsigned flags, uint64_t hugepage_sz)
{
	unsigned check_flag = 0;

	if (!(flags & ~RTE_MEMZONE_SIZE_HINT_ONLY))
		return 1;

	switch (hugepage_sz) {
	case RTE_PGSIZE_256K:
		check_flag = RTE_MEMZONE_256KB;
		break;
	case RTE_PGSIZE_2M:
		check_flag = RTE_MEMZONE_2MB;
		break;
	case RTE_PGSIZE_16M:
		check_flag = RTE_MEMZONE_16MB;
		break;
	case RTE_PGSIZE_256M:
		check_flag = RTE_MEMZONE_256MB;
		break;
	case RTE_PGSIZE_512M:
		check_flag = RTE_MEMZONE_512MB;
		break;
	case RTE_PGSIZE_1G:
		check_flag = RTE_MEMZONE_1GB;
		break;
	case RTE_PGSIZE_4G:
		check_flag = RTE_MEMZONE_4GB;
		break;
	case RTE_PGSIZE_16G:
		check_flag = RTE_MEMZONE_16GB;
	}

	return check_flag & flags;
}

int
malloc_socket_to_heap_id(unsigned int socket_id)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	int i;

	for (i = 0; i < RTE_MAX_HEAPS; i++) {
		struct malloc_heap *heap = &mcfg->malloc_heaps[i];

		if (heap->socket_id == socket_id)
			return i;
	}
	return -1;
}

/*
 * Expand the heap with a memory area.
 */
static struct malloc_elem *
malloc_heap_add_memory(struct malloc_heap *heap, struct rte_memseg_list *msl,
		void *start, size_t len)
{
	struct malloc_elem *elem = start;

	malloc_elem_init(elem, heap, msl, len, elem, len);

	malloc_elem_insert(elem);

	elem = malloc_elem_join_adjacent_free(elem);

	malloc_elem_free_list_insert(elem);

	return elem;
}

static int
malloc_add_seg(const struct rte_memseg_list *msl,
		const struct rte_memseg *ms, size_t len, void *arg __rte_unused)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct rte_memseg_list *found_msl;
	struct malloc_heap *heap;
	int msl_idx, heap_idx;

	if (msl->external)
		return 0;

	heap_idx = malloc_socket_to_heap_id(msl->socket_id);
	if (heap_idx < 0) {
		RTE_LOG(ERR, EAL, "Memseg list has invalid socket id\n");
		return -1;
	}
	heap = &mcfg->malloc_heaps[heap_idx];

	/* msl is const, so find it */
	msl_idx = msl - mcfg->memsegs;

	if (msl_idx < 0 || msl_idx >= RTE_MAX_MEMSEG_LISTS)
		return -1;

	found_msl = &mcfg->memsegs[msl_idx];

	malloc_heap_add_memory(heap, found_msl, ms->addr, len);

	heap->total_size += len;

	RTE_LOG(DEBUG, EAL, "Added %zuM to heap on socket %i\n", len >> 20,
			msl->socket_id);
	return 0;
}

/*
 * Iterates through the freelist for a heap to find a free element
 * which can store data of the required size and with the requested alignment.
 * If size is 0, find the biggest available elem.
 * Returns null on failure, or pointer to element on success.
 */
static struct malloc_elem *
find_suitable_element(struct malloc_heap *heap, size_t size,
		unsigned int flags, size_t align, size_t bound, bool contig)
{
	size_t idx;
	struct malloc_elem *elem, *alt_elem = NULL;

	for (idx = malloc_elem_free_list_index(size);
			idx < RTE_HEAP_NUM_FREELISTS; idx++) {
		for (elem = LIST_FIRST(&heap->free_head[idx]);
				!!elem; elem = LIST_NEXT(elem, free_list)) {
			if (malloc_elem_can_hold(elem, size, align, bound,
					contig)) {
				if (check_hugepage_sz(flags,
						elem->msl->page_sz))
					return elem;
				if (alt_elem == NULL)
					alt_elem = elem;
			}
		}
	}

	if ((alt_elem != NULL) && (flags & RTE_MEMZONE_SIZE_HINT_ONLY))
		return alt_elem;

	return NULL;
}

/*
 * Iterates through the freelist for a heap to find a free element with the
 * biggest size and requested alignment. Will also set size to whatever element
 * size that was found.
 * Returns null on failure, or pointer to element on success.
 */
static struct malloc_elem *
find_biggest_element(struct malloc_heap *heap, size_t *size,
		unsigned int flags, size_t align, bool contig)
{
	struct malloc_elem *elem, *max_elem = NULL;
	size_t idx, max_size = 0;

	for (idx = 0; idx < RTE_HEAP_NUM_FREELISTS; idx++) {
		for (elem = LIST_FIRST(&heap->free_head[idx]);
				!!elem; elem = LIST_NEXT(elem, free_list)) {
			size_t cur_size;
			if ((flags & RTE_MEMZONE_SIZE_HINT_ONLY) == 0 &&
					!check_hugepage_sz(flags,
						elem->msl->page_sz))
				continue;
			if (contig) {
				cur_size =
					malloc_elem_find_max_iova_contig(elem,
							align);
			} else {
				void *data_start = RTE_PTR_ADD(elem,
						MALLOC_ELEM_HEADER_LEN);
				void *data_end = RTE_PTR_ADD(elem, elem->size -
						MALLOC_ELEM_TRAILER_LEN);
				void *aligned = RTE_PTR_ALIGN_CEIL(data_start,
						align);
				/* check if aligned data start is beyond end */
				if (aligned >= data_end)
					continue;
				cur_size = RTE_PTR_DIFF(data_end, aligned);
			}
			if (cur_size > max_size) {
				max_size = cur_size;
				max_elem = elem;
			}
		}
	}

	*size = max_size;
	return max_elem;
}

/*
 * Main function to allocate a block of memory from the heap.
 * It locks the free list, scans it, and adds a new memseg if the
 * scan fails. Once the new memseg is added, it re-scans and should return
 * the new element after releasing the lock.
 */
static void *
heap_alloc(struct malloc_heap *heap, const char *type __rte_unused, size_t size,
		unsigned int flags, size_t align, size_t bound, bool contig)
{
	struct malloc_elem *elem;

	size = RTE_CACHE_LINE_ROUNDUP(size);
	align = RTE_CACHE_LINE_ROUNDUP(align);

	/* roundup might cause an overflow */
	if (size == 0)
		return NULL;
	elem = find_suitable_element(heap, size, flags, align, bound, contig);
	if (elem != NULL) {
		elem = malloc_elem_alloc(elem, size, align, bound, contig);

		/* increase heap's count of allocated elements */
		heap->alloc_count++;
	}

	return elem == NULL ? NULL : (void *)(&elem[1]);
}

static void *
heap_alloc_biggest(struct malloc_heap *heap, const char *type __rte_unused,
		unsigned int flags, size_t align, bool contig)
{
	struct malloc_elem *elem;
	size_t size;

	align = RTE_CACHE_LINE_ROUNDUP(align);

	elem = find_biggest_element(heap, &size, flags, align, contig);
	if (elem != NULL) {
		elem = malloc_elem_alloc(elem, size, align, 0, contig);

		/* increase heap's count of allocated elements */
		heap->alloc_count++;
	}

	return elem == NULL ? NULL : (void *)(&elem[1]);
}

/* this function is exposed in malloc_mp.h */
void
rollback_expand_heap(struct rte_memseg **ms, int n_segs,
		struct malloc_elem *elem, void *map_addr, size_t map_len)
{
	if (elem != NULL) {
		malloc_elem_free_list_remove(elem);
		malloc_elem_hide_region(elem, map_addr, map_len);
	}

	eal_memalloc_free_seg_bulk(ms, n_segs);
}

/* this function is exposed in malloc_mp.h */
struct malloc_elem *
alloc_pages_on_heap(struct malloc_heap *heap, uint64_t pg_sz, size_t elt_size,
		int socket, unsigned int flags, size_t align, size_t bound,
		bool contig, struct rte_memseg **ms, int n_segs)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct rte_memseg_list *msl;
	struct malloc_elem *elem = NULL;
	size_t alloc_sz;
	int allocd_pages;
	void *ret, *map_addr;

	alloc_sz = (size_t)pg_sz * n_segs;

	/* first, check if we're allowed to allocate this memory */
	if (eal_memalloc_mem_alloc_validate(socket,
			heap->total_size + alloc_sz) < 0) {
		RTE_LOG(DEBUG, EAL, "User has disallowed allocation\n");
		return NULL;
	}

	allocd_pages = eal_memalloc_alloc_seg_bulk(ms, n_segs, pg_sz,
			socket, true);

	/* make sure we've allocated our pages... */
	if (allocd_pages < 0)
		return NULL;

	map_addr = ms[0]->addr;
	msl = rte_mem_virt2memseg_list(map_addr);

	/* check if we wanted contiguous memory but didn't get it */
	if (contig && !eal_memalloc_is_contig(msl, map_addr, alloc_sz)) {
		RTE_LOG(DEBUG, EAL, "%s(): couldn't allocate physically contiguous space\n",
				__func__);
		goto fail;
	}

	/*
	 * Once we have all the memseg lists configured, if there is a dma mask
	 * set, check iova addresses are not out of range. Otherwise the device
	 * setting the dma mask could have problems with the mapped memory.
	 *
	 * There are two situations when this can happen:
	 *	1) memory initialization
	 *	2) dynamic memory allocation
	 *
	 * For 1), an error when checking dma mask implies app can not be
	 * executed. For 2) implies the new memory can not be added.
	 */
	if (mcfg->dma_maskbits &&
	    rte_mem_check_dma_mask_thread_unsafe(mcfg->dma_maskbits)) {
		/*
		 * Currently this can only happen if IOMMU is enabled
		 * and the address width supported by the IOMMU hw is
		 * not enough for using the memory mapped IOVAs.
		 *
		 * If IOVA is VA, advice to try with '--iova-mode pa'
		 * which could solve some situations when IOVA VA is not
		 * really needed.
		 */
		RTE_LOG(ERR, EAL,
			"%s(): couldn't allocate memory due to IOVA exceeding limits of current DMA mask\n",
			__func__);

		/*
		 * If IOVA is VA and it is possible to run with IOVA PA,
		 * because user is root, give and advice for solving the
		 * problem.
		 */
		if ((rte_eal_iova_mode() == RTE_IOVA_VA) &&
		     rte_eal_using_phys_addrs())
			RTE_LOG(ERR, EAL,
				"%s(): Please try initializing EAL with --iova-mode=pa parameter\n",
				__func__);
		goto fail;
	}

	/* add newly minted memsegs to malloc heap */
	elem = malloc_heap_add_memory(heap, msl, map_addr, alloc_sz);

	/* try once more, as now we have allocated new memory */
	ret = find_suitable_element(heap, elt_size, flags, align, bound,
			contig);

	if (ret == NULL)
		goto fail;

	return elem;

fail:
	rollback_expand_heap(ms, n_segs, elem, map_addr, alloc_sz);
	return NULL;
}

static int
try_expand_heap_primary(struct malloc_heap *heap, uint64_t pg_sz,
		size_t elt_size, int socket, unsigned int flags, size_t align,
		size_t bound, bool contig)
{
	struct malloc_elem *elem;
	struct rte_memseg **ms;
	void *map_addr;
	size_t alloc_sz;
	int n_segs;
	bool callback_triggered = false;

	alloc_sz = RTE_ALIGN_CEIL(align + elt_size +
			MALLOC_ELEM_TRAILER_LEN, pg_sz);
	n_segs = alloc_sz / pg_sz;

	/* we can't know in advance how many pages we'll need, so we malloc */
	ms = malloc(sizeof(*ms) * n_segs);
	if (ms == NULL)
		return -1;
	memset(ms, 0, sizeof(*ms) * n_segs);

	elem = alloc_pages_on_heap(heap, pg_sz, elt_size, socket, flags, align,
			bound, contig, ms, n_segs);

	if (elem == NULL)
		goto free_ms;

	map_addr = ms[0]->addr;

	/* notify user about changes in memory map */
	eal_memalloc_mem_event_notify(RTE_MEM_EVENT_ALLOC, map_addr, alloc_sz);

	/* notify other processes that this has happened */
	if (request_sync()) {
		/* we couldn't ensure all processes have mapped memory,
		 * so free it back and notify everyone that it's been
		 * freed back.
		 *
		 * technically, we could've avoided adding memory addresses to
		 * the map, but that would've led to inconsistent behavior
		 * between primary and secondary processes, as those get
		 * callbacks during sync. therefore, force primary process to
		 * do alloc-and-rollback syncs as well.
		 */
		callback_triggered = true;
		goto free_elem;
	}
	heap->total_size += alloc_sz;

	RTE_LOG(DEBUG, EAL, "Heap on socket %d was expanded by %zdMB\n",
		socket, alloc_sz >> 20ULL);

	free(ms);

	return 0;

free_elem:
	if (callback_triggered)
		eal_memalloc_mem_event_notify(RTE_MEM_EVENT_FREE,
				map_addr, alloc_sz);

	rollback_expand_heap(ms, n_segs, elem, map_addr, alloc_sz);

	request_sync();
free_ms:
	free(ms);

	return -1;
}

static int
try_expand_heap_secondary(struct malloc_heap *heap, uint64_t pg_sz,
		size_t elt_size, int socket, unsigned int flags, size_t align,
		size_t bound, bool contig)
{
	struct malloc_mp_req req;
	int req_result;

	memset(&req, 0, sizeof(req));

	req.t = REQ_TYPE_ALLOC;
	req.alloc_req.align = align;
	req.alloc_req.bound = bound;
	req.alloc_req.contig = contig;
	req.alloc_req.flags = flags;
	req.alloc_req.elt_size = elt_size;
	req.alloc_req.page_sz = pg_sz;
	req.alloc_req.socket = socket;
	req.alloc_req.heap = heap; /* it's in shared memory */

	req_result = request_to_primary(&req);

	if (req_result != 0)
		return -1;

	if (req.result != REQ_RESULT_SUCCESS)
		return -1;

	return 0;
}

static int
try_expand_heap(struct malloc_heap *heap, uint64_t pg_sz, size_t elt_size,
		int socket, unsigned int flags, size_t align, size_t bound,
		bool contig)
{
	int ret;

	rte_mcfg_mem_write_lock();

	if (rte_eal_process_type() == RTE_PROC_PRIMARY) {
		ret = try_expand_heap_primary(heap, pg_sz, elt_size, socket,
				flags, align, bound, contig);
	} else {
		ret = try_expand_heap_secondary(heap, pg_sz, elt_size, socket,
				flags, align, bound, contig);
	}

	rte_mcfg_mem_write_unlock();
	return ret;
}

static int
compare_pagesz(const void *a, const void *b)
{
	const struct rte_memseg_list * const*mpa = a;
	const struct rte_memseg_list * const*mpb = b;
	const struct rte_memseg_list *msla = *mpa;
	const struct rte_memseg_list *mslb = *mpb;
	uint64_t pg_sz_a = msla->page_sz;
	uint64_t pg_sz_b = mslb->page_sz;

	if (pg_sz_a < pg_sz_b)
		return -1;
	if (pg_sz_a > pg_sz_b)
		return 1;
	return 0;
}

static int
alloc_more_mem_on_socket(struct malloc_heap *heap, size_t size, int socket,
		unsigned int flags, size_t align, size_t bound, bool contig)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct rte_memseg_list *requested_msls[RTE_MAX_MEMSEG_LISTS];
	struct rte_memseg_list *other_msls[RTE_MAX_MEMSEG_LISTS];
	uint64_t requested_pg_sz[RTE_MAX_MEMSEG_LISTS];
	uint64_t other_pg_sz[RTE_MAX_MEMSEG_LISTS];
	uint64_t prev_pg_sz;
	int i, n_other_msls, n_other_pg_sz, n_requested_msls, n_requested_pg_sz;
	bool size_hint = (flags & RTE_MEMZONE_SIZE_HINT_ONLY) > 0;
	unsigned int size_flags = flags & ~RTE_MEMZONE_SIZE_HINT_ONLY;
	void *ret;

	memset(requested_msls, 0, sizeof(requested_msls));
	memset(other_msls, 0, sizeof(other_msls));
	memset(requested_pg_sz, 0, sizeof(requested_pg_sz));
	memset(other_pg_sz, 0, sizeof(other_pg_sz));

	/*
	 * go through memseg list and take note of all the page sizes available,
	 * and if any of them were specifically requested by the user.
	 */
	n_requested_msls = 0;
	n_other_msls = 0;
	for (i = 0; i < RTE_MAX_MEMSEG_LISTS; i++) {
		struct rte_memseg_list *msl = &mcfg->memsegs[i];

		if (msl->socket_id != socket)
			continue;

		if (msl->base_va == NULL)
			continue;

		/* if pages of specific size were requested */
		if (size_flags != 0 && check_hugepage_sz(size_flags,
				msl->page_sz))
			requested_msls[n_requested_msls++] = msl;
		else if (size_flags == 0 || size_hint)
			other_msls[n_other_msls++] = msl;
	}

	/* sort the lists, smallest first */
	qsort(requested_msls, n_requested_msls, sizeof(requested_msls[0]),
			compare_pagesz);
	qsort(other_msls, n_other_msls, sizeof(other_msls[0]),
			compare_pagesz);

	/* now, extract page sizes we are supposed to try */
	prev_pg_sz = 0;
	n_requested_pg_sz = 0;
	for (i = 0; i < n_requested_msls; i++) {
		uint64_t pg_sz = requested_msls[i]->page_sz;

		if (prev_pg_sz != pg_sz) {
			requested_pg_sz[n_requested_pg_sz++] = pg_sz;
			prev_pg_sz = pg_sz;
		}
	}
	prev_pg_sz = 0;
	n_other_pg_sz = 0;
	for (i = 0; i < n_other_msls; i++) {
		uint64_t pg_sz = other_msls[i]->page_sz;

		if (prev_pg_sz != pg_sz) {
			other_pg_sz[n_other_pg_sz++] = pg_sz;
			prev_pg_sz = pg_sz;
		}
	}

	/* finally, try allocating memory of specified page sizes, starting from
	 * the smallest sizes
	 */
	for (i = 0; i < n_requested_pg_sz; i++) {
		uint64_t pg_sz = requested_pg_sz[i];

		/*
		 * do not pass the size hint here, as user expects other page
		 * sizes first, before resorting to best effort allocation.
		 */
		if (!try_expand_heap(heap, pg_sz, size, socket, size_flags,
				align, bound, contig))
			return 0;
	}
	if (n_other_pg_sz == 0)
		return -1;

	/* now, check if we can reserve anything with size hint */
	ret = find_suitable_element(heap, size, flags, align, bound, contig);
	if (ret != NULL)
		return 0;

	/*
	 * we still couldn't reserve memory, so try expanding heap with other
	 * page sizes, if there are any
	 */
	for (i = 0; i < n_other_pg_sz; i++) {
		uint64_t pg_sz = other_pg_sz[i];

		if (!try_expand_heap(heap, pg_sz, size, socket, flags,
				align, bound, contig))
			return 0;
	}
	return -1;
}

/* this will try lower page sizes first */
static void *
malloc_heap_alloc_on_heap_id(const char *type, size_t size,
		unsigned int heap_id, unsigned int flags, size_t align,
		size_t bound, bool contig)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct malloc_heap *heap = &mcfg->malloc_heaps[heap_id];
	unsigned int size_flags = flags & ~RTE_MEMZONE_SIZE_HINT_ONLY;
	int socket_id;
	void *ret;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	rte_spinlock_lock(&(heap->lock));

	align = align == 0 ? 1 : align;

	/* for legacy mode, try once and with all flags */
	if (internal_conf->legacy_mem) {
		ret = heap_alloc(heap, type, size, flags, align, bound, contig);
		goto alloc_unlock;
	}

	/*
	 * we do not pass the size hint here, because even if allocation fails,
	 * we may still be able to allocate memory from appropriate page sizes,
	 * we just need to request more memory first.
	 */

	socket_id = rte_socket_id_by_idx(heap_id);
	/*
	 * if socket ID is negative, we cannot find a socket ID for this heap -
	 * which means it's an external heap. those can have unexpected page
	 * sizes, so if the user asked to allocate from there - assume user
	 * knows what they're doing, and allow allocating from there with any
	 * page size flags.
	 */
	if (socket_id < 0)
		size_flags |= RTE_MEMZONE_SIZE_HINT_ONLY;

	ret = heap_alloc(heap, type, size, size_flags, align, bound, contig);
	if (ret != NULL)
		goto alloc_unlock;

	/* if socket ID is invalid, this is an external heap */
	if (socket_id < 0)
		goto alloc_unlock;

	if (!alloc_more_mem_on_socket(heap, size, socket_id, flags, align,
			bound, contig)) {
		ret = heap_alloc(heap, type, size, flags, align, bound, contig);

		/* this should have succeeded */
		if (ret == NULL)
			RTE_LOG(ERR, EAL, "Error allocating from heap\n");
	}
alloc_unlock:
	rte_spinlock_unlock(&(heap->lock));
	return ret;
}

void *
malloc_heap_alloc(const char *type, size_t size, int socket_arg,
		unsigned int flags, size_t align, size_t bound, bool contig)
{
	int socket, heap_id, i;
	void *ret;

	/* return NULL if size is 0 or alignment is not power-of-2 */
	if (size == 0 || (align && !rte_is_power_of_2(align)))
		return NULL;

	if (!rte_eal_has_hugepages() && socket_arg < RTE_MAX_NUMA_NODES)
		socket_arg = SOCKET_ID_ANY;

	if (socket_arg == SOCKET_ID_ANY)
		socket = malloc_get_numa_socket();
	else
		socket = socket_arg;

	/* turn socket ID into heap ID */
	heap_id = malloc_socket_to_heap_id(socket);
	/* if heap id is negative, socket ID was invalid */
	if (heap_id < 0)
		return NULL;

	ret = malloc_heap_alloc_on_heap_id(type, size, heap_id, flags, align,
			bound, contig);
	if (ret != NULL || socket_arg != SOCKET_ID_ANY)
		return ret;

	/* try other heaps. we are only iterating through native DPDK sockets,
	 * so external heaps won't be included.
	 */
	for (i = 0; i < (int) rte_socket_count(); i++) {
		if (i == heap_id)
			continue;
		ret = malloc_heap_alloc_on_heap_id(type, size, i, flags, align,
				bound, contig);
		if (ret != NULL)
			return ret;
	}
	return NULL;
}

static void *
heap_alloc_biggest_on_heap_id(const char *type, unsigned int heap_id,
		unsigned int flags, size_t align, bool contig)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct malloc_heap *heap = &mcfg->malloc_heaps[heap_id];
	void *ret;

	rte_spinlock_lock(&(heap->lock));

	align = align == 0 ? 1 : align;

	ret = heap_alloc_biggest(heap, type, flags, align, contig);

	rte_spinlock_unlock(&(heap->lock));

	return ret;
}

void *
malloc_heap_alloc_biggest(const char *type, int socket_arg, unsigned int flags,
		size_t align, bool contig)
{
	int socket, i, cur_socket, heap_id;
	void *ret;

	/* return NULL if align is not power-of-2 */
	if ((align && !rte_is_power_of_2(align)))
		return NULL;

	if (!rte_eal_has_hugepages())
		socket_arg = SOCKET_ID_ANY;

	if (socket_arg == SOCKET_ID_ANY)
		socket = malloc_get_numa_socket();
	else
		socket = socket_arg;

	/* turn socket ID into heap ID */
	heap_id = malloc_socket_to_heap_id(socket);
	/* if heap id is negative, socket ID was invalid */
	if (heap_id < 0)
		return NULL;

	ret = heap_alloc_biggest_on_heap_id(type, heap_id, flags, align,
			contig);
	if (ret != NULL || socket_arg != SOCKET_ID_ANY)
		return ret;

	/* try other heaps */
	for (i = 0; i < (int) rte_socket_count(); i++) {
		cur_socket = rte_socket_id_by_idx(i);
		if (cur_socket == socket)
			continue;
		ret = heap_alloc_biggest_on_heap_id(type, i, flags, align,
				contig);
		if (ret != NULL)
			return ret;
	}
	return NULL;
}

/* this function is exposed in malloc_mp.h */
int
malloc_heap_free_pages(void *aligned_start, size_t aligned_len)
{
	int n_segs, seg_idx, max_seg_idx;
	struct rte_memseg_list *msl;
	size_t page_sz;

	msl = rte_mem_virt2memseg_list(aligned_start);
	if (msl == NULL)
		return -1;

	page_sz = (size_t)msl->page_sz;
	n_segs = aligned_len / page_sz;
	seg_idx = RTE_PTR_DIFF(aligned_start, msl->base_va) / page_sz;
	max_seg_idx = seg_idx + n_segs;

	for (; seg_idx < max_seg_idx; seg_idx++) {
		struct rte_memseg *ms;

		ms = rte_fbarray_get(&msl->memseg_arr, seg_idx);
		eal_memalloc_free_seg(ms);
	}
	return 0;
}

int
malloc_heap_free(struct malloc_elem *elem)
{
	struct malloc_heap *heap;
	void *start, *aligned_start, *end, *aligned_end;
	size_t len, aligned_len, page_sz;
	struct rte_memseg_list *msl;
	unsigned int i, n_segs, before_space, after_space;
	int ret;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (!malloc_elem_cookies_ok(elem) || elem->state != ELEM_BUSY)
		return -1;

	/* elem may be merged with previous element, so keep heap address */
	heap = elem->heap;
	msl = elem->msl;
	page_sz = (size_t)msl->page_sz;

	rte_spinlock_lock(&(heap->lock));

	/* mark element as free */
	elem->state = ELEM_FREE;

	elem = malloc_elem_free(elem);

	/* anything after this is a bonus */
	ret = 0;

	/* ...of which we can't avail if we are in legacy mode, or if this is an
	 * externally allocated segment.
	 */
	if (internal_conf->legacy_mem || (msl->external > 0))
		goto free_unlock;

	/* check if we can free any memory back to the system */
	if (elem->size < page_sz)
		goto free_unlock;

	/* if user requested to match allocations, the sizes must match - if not,
	 * we will defer freeing these hugepages until the entire original allocation
	 * can be freed
	 */
	if (internal_conf->match_allocations && elem->size != elem->orig_size)
		goto free_unlock;

	/* probably, but let's make sure, as we may not be using up full page */
	start = elem;
	len = elem->size;
	aligned_start = RTE_PTR_ALIGN_CEIL(start, page_sz);
	end = RTE_PTR_ADD(elem, len);
	aligned_end = RTE_PTR_ALIGN_FLOOR(end, page_sz);

	aligned_len = RTE_PTR_DIFF(aligned_end, aligned_start);

	/* can't free anything */
	if (aligned_len < page_sz)
		goto free_unlock;

	/* we can free something. however, some of these pages may be marked as
	 * unfreeable, so also check that as well
	 */
	n_segs = aligned_len / page_sz;
	for (i = 0; i < n_segs; i++) {
		const struct rte_memseg *tmp =
				rte_mem_virt2memseg(aligned_start, msl);

		if (tmp->flags & RTE_MEMSEG_FLAG_DO_NOT_FREE) {
			/* this is an unfreeable segment, so move start */
			aligned_start = RTE_PTR_ADD(tmp->addr, tmp->len);
		}
	}

	/* recalculate length and number of segments */
	aligned_len = RTE_PTR_DIFF(aligned_end, aligned_start);
	n_segs = aligned_len / page_sz;

	/* check if we can still free some pages */
	if (n_segs == 0)
		goto free_unlock;

	/* We're not done yet. We also have to check if by freeing space we will
	 * be leaving free elements that are too small to store new elements.
	 * Check if we have enough space in the beginning and at the end, or if
	 * start/end are exactly page aligned.
	 */
	before_space = RTE_PTR_DIFF(aligned_start, elem);
	after_space = RTE_PTR_DIFF(end, aligned_end);
	if (before_space != 0 &&
			before_space < MALLOC_ELEM_OVERHEAD + MIN_DATA_SIZE) {
		/* There is not enough space before start, but we may be able to
		 * move the start forward by one page.
		 */
		if (n_segs == 1)
			goto free_unlock;

		/* move start */
		aligned_start = RTE_PTR_ADD(aligned_start, page_sz);
		aligned_len -= page_sz;
		n_segs--;
	}
	if (after_space != 0 && after_space <
			MALLOC_ELEM_OVERHEAD + MIN_DATA_SIZE) {
		/* There is not enough space after end, but we may be able to
		 * move the end backwards by one page.
		 */
		if (n_segs == 1)
			goto free_unlock;

		/* move end */
		aligned_end = RTE_PTR_SUB(aligned_end, page_sz);
		aligned_len -= page_sz;
		n_segs--;
	}

	/* now we can finally free us some pages */

	rte_mcfg_mem_write_lock();

	/*
	 * we allow secondary processes to clear the heap of this allocated
	 * memory because it is safe to do so, as even if notifications about
	 * unmapped pages don't make it to other processes, heap is shared
	 * across all processes, and will become empty of this memory anyway,
	 * and nothing can allocate it back unless primary process will be able
	 * to deliver allocation message to every single running process.
	 */

	malloc_elem_free_list_remove(elem);

	malloc_elem_hide_region(elem, (void *) aligned_start, aligned_len);

	heap->total_size -= aligned_len;

	if (rte_eal_process_type() == RTE_PROC_PRIMARY) {
		/* notify user about changes in memory map */
		eal_memalloc_mem_event_notify(RTE_MEM_EVENT_FREE,
				aligned_start, aligned_len);

		/* don't care if any of this fails */
		malloc_heap_free_pages(aligned_start, aligned_len);

		request_sync();
	} else {
		struct malloc_mp_req req;

		memset(&req, 0, sizeof(req));

		req.t = REQ_TYPE_FREE;
		req.free_req.addr = aligned_start;
		req.free_req.len = aligned_len;

		/*
		 * we request primary to deallocate pages, but we don't do it
		 * in this thread. instead, we notify primary that we would like
		 * to deallocate pages, and this process will receive another
		 * request (in parallel) that will do it for us on another
		 * thread.
		 *
		 * we also don't really care if this succeeds - the data is
		 * already removed from the heap, so it is, for all intents and
		 * purposes, hidden from the rest of DPDK even if some other
		 * process (including this one) may have these pages mapped.
		 *
		 * notifications about deallocated memory happen during sync.
		 */
		request_to_primary(&req);
	}

	RTE_LOG(DEBUG, EAL, "Heap on socket %d was shrunk by %zdMB\n",
		msl->socket_id, aligned_len >> 20ULL);

	rte_mcfg_mem_write_unlock();
free_unlock:
	rte_spinlock_unlock(&(heap->lock));
	return ret;
}

int
malloc_heap_resize(struct malloc_elem *elem, size_t size)
{
	int ret;

	if (!malloc_elem_cookies_ok(elem) || elem->state != ELEM_BUSY)
		return -1;

	rte_spinlock_lock(&(elem->heap->lock));

	ret = malloc_elem_resize(elem, size);

	rte_spinlock_unlock(&(elem->heap->lock));

	return ret;
}

/*
 * Function to retrieve data for a given heap
 */
int
malloc_heap_get_stats(struct malloc_heap *heap,
		struct rte_malloc_socket_stats *socket_stats)
{
	size_t idx;
	struct malloc_elem *elem;

	rte_spinlock_lock(&heap->lock);

	/* Initialise variables for heap */
	socket_stats->free_count = 0;
	socket_stats->heap_freesz_bytes = 0;
	socket_stats->greatest_free_size = 0;

	/* Iterate through free list */
	for (idx = 0; idx < RTE_HEAP_NUM_FREELISTS; idx++) {
		for (elem = LIST_FIRST(&heap->free_head[idx]);
			!!elem; elem = LIST_NEXT(elem, free_list))
		{
			socket_stats->free_count++;
			socket_stats->heap_freesz_bytes += elem->size;
			if (elem->size > socket_stats->greatest_free_size)
				socket_stats->greatest_free_size = elem->size;
		}
	}
	/* Get stats on overall heap and allocated memory on this heap */
	socket_stats->heap_totalsz_bytes = heap->total_size;
	socket_stats->heap_allocsz_bytes = (socket_stats->heap_totalsz_bytes -
			socket_stats->heap_freesz_bytes);
	socket_stats->alloc_count = heap->alloc_count;

	rte_spinlock_unlock(&heap->lock);
	return 0;
}

/*
 * Function to retrieve data for a given heap
 */
void
malloc_heap_dump(struct malloc_heap *heap, FILE *f)
{
	struct malloc_elem *elem;

	rte_spinlock_lock(&heap->lock);

	fprintf(f, "Heap size: 0x%zx\n", heap->total_size);
	fprintf(f, "Heap alloc count: %u\n", heap->alloc_count);

	elem = heap->first;
	while (elem) {
		malloc_elem_dump(elem, f);
		elem = elem->next;
	}

	rte_spinlock_unlock(&heap->lock);
}

static int
destroy_elem(struct malloc_elem *elem, size_t len)
{
	struct malloc_heap *heap = elem->heap;

	/* notify all subscribers that a memory area is going to be removed */
	eal_memalloc_mem_event_notify(RTE_MEM_EVENT_FREE, elem, len);

	/* this element can be removed */
	malloc_elem_free_list_remove(elem);
	malloc_elem_hide_region(elem, elem, len);

	heap->total_size -= len;

	memset(elem, 0, sizeof(*elem));

	return 0;
}

struct rte_memseg_list *
malloc_heap_create_external_seg(void *va_addr, rte_iova_t iova_addrs[],
		unsigned int n_pages, size_t page_sz, const char *seg_name,
		unsigned int socket_id)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	char fbarray_name[RTE_FBARRAY_NAME_LEN];
	struct rte_memseg_list *msl = NULL;
	struct rte_fbarray *arr;
	size_t seg_len = n_pages * page_sz;
	unsigned int i;

	/* first, find a free memseg list */
	for (i = 0; i < RTE_MAX_MEMSEG_LISTS; i++) {
		struct rte_memseg_list *tmp = &mcfg->memsegs[i];
		if (tmp->base_va == NULL) {
			msl = tmp;
			break;
		}
	}
	if (msl == NULL) {
		RTE_LOG(ERR, EAL, "Couldn't find empty memseg list\n");
		rte_errno = ENOSPC;
		return NULL;
	}

	snprintf(fbarray_name, sizeof(fbarray_name), "%s_%p",
			seg_name, va_addr);

	/* create the backing fbarray */
	if (rte_fbarray_init(&msl->memseg_arr, fbarray_name, n_pages,
			sizeof(struct rte_memseg)) < 0) {
		RTE_LOG(ERR, EAL, "Couldn't create fbarray backing the memseg list\n");
		return NULL;
	}
	arr = &msl->memseg_arr;

	/* fbarray created, fill it up */
	for (i = 0; i < n_pages; i++) {
		struct rte_memseg *ms;

		rte_fbarray_set_used(arr, i);
		ms = rte_fbarray_get(arr, i);
		ms->addr = RTE_PTR_ADD(va_addr, i * page_sz);
		ms->iova = iova_addrs == NULL ? RTE_BAD_IOVA : iova_addrs[i];
		ms->hugepage_sz = page_sz;
		ms->len = page_sz;
		ms->nchannel = rte_memory_get_nchannel();
		ms->nrank = rte_memory_get_nrank();
		ms->socket_id = socket_id;
	}

	/* set up the memseg list */
	msl->base_va = va_addr;
	msl->page_sz = page_sz;
	msl->socket_id = socket_id;
	msl->len = seg_len;
	msl->version = 0;
	msl->external = 1;

	return msl;
}

struct extseg_walk_arg {
	void *va_addr;
	size_t len;
	struct rte_memseg_list *msl;
};

static int
extseg_walk(const struct rte_memseg_list *msl, void *arg)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct extseg_walk_arg *wa = arg;

	if (msl->base_va == wa->va_addr && msl->len == wa->len) {
		unsigned int found_idx;

		/* msl is const */
		found_idx = msl - mcfg->memsegs;
		wa->msl = &mcfg->memsegs[found_idx];
		return 1;
	}
	return 0;
}

struct rte_memseg_list *
malloc_heap_find_external_seg(void *va_addr, size_t len)
{
	struct extseg_walk_arg wa;
	int res;

	wa.va_addr = va_addr;
	wa.len = len;

	res = rte_memseg_list_walk_thread_unsafe(extseg_walk, &wa);

	if (res != 1) {
		/* 0 means nothing was found, -1 shouldn't happen */
		if (res == 0)
			rte_errno = ENOENT;
		return NULL;
	}
	return wa.msl;
}

int
malloc_heap_destroy_external_seg(struct rte_memseg_list *msl)
{
	/* destroy the fbarray backing this memory */
	if (rte_fbarray_destroy(&msl->memseg_arr) < 0)
		return -1;

	/* reset the memseg list */
	memset(msl, 0, sizeof(*msl));

	return 0;
}

int
malloc_heap_add_external_memory(struct malloc_heap *heap,
		struct rte_memseg_list *msl)
{
	/* erase contents of new memory */
	memset(msl->base_va, 0, msl->len);

	/* now, add newly minted memory to the malloc heap */
	malloc_heap_add_memory(heap, msl, msl->base_va, msl->len);

	heap->total_size += msl->len;

	/* all done! */
	RTE_LOG(DEBUG, EAL, "Added segment for heap %s starting at %p\n",
			heap->name, msl->base_va);

	/* notify all subscribers that a new memory area has been added */
	eal_memalloc_mem_event_notify(RTE_MEM_EVENT_ALLOC,
			msl->base_va, msl->len);

	return 0;
}

int
malloc_heap_remove_external_memory(struct malloc_heap *heap, void *va_addr,
		size_t len)
{
	struct malloc_elem *elem = heap->first;

	/* find element with specified va address */
	while (elem != NULL && elem != va_addr) {
		elem = elem->next;
		/* stop if we've blown past our VA */
		if (elem > (struct malloc_elem *)va_addr) {
			rte_errno = ENOENT;
			return -1;
		}
	}
	/* check if element was found */
	if (elem == NULL || elem->msl->len != len) {
		rte_errno = ENOENT;
		return -1;
	}
	/* if element's size is not equal to segment len, segment is busy */
	if (elem->state == ELEM_BUSY || elem->size != len) {
		rte_errno = EBUSY;
		return -1;
	}
	return destroy_elem(elem, len);
}

int
malloc_heap_create(struct malloc_heap *heap, const char *heap_name)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	uint32_t next_socket_id = mcfg->next_socket_id;

	/* prevent overflow. did you really create 2 billion heaps??? */
	if (next_socket_id > INT32_MAX) {
		RTE_LOG(ERR, EAL, "Cannot assign new socket ID's\n");
		rte_errno = ENOSPC;
		return -1;
	}

	/* initialize empty heap */
	heap->alloc_count = 0;
	heap->first = NULL;
	heap->last = NULL;
	LIST_INIT(heap->free_head);
	rte_spinlock_init(&heap->lock);
	heap->total_size = 0;
	heap->socket_id = next_socket_id;

	/* we hold a global mem hotplug writelock, so it's safe to increment */
	mcfg->next_socket_id++;

	/* set up name */
	strlcpy(heap->name, heap_name, RTE_HEAP_NAME_MAX_LEN);
	return 0;
}

int
malloc_heap_destroy(struct malloc_heap *heap)
{
	if (heap->alloc_count != 0) {
		RTE_LOG(ERR, EAL, "Heap is still in use\n");
		rte_errno = EBUSY;
		return -1;
	}
	if (heap->first != NULL || heap->last != NULL) {
		RTE_LOG(ERR, EAL, "Heap still contains memory segments\n");
		rte_errno = EBUSY;
		return -1;
	}
	if (heap->total_size != 0)
		RTE_LOG(ERR, EAL, "Total size not zero, heap is likely corrupt\n");

	/* after this, the lock will be dropped */
	memset(heap, 0, sizeof(*heap));

	return 0;
}

int
rte_eal_malloc_heap_init(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	unsigned int i;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	if (internal_conf->match_allocations)
		RTE_LOG(DEBUG, EAL, "Hugepages will be freed exactly as allocated.\n");

	if (rte_eal_process_type() == RTE_PROC_PRIMARY) {
		/* assign min socket ID to external heaps */
		mcfg->next_socket_id = EXTERNAL_HEAP_MIN_SOCKET_ID;

		/* assign names to default DPDK heaps */
		for (i = 0; i < rte_socket_count(); i++) {
			struct malloc_heap *heap = &mcfg->malloc_heaps[i];
			char heap_name[RTE_HEAP_NAME_MAX_LEN];
			int socket_id = rte_socket_id_by_idx(i);

			snprintf(heap_name, sizeof(heap_name),
					"socket_%i", socket_id);
			strlcpy(heap->name, heap_name, RTE_HEAP_NAME_MAX_LEN);
			heap->socket_id = socket_id;
		}
	}


	if (register_mp_requests()) {
		RTE_LOG(ERR, EAL, "Couldn't register malloc multiprocess actions\n");
		rte_mcfg_mem_read_unlock();
		return -1;
	}

	/* unlock mem hotplug here. it's safe for primary as no requests can
	 * even come before primary itself is fully initialized, and secondaries
	 * do not need to initialize the heap.
	 */
	rte_mcfg_mem_read_unlock();

	/* secondary process does not need to initialize anything */
	if (rte_eal_process_type() != RTE_PROC_PRIMARY)
		return 0;

	/* add all IOVA-contiguous areas to the heap */
	return rte_memseg_contig_walk(malloc_add_seg, NULL);
}
