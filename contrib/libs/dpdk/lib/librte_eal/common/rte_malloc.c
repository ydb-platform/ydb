#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2019 Intel Corporation
 */

#include <stdint.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/queue.h>

#include <rte_errno.h>
#include <rte_memcpy.h>
#include <rte_memory.h>
#include <rte_eal.h>
#include <rte_eal_memconfig.h>
#include <rte_branch_prediction.h>
#include <rte_debug.h>
#include <rte_launch.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_common.h>
#include <rte_spinlock.h>

#include <rte_eal_trace.h>

#include <rte_malloc.h>
#include "malloc_elem.h"
#include "malloc_heap.h"
#include "eal_memalloc.h"
#include "eal_memcfg.h"
#include "eal_private.h"


/* Free the memory space back to heap */
static void
mem_free(void *addr, const bool trace_ena)
{
	if (trace_ena)
		rte_eal_trace_mem_free(addr);

	if (addr == NULL) return;
	if (malloc_heap_free(malloc_elem_from_data(addr)) < 0)
		RTE_LOG(ERR, EAL, "Error: Invalid memory\n");
}

void
rte_free(void *addr)
{
	return mem_free(addr, true);
}

void
eal_free_no_trace(void *addr)
{
	return mem_free(addr, false);
}

static void *
malloc_socket(const char *type, size_t size, unsigned int align,
		int socket_arg, const bool trace_ena)
{
	void *ptr;

	/* return NULL if size is 0 or alignment is not power-of-2 */
	if (size == 0 || (align && !rte_is_power_of_2(align)))
		return NULL;

	/* if there are no hugepages and if we are not allocating from an
	 * external heap, use memory from any socket available. checking for
	 * socket being external may return -1 in case of invalid socket, but
	 * that's OK - if there are no hugepages, it doesn't matter.
	 */
	if (rte_malloc_heap_socket_is_external(socket_arg) != 1 &&
				!rte_eal_has_hugepages())
		socket_arg = SOCKET_ID_ANY;

	ptr = malloc_heap_alloc(type, size, socket_arg, 0,
			align == 0 ? 1 : align, 0, false);

	if (trace_ena)
		rte_eal_trace_mem_malloc(type, size, align, socket_arg, ptr);
	return ptr;
}

/*
 * Allocate memory on specified heap.
 */
void *
rte_malloc_socket(const char *type, size_t size, unsigned int align,
		int socket_arg)
{
	return malloc_socket(type, size, align, socket_arg, true);
}

void *
eal_malloc_no_trace(const char *type, size_t size, unsigned int align)
{
	return malloc_socket(type, size, align, SOCKET_ID_ANY, false);
}

/*
 * Allocate memory on default heap.
 */
void *
rte_malloc(const char *type, size_t size, unsigned align)
{
	return rte_malloc_socket(type, size, align, SOCKET_ID_ANY);
}

/*
 * Allocate zero'd memory on specified heap.
 */
void *
rte_zmalloc_socket(const char *type, size_t size, unsigned align, int socket)
{
	void *ptr = rte_malloc_socket(type, size, align, socket);

#ifdef RTE_MALLOC_DEBUG
	/*
	 * If DEBUG is enabled, then freed memory is marked with poison
	 * value and set to zero on allocation.
	 * If DEBUG is not enabled then  memory is already zeroed.
	 */
	if (ptr != NULL)
		memset(ptr, 0, size);
#endif

	rte_eal_trace_mem_zmalloc(type, size, align, socket, ptr);
	return ptr;
}

/*
 * Allocate zero'd memory on default heap.
 */
void *
rte_zmalloc(const char *type, size_t size, unsigned align)
{
	return rte_zmalloc_socket(type, size, align, SOCKET_ID_ANY);
}

/*
 * Allocate zero'd memory on specified heap.
 */
void *
rte_calloc_socket(const char *type, size_t num, size_t size, unsigned align, int socket)
{
	return rte_zmalloc_socket(type, num * size, align, socket);
}

/*
 * Allocate zero'd memory on default heap.
 */
void *
rte_calloc(const char *type, size_t num, size_t size, unsigned align)
{
	return rte_zmalloc(type, num * size, align);
}

/*
 * Resize allocated memory on specified heap.
 */
void *
rte_realloc_socket(void *ptr, size_t size, unsigned int align, int socket)
{
	if (ptr == NULL)
		return rte_malloc_socket(NULL, size, align, socket);

	struct malloc_elem *elem = malloc_elem_from_data(ptr);
	if (elem == NULL) {
		RTE_LOG(ERR, EAL, "Error: memory corruption detected\n");
		return NULL;
	}

	size = RTE_CACHE_LINE_ROUNDUP(size), align = RTE_CACHE_LINE_ROUNDUP(align);

	/* check requested socket id and alignment matches first, and if ok,
	 * see if we can resize block
	 */
	if ((socket == SOCKET_ID_ANY ||
	     (unsigned int)socket == elem->heap->socket_id) &&
			RTE_PTR_ALIGN(ptr, align) == ptr &&
			malloc_heap_resize(elem, size) == 0) {
		rte_eal_trace_mem_realloc(size, align, socket, ptr);
		return ptr;
	}

	/* either requested socket id doesn't match, alignment is off
	 * or we have no room to expand,
	 * so move the data.
	 */
	void *new_ptr = rte_malloc_socket(NULL, size, align, socket);
	if (new_ptr == NULL)
		return NULL;
	/* elem: |pad|data_elem|data|trailer| */
	const size_t old_size = elem->size - elem->pad - MALLOC_ELEM_OVERHEAD;
	rte_memcpy(new_ptr, ptr, old_size < size ? old_size : size);
	rte_free(ptr);

	rte_eal_trace_mem_realloc(size, align, socket, new_ptr);
	return new_ptr;
}

/*
 * Resize allocated memory.
 */
void *
rte_realloc(void *ptr, size_t size, unsigned int align)
{
	return rte_realloc_socket(ptr, size, align, SOCKET_ID_ANY);
}

int
rte_malloc_validate(const void *ptr, size_t *size)
{
	const struct malloc_elem *elem = malloc_elem_from_data(ptr);
	if (!malloc_elem_cookies_ok(elem))
		return -1;
	if (size != NULL)
		*size = elem->size - elem->pad - MALLOC_ELEM_OVERHEAD;
	return 0;
}

/*
 * Function to retrieve data for heap on given socket
 */
int
rte_malloc_get_socket_stats(int socket,
		struct rte_malloc_socket_stats *socket_stats)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	int heap_idx;

	heap_idx = malloc_socket_to_heap_id(socket);
	if (heap_idx < 0)
		return -1;

	return malloc_heap_get_stats(&mcfg->malloc_heaps[heap_idx],
			socket_stats);
}

/*
 * Function to dump contents of all heaps
 */
void
rte_malloc_dump_heaps(FILE *f)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	unsigned int idx;

	for (idx = 0; idx < RTE_MAX_HEAPS; idx++) {
		fprintf(f, "Heap id: %u\n", idx);
		malloc_heap_dump(&mcfg->malloc_heaps[idx], f);
	}
}

int
rte_malloc_heap_get_socket(const char *name)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct malloc_heap *heap = NULL;
	unsigned int idx;
	int ret;

	if (name == NULL ||
			strnlen(name, RTE_HEAP_NAME_MAX_LEN) == 0 ||
			strnlen(name, RTE_HEAP_NAME_MAX_LEN) ==
				RTE_HEAP_NAME_MAX_LEN) {
		rte_errno = EINVAL;
		return -1;
	}
	rte_mcfg_mem_read_lock();
	for (idx = 0; idx < RTE_MAX_HEAPS; idx++) {
		struct malloc_heap *tmp = &mcfg->malloc_heaps[idx];

		if (!strncmp(name, tmp->name, RTE_HEAP_NAME_MAX_LEN)) {
			heap = tmp;
			break;
		}
	}

	if (heap != NULL) {
		ret = heap->socket_id;
	} else {
		rte_errno = ENOENT;
		ret = -1;
	}
	rte_mcfg_mem_read_unlock();

	return ret;
}

int
rte_malloc_heap_socket_is_external(int socket_id)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	unsigned int idx;
	int ret = -1;

	if (socket_id == SOCKET_ID_ANY)
		return 0;

	rte_mcfg_mem_read_lock();
	for (idx = 0; idx < RTE_MAX_HEAPS; idx++) {
		struct malloc_heap *tmp = &mcfg->malloc_heaps[idx];

		if ((int)tmp->socket_id == socket_id) {
			/* external memory always has large socket ID's */
			ret = tmp->socket_id >= RTE_MAX_NUMA_NODES;
			break;
		}
	}
	rte_mcfg_mem_read_unlock();

	return ret;
}

/*
 * Print stats on memory type. If type is NULL, info on all types is printed
 */
void
rte_malloc_dump_stats(FILE *f, __rte_unused const char *type)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	unsigned int heap_id;
	struct rte_malloc_socket_stats sock_stats;

	/* Iterate through all initialised heaps */
	for (heap_id = 0; heap_id < RTE_MAX_HEAPS; heap_id++) {
		struct malloc_heap *heap = &mcfg->malloc_heaps[heap_id];

		malloc_heap_get_stats(heap, &sock_stats);

		fprintf(f, "Heap id:%u\n", heap_id);
		fprintf(f, "\tHeap name:%s\n", heap->name);
		fprintf(f, "\tHeap_size:%zu,\n", sock_stats.heap_totalsz_bytes);
		fprintf(f, "\tFree_size:%zu,\n", sock_stats.heap_freesz_bytes);
		fprintf(f, "\tAlloc_size:%zu,\n", sock_stats.heap_allocsz_bytes);
		fprintf(f, "\tGreatest_free_size:%zu,\n",
				sock_stats.greatest_free_size);
		fprintf(f, "\tAlloc_count:%u,\n",sock_stats.alloc_count);
		fprintf(f, "\tFree_count:%u,\n", sock_stats.free_count);
	}
	return;
}

/*
 * TODO: Set limit to memory that can be allocated to memory type
 */
int
rte_malloc_set_limit(__rte_unused const char *type,
		__rte_unused size_t max)
{
	return 0;
}

/*
 * Return the IO address of a virtual address obtained through rte_malloc
 */
rte_iova_t
rte_malloc_virt2iova(const void *addr)
{
	const struct rte_memseg *ms;
	struct malloc_elem *elem = malloc_elem_from_data(addr);

	if (elem == NULL)
		return RTE_BAD_IOVA;

	if (!elem->msl->external && rte_eal_iova_mode() == RTE_IOVA_VA)
		return (uintptr_t) addr;

	ms = rte_mem_virt2memseg(addr, elem->msl);
	if (ms == NULL)
		return RTE_BAD_IOVA;

	if (ms->iova == RTE_BAD_IOVA)
		return RTE_BAD_IOVA;

	return ms->iova + RTE_PTR_DIFF(addr, ms->addr);
}

static struct malloc_heap *
find_named_heap(const char *name)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	unsigned int i;

	for (i = 0; i < RTE_MAX_HEAPS; i++) {
		struct malloc_heap *heap = &mcfg->malloc_heaps[i];

		if (!strncmp(name, heap->name, RTE_HEAP_NAME_MAX_LEN))
			return heap;
	}
	return NULL;
}

int
rte_malloc_heap_memory_add(const char *heap_name, void *va_addr, size_t len,
		rte_iova_t iova_addrs[], unsigned int n_pages, size_t page_sz)
{
	struct malloc_heap *heap = NULL;
	struct rte_memseg_list *msl;
	unsigned int n;
	int ret;

	if (heap_name == NULL || va_addr == NULL ||
			page_sz == 0 || !rte_is_power_of_2(page_sz) ||
			RTE_ALIGN(len, page_sz) != len ||
			!rte_is_aligned(va_addr, page_sz) ||
			((len / page_sz) != n_pages && iova_addrs != NULL) ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) == 0 ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) ==
				RTE_HEAP_NAME_MAX_LEN) {
		rte_errno = EINVAL;
		return -1;
	}
	rte_mcfg_mem_write_lock();

	/* find our heap */
	heap = find_named_heap(heap_name);
	if (heap == NULL) {
		rte_errno = ENOENT;
		ret = -1;
		goto unlock;
	}
	if (heap->socket_id < RTE_MAX_NUMA_NODES) {
		/* cannot add memory to internal heaps */
		rte_errno = EPERM;
		ret = -1;
		goto unlock;
	}
	n = len / page_sz;

	msl = malloc_heap_create_external_seg(va_addr, iova_addrs, n, page_sz,
			heap_name, heap->socket_id);
	if (msl == NULL) {
		ret = -1;
		goto unlock;
	}

	rte_spinlock_lock(&heap->lock);
	ret = malloc_heap_add_external_memory(heap, msl);
	msl->heap = 1; /* mark it as heap segment */
	rte_spinlock_unlock(&heap->lock);

unlock:
	rte_mcfg_mem_write_unlock();

	return ret;
}

int
rte_malloc_heap_memory_remove(const char *heap_name, void *va_addr, size_t len)
{
	struct malloc_heap *heap = NULL;
	struct rte_memseg_list *msl;
	int ret;

	if (heap_name == NULL || va_addr == NULL || len == 0 ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) == 0 ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) ==
				RTE_HEAP_NAME_MAX_LEN) {
		rte_errno = EINVAL;
		return -1;
	}
	rte_mcfg_mem_write_lock();
	/* find our heap */
	heap = find_named_heap(heap_name);
	if (heap == NULL) {
		rte_errno = ENOENT;
		ret = -1;
		goto unlock;
	}
	if (heap->socket_id < RTE_MAX_NUMA_NODES) {
		/* cannot remove memory from internal heaps */
		rte_errno = EPERM;
		ret = -1;
		goto unlock;
	}

	msl = malloc_heap_find_external_seg(va_addr, len);
	if (msl == NULL) {
		ret = -1;
		goto unlock;
	}

	rte_spinlock_lock(&heap->lock);
	ret = malloc_heap_remove_external_memory(heap, va_addr, len);
	rte_spinlock_unlock(&heap->lock);
	if (ret != 0)
		goto unlock;

	ret = malloc_heap_destroy_external_seg(msl);

unlock:
	rte_mcfg_mem_write_unlock();

	return ret;
}

static int
sync_memory(const char *heap_name, void *va_addr, size_t len, bool attach)
{
	struct malloc_heap *heap = NULL;
	struct rte_memseg_list *msl;
	int ret;

	if (heap_name == NULL || va_addr == NULL || len == 0 ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) == 0 ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) ==
				RTE_HEAP_NAME_MAX_LEN) {
		rte_errno = EINVAL;
		return -1;
	}
	rte_mcfg_mem_read_lock();

	/* find our heap */
	heap = find_named_heap(heap_name);
	if (heap == NULL) {
		rte_errno = ENOENT;
		ret = -1;
		goto unlock;
	}
	/* we shouldn't be able to sync to internal heaps */
	if (heap->socket_id < RTE_MAX_NUMA_NODES) {
		rte_errno = EPERM;
		ret = -1;
		goto unlock;
	}

	/* find corresponding memseg list to sync to */
	msl = malloc_heap_find_external_seg(va_addr, len);
	if (msl == NULL) {
		ret = -1;
		goto unlock;
	}

	if (attach) {
		ret = rte_fbarray_attach(&msl->memseg_arr);
		if (ret == 0) {
			/* notify all subscribers that a new memory area was
			 * added.
			 */
			eal_memalloc_mem_event_notify(RTE_MEM_EVENT_ALLOC,
					va_addr, len);
		} else {
			ret = -1;
			goto unlock;
		}
	} else {
		/* notify all subscribers that a memory area is about to
		 * be removed.
		 */
		eal_memalloc_mem_event_notify(RTE_MEM_EVENT_FREE,
				msl->base_va, msl->len);
		ret = rte_fbarray_detach(&msl->memseg_arr);
		if (ret < 0) {
			ret = -1;
			goto unlock;
		}
	}
unlock:
	rte_mcfg_mem_read_unlock();
	return ret;
}

int
rte_malloc_heap_memory_attach(const char *heap_name, void *va_addr, size_t len)
{
	return sync_memory(heap_name, va_addr, len, true);
}

int
rte_malloc_heap_memory_detach(const char *heap_name, void *va_addr, size_t len)
{
	return sync_memory(heap_name, va_addr, len, false);
}

int
rte_malloc_heap_create(const char *heap_name)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct malloc_heap *heap = NULL;
	int i, ret;

	if (heap_name == NULL ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) == 0 ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) ==
				RTE_HEAP_NAME_MAX_LEN) {
		rte_errno = EINVAL;
		return -1;
	}
	/* check if there is space in the heap list, or if heap with this name
	 * already exists.
	 */
	rte_mcfg_mem_write_lock();

	for (i = 0; i < RTE_MAX_HEAPS; i++) {
		struct malloc_heap *tmp = &mcfg->malloc_heaps[i];
		/* existing heap */
		if (strncmp(heap_name, tmp->name,
				RTE_HEAP_NAME_MAX_LEN) == 0) {
			RTE_LOG(ERR, EAL, "Heap %s already exists\n",
				heap_name);
			rte_errno = EEXIST;
			ret = -1;
			goto unlock;
		}
		/* empty heap */
		if (strnlen(tmp->name, RTE_HEAP_NAME_MAX_LEN) == 0) {
			heap = tmp;
			break;
		}
	}
	if (heap == NULL) {
		RTE_LOG(ERR, EAL, "Cannot create new heap: no space\n");
		rte_errno = ENOSPC;
		ret = -1;
		goto unlock;
	}

	/* we're sure that we can create a new heap, so do it */
	ret = malloc_heap_create(heap, heap_name);
unlock:
	rte_mcfg_mem_write_unlock();

	return ret;
}

int
rte_malloc_heap_destroy(const char *heap_name)
{
	struct malloc_heap *heap = NULL;
	int ret;

	if (heap_name == NULL ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) == 0 ||
			strnlen(heap_name, RTE_HEAP_NAME_MAX_LEN) ==
				RTE_HEAP_NAME_MAX_LEN) {
		rte_errno = EINVAL;
		return -1;
	}
	rte_mcfg_mem_write_lock();

	/* start from non-socket heaps */
	heap = find_named_heap(heap_name);
	if (heap == NULL) {
		RTE_LOG(ERR, EAL, "Heap %s not found\n", heap_name);
		rte_errno = ENOENT;
		ret = -1;
		goto unlock;
	}
	/* we shouldn't be able to destroy internal heaps */
	if (heap->socket_id < RTE_MAX_NUMA_NODES) {
		rte_errno = EPERM;
		ret = -1;
		goto unlock;
	}
	/* sanity checks done, now we can destroy the heap */
	rte_spinlock_lock(&heap->lock);
	ret = malloc_heap_destroy(heap);

	/* if we failed, lock is still active */
	if (ret < 0)
		rte_spinlock_unlock(&heap->lock);
unlock:
	rte_mcfg_mem_write_unlock();

	return ret;
}
