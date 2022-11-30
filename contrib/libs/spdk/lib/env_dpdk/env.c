#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"
#include "spdk/util.h"
#include "spdk/env_dpdk.h"
#include "spdk/log.h"

#include "env_internal.h"

#include <rte_config.h>
#include <rte_cycles.h>
#include <rte_malloc.h>
#include <rte_mempool.h>
#include <rte_memzone.h>
#include <rte_version.h>

static uint64_t
virt_to_phys(void *vaddr)
{
	uint64_t ret;

	ret = rte_malloc_virt2iova(vaddr);
	if (ret != RTE_BAD_IOVA) {
		return ret;
	}

	return spdk_vtophys(vaddr, NULL);
}

void *
spdk_malloc(size_t size, size_t align, uint64_t *phys_addr, int socket_id, uint32_t flags)
{
	void *buf;

	if (flags == 0) {
		return NULL;
	}

	align = spdk_max(align, RTE_CACHE_LINE_SIZE);
	buf = rte_malloc_socket(NULL, size, align, socket_id);
	if (buf && phys_addr) {
#ifdef DEBUG
		SPDK_ERRLOG("phys_addr param in spdk_*malloc() is deprecated\n");
#endif
		*phys_addr = virt_to_phys(buf);
	}
	return buf;
}

void *
spdk_zmalloc(size_t size, size_t align, uint64_t *phys_addr, int socket_id, uint32_t flags)
{
	void *buf = spdk_malloc(size, align, phys_addr, socket_id, flags);
	if (buf) {
		memset(buf, 0, size);
	}
	return buf;
}

void *
spdk_realloc(void *buf, size_t size, size_t align)
{
	align = spdk_max(align, RTE_CACHE_LINE_SIZE);
	return rte_realloc(buf, size, align);
}

void
spdk_free(void *buf)
{
	rte_free(buf);
}

void *
spdk_dma_malloc_socket(size_t size, size_t align, uint64_t *phys_addr, int socket_id)
{
	return spdk_malloc(size, align, phys_addr, socket_id, (SPDK_MALLOC_DMA | SPDK_MALLOC_SHARE));
}

void *
spdk_dma_zmalloc_socket(size_t size, size_t align, uint64_t *phys_addr, int socket_id)
{
	return spdk_zmalloc(size, align, phys_addr, socket_id, (SPDK_MALLOC_DMA | SPDK_MALLOC_SHARE));
}

void *
spdk_dma_malloc(size_t size, size_t align, uint64_t *phys_addr)
{
	return spdk_dma_malloc_socket(size, align, phys_addr, SPDK_ENV_SOCKET_ID_ANY);
}

void *
spdk_dma_zmalloc(size_t size, size_t align, uint64_t *phys_addr)
{
	return spdk_dma_zmalloc_socket(size, align, phys_addr, SPDK_ENV_SOCKET_ID_ANY);
}

void *
spdk_dma_realloc(void *buf, size_t size, size_t align, uint64_t *phys_addr)
{
	void *new_buf;

	align = spdk_max(align, RTE_CACHE_LINE_SIZE);
	new_buf = rte_realloc(buf, size, align);
	if (new_buf && phys_addr) {
		*phys_addr = virt_to_phys(new_buf);
	}
	return new_buf;
}

void
spdk_dma_free(void *buf)
{
	spdk_free(buf);
}

void *
spdk_memzone_reserve_aligned(const char *name, size_t len, int socket_id,
			     unsigned flags, unsigned align)
{
	const struct rte_memzone *mz;
	unsigned dpdk_flags = 0;

	if ((flags & SPDK_MEMZONE_NO_IOVA_CONTIG) == 0) {
		dpdk_flags |= RTE_MEMZONE_IOVA_CONTIG;
	}

	if (socket_id == SPDK_ENV_SOCKET_ID_ANY) {
		socket_id = SOCKET_ID_ANY;
	}

	mz = rte_memzone_reserve_aligned(name, len, socket_id, dpdk_flags, align);

	if (mz != NULL) {
		memset(mz->addr, 0, len);
		return mz->addr;
	} else {
		return NULL;
	}
}

void *
spdk_memzone_reserve(const char *name, size_t len, int socket_id, unsigned flags)
{
	return spdk_memzone_reserve_aligned(name, len, socket_id, flags,
					    RTE_CACHE_LINE_SIZE);
}

void *
spdk_memzone_lookup(const char *name)
{
	const struct rte_memzone *mz = rte_memzone_lookup(name);

	if (mz != NULL) {
		return mz->addr;
	} else {
		return NULL;
	}
}

int
spdk_memzone_free(const char *name)
{
	const struct rte_memzone *mz = rte_memzone_lookup(name);

	if (mz != NULL) {
		return rte_memzone_free(mz);
	}

	return -1;
}

void
spdk_memzone_dump(FILE *f)
{
	rte_memzone_dump(f);
}

struct spdk_mempool *
spdk_mempool_create_ctor(const char *name, size_t count,
			 size_t ele_size, size_t cache_size, int socket_id,
			 spdk_mempool_obj_cb_t *obj_init, void *obj_init_arg)
{
	struct rte_mempool *mp;
	size_t tmp;

	if (socket_id == SPDK_ENV_SOCKET_ID_ANY) {
		socket_id = SOCKET_ID_ANY;
	}

	/* No more than half of all elements can be in cache */
	tmp = (count / 2) / rte_lcore_count();
	if (cache_size > tmp) {
		cache_size = tmp;
	}

	if (cache_size > RTE_MEMPOOL_CACHE_MAX_SIZE) {
		cache_size = RTE_MEMPOOL_CACHE_MAX_SIZE;
	}

	mp = rte_mempool_create(name, count, ele_size, cache_size,
				0, NULL, NULL, (rte_mempool_obj_cb_t *)obj_init, obj_init_arg,
				socket_id, MEMPOOL_F_NO_IOVA_CONTIG);

	return (struct spdk_mempool *)mp;
}


struct spdk_mempool *
spdk_mempool_create(const char *name, size_t count,
		    size_t ele_size, size_t cache_size, int socket_id)
{
	return spdk_mempool_create_ctor(name, count, ele_size, cache_size, socket_id,
					NULL, NULL);
}

char *
spdk_mempool_get_name(struct spdk_mempool *mp)
{
	return ((struct rte_mempool *)mp)->name;
}

void
spdk_mempool_free(struct spdk_mempool *mp)
{
	rte_mempool_free((struct rte_mempool *)mp);
}

void *
spdk_mempool_get(struct spdk_mempool *mp)
{
	void *ele = NULL;
	int rc;

	rc = rte_mempool_get((struct rte_mempool *)mp, &ele);
	if (rc != 0) {
		return NULL;
	}
	return ele;
}

int
spdk_mempool_get_bulk(struct spdk_mempool *mp, void **ele_arr, size_t count)
{
	return rte_mempool_get_bulk((struct rte_mempool *)mp, ele_arr, count);
}

void
spdk_mempool_put(struct spdk_mempool *mp, void *ele)
{
	rte_mempool_put((struct rte_mempool *)mp, ele);
}

void
spdk_mempool_put_bulk(struct spdk_mempool *mp, void **ele_arr, size_t count)
{
	rte_mempool_put_bulk((struct rte_mempool *)mp, ele_arr, count);
}

size_t
spdk_mempool_count(const struct spdk_mempool *pool)
{
	return rte_mempool_avail_count((struct rte_mempool *)pool);
}

uint32_t
spdk_mempool_obj_iter(struct spdk_mempool *mp, spdk_mempool_obj_cb_t obj_cb,
		      void *obj_cb_arg)
{
	return rte_mempool_obj_iter((struct rte_mempool *)mp, (rte_mempool_obj_cb_t *)obj_cb,
				    obj_cb_arg);
}

struct spdk_mempool *
spdk_mempool_lookup(const char *name)
{
	return (struct spdk_mempool *)rte_mempool_lookup(name);
}

bool
spdk_process_is_primary(void)
{
	return (rte_eal_process_type() == RTE_PROC_PRIMARY);
}

uint64_t spdk_get_ticks(void)
{
	return rte_get_timer_cycles();
}

uint64_t spdk_get_ticks_hz(void)
{
	return rte_get_timer_hz();
}

void spdk_delay_us(unsigned int us)
{
	rte_delay_us(us);
}

void spdk_pause(void)
{
	rte_pause();
}

void
spdk_unaffinitize_thread(void)
{
	rte_cpuset_t new_cpuset, orig_cpuset;
	long num_cores, i, orig_num_cores;

	CPU_ZERO(&new_cpuset);

	num_cores = sysconf(_SC_NPROCESSORS_CONF);

	/* Create a mask containing all CPUs */
	for (i = 0; i < num_cores; i++) {
		CPU_SET(i, &new_cpuset);
	}

	rte_thread_get_affinity(&orig_cpuset);
	orig_num_cores = CPU_COUNT(&orig_cpuset);
	if (orig_num_cores < num_cores) {
		for (i = 0; i < orig_num_cores; i++) {
			if (CPU_ISSET(i, &orig_cpuset)) {
				CPU_CLR(i, &new_cpuset);
			}
		}
	}

	rte_thread_set_affinity(&new_cpuset);
}

void *
spdk_call_unaffinitized(void *cb(void *arg), void *arg)
{
	rte_cpuset_t orig_cpuset;
	void *ret;

	if (cb == NULL) {
		return NULL;
	}

	rte_thread_get_affinity(&orig_cpuset);

	spdk_unaffinitize_thread();

	ret = cb(arg);

	rte_thread_set_affinity(&orig_cpuset);

	return ret;
}

struct spdk_ring *
spdk_ring_create(enum spdk_ring_type type, size_t count, int socket_id)
{
	char ring_name[64];
	static uint32_t ring_num = 0;
	unsigned flags = RING_F_EXACT_SZ;

	switch (type) {
	case SPDK_RING_TYPE_SP_SC:
		flags |= RING_F_SP_ENQ | RING_F_SC_DEQ;
		break;
	case SPDK_RING_TYPE_MP_SC:
		flags |= RING_F_SC_DEQ;
		break;
	case SPDK_RING_TYPE_MP_MC:
		flags |= 0;
		break;
	default:
		return NULL;
	}

	snprintf(ring_name, sizeof(ring_name), "ring_%u_%d",
		 __atomic_fetch_add(&ring_num, 1, __ATOMIC_RELAXED), getpid());

	return (struct spdk_ring *)rte_ring_create(ring_name, count, socket_id, flags);
}

void
spdk_ring_free(struct spdk_ring *ring)
{
	rte_ring_free((struct rte_ring *)ring);
}

size_t
spdk_ring_count(struct spdk_ring *ring)
{
	return rte_ring_count((struct rte_ring *)ring);
}

size_t
spdk_ring_enqueue(struct spdk_ring *ring, void **objs, size_t count,
		  size_t *free_space)
{
	return rte_ring_enqueue_bulk((struct rte_ring *)ring, objs, count,
				     (unsigned int *)free_space);
}

size_t
spdk_ring_dequeue(struct spdk_ring *ring, void **objs, size_t count)
{
	return rte_ring_dequeue_burst((struct rte_ring *)ring, objs, count, NULL);
}

void
spdk_env_dpdk_dump_mem_stats(FILE *file)
{
	fprintf(file, "DPDK memory size %" PRIu64 "\n", rte_eal_get_physmem_size());
	fprintf(file, "DPDK memory layout\n");
	rte_dump_physmem_layout(file);
	fprintf(file, "DPDK memzones.\n");
	rte_memzone_dump(file);
	fprintf(file, "DPDK mempools.\n");
	rte_mempool_list_dump(file);
	fprintf(file, "DPDK malloc stats.\n");
	rte_malloc_dump_stats(file, NULL);
	fprintf(file, "DPDK malloc heaps.\n");
	rte_malloc_dump_heaps(file);
}
