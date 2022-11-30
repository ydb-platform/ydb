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

#include "env_internal.h"

#include <rte_dev.h>
#include <rte_config.h>
#include <rte_memory.h>
#include <rte_eal_memconfig.h>

#include "spdk_internal/assert.h"

#include "spdk/assert.h"
#include "spdk/likely.h"
#include "spdk/queue.h"
#include "spdk/util.h"
#include "spdk/memory.h"
#include "spdk/env_dpdk.h"
#include "spdk/log.h"

#ifndef __linux__
#define VFIO_ENABLED 0
#else
#include <linux/version.h>
#if LINUX_VERSION_CODE >= KERNEL_VERSION(3, 6, 0)
#define VFIO_ENABLED 1
#include <linux/vfio.h>
#include <rte_vfio.h>

struct spdk_vfio_dma_map {
	struct vfio_iommu_type1_dma_map map;
	TAILQ_ENTRY(spdk_vfio_dma_map) tailq;
};

struct vfio_cfg {
	int fd;
	bool enabled;
	bool noiommu_enabled;
	unsigned device_ref;
	TAILQ_HEAD(, spdk_vfio_dma_map) maps;
	pthread_mutex_t mutex;
};

static struct vfio_cfg g_vfio = {
	.fd = -1,
	.enabled = false,
	.noiommu_enabled = false,
	.device_ref = 0,
	.maps = TAILQ_HEAD_INITIALIZER(g_vfio.maps),
	.mutex = PTHREAD_MUTEX_INITIALIZER
};

#else
#define VFIO_ENABLED 0
#endif
#endif

#if DEBUG
#define DEBUG_PRINT(...) SPDK_ERRLOG(__VA_ARGS__)
#else
#define DEBUG_PRINT(...)
#endif

#define FN_2MB_TO_4KB(fn)	(fn << (SHIFT_2MB - SHIFT_4KB))
#define FN_4KB_TO_2MB(fn)	(fn >> (SHIFT_2MB - SHIFT_4KB))

#define MAP_256TB_IDX(vfn_2mb)	((vfn_2mb) >> (SHIFT_1GB - SHIFT_2MB))
#define MAP_1GB_IDX(vfn_2mb)	((vfn_2mb) & ((1ULL << (SHIFT_1GB - SHIFT_2MB)) - 1))

/* Page is registered */
#define REG_MAP_REGISTERED	(1ULL << 62)

/* A notification region barrier. The 2MB translation entry that's marked
 * with this flag must be unregistered separately. This allows contiguous
 * regions to be unregistered in the same chunks they were registered.
 */
#define REG_MAP_NOTIFY_START	(1ULL << 63)

/* Translation of a single 2MB page. */
struct map_2mb {
	uint64_t translation_2mb;
};

/* Second-level map table indexed by bits [21..29] of the virtual address.
 * Each entry contains the address translation or error for entries that haven't
 * been retrieved yet.
 */
struct map_1gb {
	struct map_2mb map[1ULL << (SHIFT_1GB - SHIFT_2MB)];
};

/* Top-level map table indexed by bits [30..47] of the virtual address.
 * Each entry points to a second-level map table or NULL.
 */
struct map_256tb {
	struct map_1gb *map[1ULL << (SHIFT_256TB - SHIFT_1GB)];
};

/* Page-granularity memory address translation */
struct spdk_mem_map {
	struct map_256tb map_256tb;
	pthread_mutex_t mutex;
	uint64_t default_translation;
	struct spdk_mem_map_ops ops;
	void *cb_ctx;
	TAILQ_ENTRY(spdk_mem_map) tailq;
};

/* Registrations map. The 64 bit translations are bit fields with the
 * following layout (starting with the low bits):
 *    0 - 61 : reserved
 *   62 - 63 : flags
 */
static struct spdk_mem_map *g_mem_reg_map;
static TAILQ_HEAD(spdk_mem_map_head, spdk_mem_map) g_spdk_mem_maps =
	TAILQ_HEAD_INITIALIZER(g_spdk_mem_maps);
static pthread_mutex_t g_spdk_mem_map_mutex = PTHREAD_MUTEX_INITIALIZER;

static bool g_legacy_mem;

/*
 * Walk the currently registered memory via the main memory registration map
 * and call the new map's notify callback for each virtually contiguous region.
 */
static int
mem_map_notify_walk(struct spdk_mem_map *map, enum spdk_mem_map_notify_action action)
{
	size_t idx_256tb;
	uint64_t idx_1gb;
	uint64_t contig_start = UINT64_MAX;
	uint64_t contig_end = UINT64_MAX;
	struct map_1gb *map_1gb;
	int rc;

	if (!g_mem_reg_map) {
		return -EINVAL;
	}

	/* Hold the memory registration map mutex so no new registrations can be added while we are looping. */
	pthread_mutex_lock(&g_mem_reg_map->mutex);

	for (idx_256tb = 0;
	     idx_256tb < sizeof(g_mem_reg_map->map_256tb.map) / sizeof(g_mem_reg_map->map_256tb.map[0]);
	     idx_256tb++) {
		map_1gb = g_mem_reg_map->map_256tb.map[idx_256tb];

		if (!map_1gb) {
			if (contig_start != UINT64_MAX) {
				/* End of of a virtually contiguous range */
				rc = map->ops.notify_cb(map->cb_ctx, map, action,
							(void *)contig_start,
							contig_end - contig_start + VALUE_2MB);
				/* Don't bother handling unregister failures. It can't be any worse */
				if (rc != 0 && action == SPDK_MEM_MAP_NOTIFY_REGISTER) {
					goto err_unregister;
				}
			}
			contig_start = UINT64_MAX;
			continue;
		}

		for (idx_1gb = 0; idx_1gb < sizeof(map_1gb->map) / sizeof(map_1gb->map[0]); idx_1gb++) {
			if ((map_1gb->map[idx_1gb].translation_2mb & REG_MAP_REGISTERED) &&
			    (contig_start == UINT64_MAX ||
			     (map_1gb->map[idx_1gb].translation_2mb & REG_MAP_NOTIFY_START) == 0)) {
				/* Rebuild the virtual address from the indexes */
				uint64_t vaddr = (idx_256tb << SHIFT_1GB) | (idx_1gb << SHIFT_2MB);

				if (contig_start == UINT64_MAX) {
					contig_start = vaddr;
				}

				contig_end = vaddr;
			} else {
				if (contig_start != UINT64_MAX) {
					/* End of of a virtually contiguous range */
					rc = map->ops.notify_cb(map->cb_ctx, map, action,
								(void *)contig_start,
								contig_end - contig_start + VALUE_2MB);
					/* Don't bother handling unregister failures. It can't be any worse */
					if (rc != 0 && action == SPDK_MEM_MAP_NOTIFY_REGISTER) {
						goto err_unregister;
					}

					/* This page might be a part of a neighbour region, so process
					 * it again. The idx_1gb will be incremented immediately.
					 */
					idx_1gb--;
				}
				contig_start = UINT64_MAX;
			}
		}
	}

	pthread_mutex_unlock(&g_mem_reg_map->mutex);
	return 0;

err_unregister:
	/* Unwind to the first empty translation so we don't unregister
	 * a region that just failed to register.
	 */
	idx_256tb = MAP_256TB_IDX((contig_start >> SHIFT_2MB) - 1);
	idx_1gb = MAP_1GB_IDX((contig_start >> SHIFT_2MB) - 1);
	contig_start = UINT64_MAX;
	contig_end = UINT64_MAX;

	/* Unregister any memory we managed to register before the failure */
	for (; idx_256tb < SIZE_MAX; idx_256tb--) {
		map_1gb = g_mem_reg_map->map_256tb.map[idx_256tb];

		if (!map_1gb) {
			if (contig_end != UINT64_MAX) {
				/* End of of a virtually contiguous range */
				map->ops.notify_cb(map->cb_ctx, map,
						   SPDK_MEM_MAP_NOTIFY_UNREGISTER,
						   (void *)contig_start,
						   contig_end - contig_start + VALUE_2MB);
			}
			contig_end = UINT64_MAX;
			continue;
		}

		for (; idx_1gb < UINT64_MAX; idx_1gb--) {
			if ((map_1gb->map[idx_1gb].translation_2mb & REG_MAP_REGISTERED) &&
			    (contig_end == UINT64_MAX || (map_1gb->map[idx_1gb].translation_2mb & REG_MAP_NOTIFY_START) == 0)) {
				/* Rebuild the virtual address from the indexes */
				uint64_t vaddr = (idx_256tb << SHIFT_1GB) | (idx_1gb << SHIFT_2MB);

				if (contig_end == UINT64_MAX) {
					contig_end = vaddr;
				}
				contig_start = vaddr;
			} else {
				if (contig_end != UINT64_MAX) {
					/* End of of a virtually contiguous range */
					map->ops.notify_cb(map->cb_ctx, map,
							   SPDK_MEM_MAP_NOTIFY_UNREGISTER,
							   (void *)contig_start,
							   contig_end - contig_start + VALUE_2MB);
					idx_1gb++;
				}
				contig_end = UINT64_MAX;
			}
		}
		idx_1gb = sizeof(map_1gb->map) / sizeof(map_1gb->map[0]) - 1;
	}

	pthread_mutex_unlock(&g_mem_reg_map->mutex);
	return rc;
}

struct spdk_mem_map *
spdk_mem_map_alloc(uint64_t default_translation, const struct spdk_mem_map_ops *ops, void *cb_ctx)
{
	struct spdk_mem_map *map;
	int rc;

	map = calloc(1, sizeof(*map));
	if (map == NULL) {
		return NULL;
	}

	if (pthread_mutex_init(&map->mutex, NULL)) {
		free(map);
		return NULL;
	}

	map->default_translation = default_translation;
	map->cb_ctx = cb_ctx;
	if (ops) {
		map->ops = *ops;
	}

	if (ops && ops->notify_cb) {
		pthread_mutex_lock(&g_spdk_mem_map_mutex);
		rc = mem_map_notify_walk(map, SPDK_MEM_MAP_NOTIFY_REGISTER);
		if (rc != 0) {
			pthread_mutex_unlock(&g_spdk_mem_map_mutex);
			DEBUG_PRINT("Initial mem_map notify failed\n");
			pthread_mutex_destroy(&map->mutex);
			free(map);
			return NULL;
		}
		TAILQ_INSERT_TAIL(&g_spdk_mem_maps, map, tailq);
		pthread_mutex_unlock(&g_spdk_mem_map_mutex);
	}

	return map;
}

void
spdk_mem_map_free(struct spdk_mem_map **pmap)
{
	struct spdk_mem_map *map;
	size_t i;

	if (!pmap) {
		return;
	}

	map = *pmap;

	if (!map) {
		return;
	}

	if (map->ops.notify_cb) {
		pthread_mutex_lock(&g_spdk_mem_map_mutex);
		mem_map_notify_walk(map, SPDK_MEM_MAP_NOTIFY_UNREGISTER);
		TAILQ_REMOVE(&g_spdk_mem_maps, map, tailq);
		pthread_mutex_unlock(&g_spdk_mem_map_mutex);
	}

	for (i = 0; i < sizeof(map->map_256tb.map) / sizeof(map->map_256tb.map[0]); i++) {
		free(map->map_256tb.map[i]);
	}

	pthread_mutex_destroy(&map->mutex);

	free(map);
	*pmap = NULL;
}

int
spdk_mem_register(void *vaddr, size_t len)
{
	struct spdk_mem_map *map;
	int rc;
	void *seg_vaddr;
	size_t seg_len;
	uint64_t reg;

	if ((uintptr_t)vaddr & ~MASK_256TB) {
		DEBUG_PRINT("invalid usermode virtual address %p\n", vaddr);
		return -EINVAL;
	}

	if (((uintptr_t)vaddr & MASK_2MB) || (len & MASK_2MB)) {
		DEBUG_PRINT("invalid %s parameters, vaddr=%p len=%ju\n",
			    __func__, vaddr, len);
		return -EINVAL;
	}

	if (len == 0) {
		return 0;
	}

	pthread_mutex_lock(&g_spdk_mem_map_mutex);

	seg_vaddr = vaddr;
	seg_len = len;
	while (seg_len > 0) {
		reg = spdk_mem_map_translate(g_mem_reg_map, (uint64_t)seg_vaddr, NULL);
		if (reg & REG_MAP_REGISTERED) {
			pthread_mutex_unlock(&g_spdk_mem_map_mutex);
			return -EBUSY;
		}
		seg_vaddr += VALUE_2MB;
		seg_len -= VALUE_2MB;
	}

	seg_vaddr = vaddr;
	seg_len = 0;
	while (len > 0) {
		spdk_mem_map_set_translation(g_mem_reg_map, (uint64_t)vaddr, VALUE_2MB,
					     seg_len == 0 ? REG_MAP_REGISTERED | REG_MAP_NOTIFY_START : REG_MAP_REGISTERED);
		seg_len += VALUE_2MB;
		vaddr += VALUE_2MB;
		len -= VALUE_2MB;
	}

	TAILQ_FOREACH(map, &g_spdk_mem_maps, tailq) {
		rc = map->ops.notify_cb(map->cb_ctx, map, SPDK_MEM_MAP_NOTIFY_REGISTER, seg_vaddr, seg_len);
		if (rc != 0) {
			pthread_mutex_unlock(&g_spdk_mem_map_mutex);
			return rc;
		}
	}

	pthread_mutex_unlock(&g_spdk_mem_map_mutex);
	return 0;
}

int
spdk_mem_unregister(void *vaddr, size_t len)
{
	struct spdk_mem_map *map;
	int rc;
	void *seg_vaddr;
	size_t seg_len;
	uint64_t reg, newreg;

	if ((uintptr_t)vaddr & ~MASK_256TB) {
		DEBUG_PRINT("invalid usermode virtual address %p\n", vaddr);
		return -EINVAL;
	}

	if (((uintptr_t)vaddr & MASK_2MB) || (len & MASK_2MB)) {
		DEBUG_PRINT("invalid %s parameters, vaddr=%p len=%ju\n",
			    __func__, vaddr, len);
		return -EINVAL;
	}

	pthread_mutex_lock(&g_spdk_mem_map_mutex);

	/* The first page must be a start of a region. Also check if it's
	 * registered to make sure we don't return -ERANGE for non-registered
	 * regions.
	 */
	reg = spdk_mem_map_translate(g_mem_reg_map, (uint64_t)vaddr, NULL);
	if ((reg & REG_MAP_REGISTERED) && (reg & REG_MAP_NOTIFY_START) == 0) {
		pthread_mutex_unlock(&g_spdk_mem_map_mutex);
		return -ERANGE;
	}

	seg_vaddr = vaddr;
	seg_len = len;
	while (seg_len > 0) {
		reg = spdk_mem_map_translate(g_mem_reg_map, (uint64_t)seg_vaddr, NULL);
		if ((reg & REG_MAP_REGISTERED) == 0) {
			pthread_mutex_unlock(&g_spdk_mem_map_mutex);
			return -EINVAL;
		}
		seg_vaddr += VALUE_2MB;
		seg_len -= VALUE_2MB;
	}

	newreg = spdk_mem_map_translate(g_mem_reg_map, (uint64_t)seg_vaddr, NULL);
	/* If the next page is registered, it must be a start of a region as well,
	 * otherwise we'd be unregistering only a part of a region.
	 */
	if ((newreg & REG_MAP_NOTIFY_START) == 0 && (newreg & REG_MAP_REGISTERED)) {
		pthread_mutex_unlock(&g_spdk_mem_map_mutex);
		return -ERANGE;
	}
	seg_vaddr = vaddr;
	seg_len = 0;

	while (len > 0) {
		reg = spdk_mem_map_translate(g_mem_reg_map, (uint64_t)vaddr, NULL);
		spdk_mem_map_set_translation(g_mem_reg_map, (uint64_t)vaddr, VALUE_2MB, 0);

		if (seg_len > 0 && (reg & REG_MAP_NOTIFY_START)) {
			TAILQ_FOREACH_REVERSE(map, &g_spdk_mem_maps, spdk_mem_map_head, tailq) {
				rc = map->ops.notify_cb(map->cb_ctx, map, SPDK_MEM_MAP_NOTIFY_UNREGISTER, seg_vaddr, seg_len);
				if (rc != 0) {
					pthread_mutex_unlock(&g_spdk_mem_map_mutex);
					return rc;
				}
			}

			seg_vaddr = vaddr;
			seg_len = VALUE_2MB;
		} else {
			seg_len += VALUE_2MB;
		}

		vaddr += VALUE_2MB;
		len -= VALUE_2MB;
	}

	if (seg_len > 0) {
		TAILQ_FOREACH_REVERSE(map, &g_spdk_mem_maps, spdk_mem_map_head, tailq) {
			rc = map->ops.notify_cb(map->cb_ctx, map, SPDK_MEM_MAP_NOTIFY_UNREGISTER, seg_vaddr, seg_len);
			if (rc != 0) {
				pthread_mutex_unlock(&g_spdk_mem_map_mutex);
				return rc;
			}
		}
	}

	pthread_mutex_unlock(&g_spdk_mem_map_mutex);
	return 0;
}

int
spdk_mem_reserve(void *vaddr, size_t len)
{
	struct spdk_mem_map *map;
	void *seg_vaddr;
	size_t seg_len;
	uint64_t reg;

	if ((uintptr_t)vaddr & ~MASK_256TB) {
		DEBUG_PRINT("invalid usermode virtual address %p\n", vaddr);
		return -EINVAL;
	}

	if (((uintptr_t)vaddr & MASK_2MB) || (len & MASK_2MB)) {
		DEBUG_PRINT("invalid %s parameters, vaddr=%p len=%ju\n",
			    __func__, vaddr, len);
		return -EINVAL;
	}

	if (len == 0) {
		return 0;
	}

	pthread_mutex_lock(&g_spdk_mem_map_mutex);

	/* Check if any part of this range is already registered */
	seg_vaddr = vaddr;
	seg_len = len;
	while (seg_len > 0) {
		reg = spdk_mem_map_translate(g_mem_reg_map, (uint64_t)seg_vaddr, NULL);
		if (reg & REG_MAP_REGISTERED) {
			pthread_mutex_unlock(&g_spdk_mem_map_mutex);
			return -EBUSY;
		}
		seg_vaddr += VALUE_2MB;
		seg_len -= VALUE_2MB;
	}

	/* Simply set the translation to the memory map's default. This allocates the space in the
	 * map but does not provide a valid translation. */
	spdk_mem_map_set_translation(g_mem_reg_map, (uint64_t)vaddr, len,
				     g_mem_reg_map->default_translation);

	TAILQ_FOREACH(map, &g_spdk_mem_maps, tailq) {
		spdk_mem_map_set_translation(map, (uint64_t)vaddr, len, map->default_translation);
	}

	pthread_mutex_unlock(&g_spdk_mem_map_mutex);
	return 0;
}

static struct map_1gb *
mem_map_get_map_1gb(struct spdk_mem_map *map, uint64_t vfn_2mb)
{
	struct map_1gb *map_1gb;
	uint64_t idx_256tb = MAP_256TB_IDX(vfn_2mb);
	size_t i;

	if (spdk_unlikely(idx_256tb >= SPDK_COUNTOF(map->map_256tb.map))) {
		return NULL;
	}

	map_1gb = map->map_256tb.map[idx_256tb];

	if (!map_1gb) {
		pthread_mutex_lock(&map->mutex);

		/* Recheck to make sure nobody else got the mutex first. */
		map_1gb = map->map_256tb.map[idx_256tb];
		if (!map_1gb) {
			map_1gb = malloc(sizeof(struct map_1gb));
			if (map_1gb) {
				/* initialize all entries to default translation */
				for (i = 0; i < SPDK_COUNTOF(map_1gb->map); i++) {
					map_1gb->map[i].translation_2mb = map->default_translation;
				}
				map->map_256tb.map[idx_256tb] = map_1gb;
			}
		}

		pthread_mutex_unlock(&map->mutex);

		if (!map_1gb) {
			DEBUG_PRINT("allocation failed\n");
			return NULL;
		}
	}

	return map_1gb;
}

int
spdk_mem_map_set_translation(struct spdk_mem_map *map, uint64_t vaddr, uint64_t size,
			     uint64_t translation)
{
	uint64_t vfn_2mb;
	struct map_1gb *map_1gb;
	uint64_t idx_1gb;
	struct map_2mb *map_2mb;

	if ((uintptr_t)vaddr & ~MASK_256TB) {
		DEBUG_PRINT("invalid usermode virtual address %" PRIu64 "\n", vaddr);
		return -EINVAL;
	}

	/* For now, only 2 MB-aligned registrations are supported */
	if (((uintptr_t)vaddr & MASK_2MB) || (size & MASK_2MB)) {
		DEBUG_PRINT("invalid %s parameters, vaddr=%" PRIu64 " len=%" PRIu64 "\n",
			    __func__, vaddr, size);
		return -EINVAL;
	}

	vfn_2mb = vaddr >> SHIFT_2MB;

	while (size) {
		map_1gb = mem_map_get_map_1gb(map, vfn_2mb);
		if (!map_1gb) {
			DEBUG_PRINT("could not get %p map\n", (void *)vaddr);
			return -ENOMEM;
		}

		idx_1gb = MAP_1GB_IDX(vfn_2mb);
		map_2mb = &map_1gb->map[idx_1gb];
		map_2mb->translation_2mb = translation;

		size -= VALUE_2MB;
		vfn_2mb++;
	}

	return 0;
}

int
spdk_mem_map_clear_translation(struct spdk_mem_map *map, uint64_t vaddr, uint64_t size)
{
	return spdk_mem_map_set_translation(map, vaddr, size, map->default_translation);
}

inline uint64_t
spdk_mem_map_translate(const struct spdk_mem_map *map, uint64_t vaddr, uint64_t *size)
{
	const struct map_1gb *map_1gb;
	const struct map_2mb *map_2mb;
	uint64_t idx_256tb;
	uint64_t idx_1gb;
	uint64_t vfn_2mb;
	uint64_t cur_size;
	uint64_t prev_translation;
	uint64_t orig_translation;

	if (spdk_unlikely(vaddr & ~MASK_256TB)) {
		DEBUG_PRINT("invalid usermode virtual address %p\n", (void *)vaddr);
		return map->default_translation;
	}

	vfn_2mb = vaddr >> SHIFT_2MB;
	idx_256tb = MAP_256TB_IDX(vfn_2mb);
	idx_1gb = MAP_1GB_IDX(vfn_2mb);

	map_1gb = map->map_256tb.map[idx_256tb];
	if (spdk_unlikely(!map_1gb)) {
		return map->default_translation;
	}

	cur_size = VALUE_2MB - _2MB_OFFSET(vaddr);
	map_2mb = &map_1gb->map[idx_1gb];
	if (size == NULL || map->ops.are_contiguous == NULL ||
	    map_2mb->translation_2mb == map->default_translation) {
		if (size != NULL) {
			*size = spdk_min(*size, cur_size);
		}
		return map_2mb->translation_2mb;
	}

	orig_translation = map_2mb->translation_2mb;
	prev_translation = orig_translation;
	while (cur_size < *size) {
		vfn_2mb++;
		idx_256tb = MAP_256TB_IDX(vfn_2mb);
		idx_1gb = MAP_1GB_IDX(vfn_2mb);

		map_1gb = map->map_256tb.map[idx_256tb];
		if (spdk_unlikely(!map_1gb)) {
			break;
		}

		map_2mb = &map_1gb->map[idx_1gb];
		if (!map->ops.are_contiguous(prev_translation, map_2mb->translation_2mb)) {
			break;
		}

		cur_size += VALUE_2MB;
		prev_translation = map_2mb->translation_2mb;
	}

	*size = spdk_min(*size, cur_size);
	return orig_translation;
}

static void
memory_hotplug_cb(enum rte_mem_event event_type,
		  const void *addr, size_t len, void *arg)
{
	if (event_type == RTE_MEM_EVENT_ALLOC) {
		spdk_mem_register((void *)addr, len);

		if (!spdk_env_dpdk_external_init()) {
			return;
		}

		/* When the user initialized DPDK separately, we can't
		 * be sure that --match-allocations RTE flag was specified.
		 * Without this flag, DPDK can free memory in different units
		 * than it was allocated. It doesn't work with things like RDMA MRs.
		 *
		 * For such cases, we mark segments so they aren't freed.
		 */
		while (len > 0) {
			struct rte_memseg *seg;

			seg = rte_mem_virt2memseg(addr, NULL);
			assert(seg != NULL);
			seg->flags |= RTE_MEMSEG_FLAG_DO_NOT_FREE;
			addr = (void *)((uintptr_t)addr + seg->hugepage_sz);
			len -= seg->hugepage_sz;
		}
	} else if (event_type == RTE_MEM_EVENT_FREE) {
		spdk_mem_unregister((void *)addr, len);
	}
}

static int
memory_iter_cb(const struct rte_memseg_list *msl,
	       const struct rte_memseg *ms, size_t len, void *arg)
{
	return spdk_mem_register(ms->addr, len);
}

int
mem_map_init(bool legacy_mem)
{
	g_legacy_mem = legacy_mem;

	g_mem_reg_map = spdk_mem_map_alloc(0, NULL, NULL);
	if (g_mem_reg_map == NULL) {
		DEBUG_PRINT("memory registration map allocation failed\n");
		return -ENOMEM;
	}

	/*
	 * Walk all DPDK memory segments and register them
	 * with the main memory map
	 */
	rte_mem_event_callback_register("spdk", memory_hotplug_cb, NULL);
	rte_memseg_contig_walk(memory_iter_cb, NULL);
	return 0;
}

bool
spdk_iommu_is_enabled(void)
{
#if VFIO_ENABLED
	return g_vfio.enabled && !g_vfio.noiommu_enabled;
#else
	return false;
#endif
}

struct spdk_vtophys_pci_device {
	struct rte_pci_device *pci_device;
	TAILQ_ENTRY(spdk_vtophys_pci_device) tailq;
};

static pthread_mutex_t g_vtophys_pci_devices_mutex = PTHREAD_MUTEX_INITIALIZER;
static TAILQ_HEAD(, spdk_vtophys_pci_device) g_vtophys_pci_devices =
	TAILQ_HEAD_INITIALIZER(g_vtophys_pci_devices);

static struct spdk_mem_map *g_vtophys_map;
static struct spdk_mem_map *g_phys_ref_map;

#if VFIO_ENABLED
static int
vtophys_iommu_map_dma(uint64_t vaddr, uint64_t iova, uint64_t size)
{
	struct spdk_vfio_dma_map *dma_map;
	uint64_t refcount;
	int ret;

	refcount = spdk_mem_map_translate(g_phys_ref_map, iova, NULL);
	assert(refcount < UINT64_MAX);
	if (refcount > 0) {
		spdk_mem_map_set_translation(g_phys_ref_map, iova, size, refcount + 1);
		return 0;
	}

	dma_map = calloc(1, sizeof(*dma_map));
	if (dma_map == NULL) {
		return -ENOMEM;
	}

	dma_map->map.argsz = sizeof(dma_map->map);
	dma_map->map.flags = VFIO_DMA_MAP_FLAG_READ | VFIO_DMA_MAP_FLAG_WRITE;
	dma_map->map.vaddr = vaddr;
	dma_map->map.iova = iova;
	dma_map->map.size = size;

	pthread_mutex_lock(&g_vfio.mutex);
	if (g_vfio.device_ref == 0) {
		/* VFIO requires at least one device (IOMMU group) to be added to
		 * a VFIO container before it is possible to perform any IOMMU
		 * operations on that container. This memory will be mapped once
		 * the first device (IOMMU group) is hotplugged.
		 *
		 * Since the vfio container is managed internally by DPDK, it is
		 * also possible that some device is already in that container, but
		 * it's not managed by SPDK -  e.g. an NIC attached internally
		 * inside DPDK. We could map the memory straight away in such
		 * scenario, but there's no need to do it. DPDK devices clearly
		 * don't need our mappings and hence we defer the mapping
		 * unconditionally until the first SPDK-managed device is
		 * hotplugged.
		 */
		goto out_insert;
	}

	ret = ioctl(g_vfio.fd, VFIO_IOMMU_MAP_DMA, &dma_map->map);
	if (ret) {
		/* There are cases the vfio container doesn't have IOMMU group, it's safe for this case */
		SPDK_NOTICELOG("Cannot set up DMA mapping, error %d, ignored\n", errno);
	}

out_insert:
	TAILQ_INSERT_TAIL(&g_vfio.maps, dma_map, tailq);
	pthread_mutex_unlock(&g_vfio.mutex);
	spdk_mem_map_set_translation(g_phys_ref_map, iova, size, refcount + 1);
	return 0;
}

static int
vtophys_iommu_unmap_dma(uint64_t iova, uint64_t size)
{
	struct spdk_vfio_dma_map *dma_map;
	uint64_t refcount;
	int ret;
	struct vfio_iommu_type1_dma_unmap unmap = {};

	pthread_mutex_lock(&g_vfio.mutex);
	TAILQ_FOREACH(dma_map, &g_vfio.maps, tailq) {
		if (dma_map->map.iova == iova) {
			break;
		}
	}

	if (dma_map == NULL) {
		DEBUG_PRINT("Cannot clear DMA mapping for IOVA %"PRIx64" - it's not mapped\n", iova);
		pthread_mutex_unlock(&g_vfio.mutex);
		return -ENXIO;
	}

	refcount = spdk_mem_map_translate(g_phys_ref_map, iova, NULL);
	assert(refcount < UINT64_MAX);
	if (refcount > 0) {
		spdk_mem_map_set_translation(g_phys_ref_map, iova, size, refcount - 1);
	}

	/* We still have outstanding references, don't clear it. */
	if (refcount > 1) {
		pthread_mutex_unlock(&g_vfio.mutex);
		return 0;
	}

	/** don't support partial or multiple-page unmap for now */
	assert(dma_map->map.size == size);

	if (g_vfio.device_ref == 0) {
		/* Memory is not mapped anymore, just remove it's references */
		goto out_remove;
	}

	unmap.argsz = sizeof(unmap);
	unmap.flags = 0;
	unmap.iova = dma_map->map.iova;
	unmap.size = dma_map->map.size;
	ret = ioctl(g_vfio.fd, VFIO_IOMMU_UNMAP_DMA, &unmap);
	if (ret) {
		SPDK_NOTICELOG("Cannot clear DMA mapping, error %d, ignored\n", errno);
	}

out_remove:
	TAILQ_REMOVE(&g_vfio.maps, dma_map, tailq);
	pthread_mutex_unlock(&g_vfio.mutex);
	free(dma_map);
	return 0;
}
#endif

static uint64_t
vtophys_get_paddr_memseg(uint64_t vaddr)
{
	uintptr_t paddr;
	struct rte_memseg *seg;

	seg = rte_mem_virt2memseg((void *)(uintptr_t)vaddr, NULL);
	if (seg != NULL) {
		paddr = seg->iova;
		if (paddr == RTE_BAD_IOVA) {
			return SPDK_VTOPHYS_ERROR;
		}
		paddr += (vaddr - (uintptr_t)seg->addr);
		return paddr;
	}

	return SPDK_VTOPHYS_ERROR;
}

/* Try to get the paddr from /proc/self/pagemap */
static uint64_t
vtophys_get_paddr_pagemap(uint64_t vaddr)
{
	uintptr_t paddr;

	/* Silence static analyzers */
	assert(vaddr != 0);
	paddr = rte_mem_virt2iova((void *)vaddr);
	if (paddr == RTE_BAD_IOVA) {
		/*
		 * The vaddr may be valid but doesn't have a backing page
		 * assigned yet.  Touch the page to ensure a backing page
		 * gets assigned, then try to translate again.
		 */
		rte_atomic64_read((rte_atomic64_t *)vaddr);
		paddr = rte_mem_virt2iova((void *)vaddr);
	}
	if (paddr == RTE_BAD_IOVA) {
		/* Unable to get to the physical address. */
		return SPDK_VTOPHYS_ERROR;
	}

	return paddr;
}

/* Try to get the paddr from pci devices */
static uint64_t
vtophys_get_paddr_pci(uint64_t vaddr)
{
	struct spdk_vtophys_pci_device *vtophys_dev;
	uintptr_t paddr;
	struct rte_pci_device	*dev;
	struct rte_mem_resource *res;
	unsigned r;

	pthread_mutex_lock(&g_vtophys_pci_devices_mutex);
	TAILQ_FOREACH(vtophys_dev, &g_vtophys_pci_devices, tailq) {
		dev = vtophys_dev->pci_device;

		for (r = 0; r < PCI_MAX_RESOURCE; r++) {
			res = &dev->mem_resource[r];
			if (res->phys_addr && vaddr >= (uint64_t)res->addr &&
			    vaddr < (uint64_t)res->addr + res->len) {
				paddr = res->phys_addr + (vaddr - (uint64_t)res->addr);
				DEBUG_PRINT("%s: %p -> %p\n", __func__, (void *)vaddr,
					    (void *)paddr);
				pthread_mutex_unlock(&g_vtophys_pci_devices_mutex);
				return paddr;
			}
		}
	}
	pthread_mutex_unlock(&g_vtophys_pci_devices_mutex);

	return  SPDK_VTOPHYS_ERROR;
}

static int
vtophys_notify(void *cb_ctx, struct spdk_mem_map *map,
	       enum spdk_mem_map_notify_action action,
	       void *vaddr, size_t len)
{
	int rc = 0, pci_phys = 0;
	uint64_t paddr;

	if ((uintptr_t)vaddr & ~MASK_256TB) {
		DEBUG_PRINT("invalid usermode virtual address %p\n", vaddr);
		return -EINVAL;
	}

	if (((uintptr_t)vaddr & MASK_2MB) || (len & MASK_2MB)) {
		DEBUG_PRINT("invalid parameters, vaddr=%p len=%ju\n",
			    vaddr, len);
		return -EINVAL;
	}

	/* Get the physical address from the DPDK memsegs */
	paddr = vtophys_get_paddr_memseg((uint64_t)vaddr);

	switch (action) {
	case SPDK_MEM_MAP_NOTIFY_REGISTER:
		if (paddr == SPDK_VTOPHYS_ERROR) {
			/* This is not an address that DPDK is managing. */
#if VFIO_ENABLED
			enum rte_iova_mode iova_mode;

			iova_mode = rte_eal_iova_mode();

			if (spdk_iommu_is_enabled() && iova_mode == RTE_IOVA_VA) {
				/* We'll use the virtual address as the iova to match DPDK. */
				paddr = (uint64_t)vaddr;
				rc = vtophys_iommu_map_dma((uint64_t)vaddr, paddr, len);
				if (rc) {
					return -EFAULT;
				}
				while (len > 0) {
					rc = spdk_mem_map_set_translation(map, (uint64_t)vaddr, VALUE_2MB, paddr);
					if (rc != 0) {
						return rc;
					}
					vaddr += VALUE_2MB;
					paddr += VALUE_2MB;
					len -= VALUE_2MB;
				}
			} else
#endif
			{
				/* Get the physical address from /proc/self/pagemap. */
				paddr = vtophys_get_paddr_pagemap((uint64_t)vaddr);
				if (paddr == SPDK_VTOPHYS_ERROR) {
					/* Get the physical address from PCI devices */
					paddr = vtophys_get_paddr_pci((uint64_t)vaddr);
					if (paddr == SPDK_VTOPHYS_ERROR) {
						DEBUG_PRINT("could not get phys addr for %p\n", vaddr);
						return -EFAULT;
					}
					/* The beginning of this address range points to a PCI resource,
					 * so the rest must point to a PCI resource as well.
					 */
					pci_phys = 1;
				}

				/* Get paddr for each 2MB chunk in this address range */
				while (len > 0) {
					/* Get the physical address from /proc/self/pagemap. */
					if (pci_phys) {
						paddr = vtophys_get_paddr_pci((uint64_t)vaddr);
					} else {
						paddr = vtophys_get_paddr_pagemap((uint64_t)vaddr);
					}

					if (paddr == SPDK_VTOPHYS_ERROR) {
						DEBUG_PRINT("could not get phys addr for %p\n", vaddr);
						return -EFAULT;
					}

					/* Since PCI paddr can break the 2MiB physical alignment skip this check for that. */
					if (!pci_phys && (paddr & MASK_2MB)) {
						DEBUG_PRINT("invalid paddr 0x%" PRIx64 " - must be 2MB aligned\n", paddr);
						return -EINVAL;
					}
#if VFIO_ENABLED
					/* If the IOMMU is on, but DPDK is using iova-mode=pa, we want to register this memory
					 * with the IOMMU using the physical address to match. */
					if (spdk_iommu_is_enabled()) {
						rc = vtophys_iommu_map_dma((uint64_t)vaddr, paddr, VALUE_2MB);
						if (rc) {
							DEBUG_PRINT("Unable to assign vaddr %p to paddr 0x%" PRIx64 "\n", vaddr, paddr);
							return -EFAULT;
						}
					}
#endif

					rc = spdk_mem_map_set_translation(map, (uint64_t)vaddr, VALUE_2MB, paddr);
					if (rc != 0) {
						return rc;
					}

					vaddr += VALUE_2MB;
					len -= VALUE_2MB;
				}
			}
		} else {
			/* This is an address managed by DPDK. Just setup the translations. */
			while (len > 0) {
				paddr = vtophys_get_paddr_memseg((uint64_t)vaddr);
				if (paddr == SPDK_VTOPHYS_ERROR) {
					DEBUG_PRINT("could not get phys addr for %p\n", vaddr);
					return -EFAULT;
				}

				rc = spdk_mem_map_set_translation(map, (uint64_t)vaddr, VALUE_2MB, paddr);
				if (rc != 0) {
					return rc;
				}

				vaddr += VALUE_2MB;
				len -= VALUE_2MB;
			}
		}

		break;
	case SPDK_MEM_MAP_NOTIFY_UNREGISTER:
#if VFIO_ENABLED
		if (paddr == SPDK_VTOPHYS_ERROR) {
			/*
			 * This is not an address that DPDK is managing. If vfio is enabled,
			 * we need to unmap the range from the IOMMU
			 */
			if (spdk_iommu_is_enabled()) {
				uint64_t buffer_len = len;
				uint8_t *va = vaddr;
				enum rte_iova_mode iova_mode;

				iova_mode = rte_eal_iova_mode();
				/*
				 * In virtual address mode, the region is contiguous and can be done in
				 * one unmap.
				 */
				if (iova_mode == RTE_IOVA_VA) {
					paddr = spdk_mem_map_translate(map, (uint64_t)va, &buffer_len);
					if (buffer_len != len || paddr != (uintptr_t)va) {
						DEBUG_PRINT("Unmapping %p with length %lu failed because "
							    "translation had address 0x%" PRIx64 " and length %lu\n",
							    va, len, paddr, buffer_len);
						return -EINVAL;
					}
					rc = vtophys_iommu_unmap_dma(paddr, len);
					if (rc) {
						DEBUG_PRINT("Failed to iommu unmap paddr 0x%" PRIx64 "\n", paddr);
						return -EFAULT;
					}
				} else if (iova_mode == RTE_IOVA_PA) {
					/* Get paddr for each 2MB chunk in this address range */
					while (buffer_len > 0) {
						paddr = spdk_mem_map_translate(map, (uint64_t)va, NULL);

						if (paddr == SPDK_VTOPHYS_ERROR || buffer_len < VALUE_2MB) {
							DEBUG_PRINT("could not get phys addr for %p\n", va);
							return -EFAULT;
						}

						rc = vtophys_iommu_unmap_dma(paddr, VALUE_2MB);
						if (rc) {
							DEBUG_PRINT("Failed to iommu unmap paddr 0x%" PRIx64 "\n", paddr);
							return -EFAULT;
						}

						va += VALUE_2MB;
						buffer_len -= VALUE_2MB;
					}
				}
			}
		}
#endif
		while (len > 0) {
			rc = spdk_mem_map_clear_translation(map, (uint64_t)vaddr, VALUE_2MB);
			if (rc != 0) {
				return rc;
			}

			vaddr += VALUE_2MB;
			len -= VALUE_2MB;
		}

		break;
	default:
		SPDK_UNREACHABLE();
	}

	return rc;
}

static int
vtophys_check_contiguous_entries(uint64_t paddr1, uint64_t paddr2)
{
	/* This function is always called with paddrs for two subsequent
	 * 2MB chunks in virtual address space, so those chunks will be only
	 * physically contiguous if the physical addresses are 2MB apart
	 * from each other as well.
	 */
	return (paddr2 - paddr1 == VALUE_2MB);
}

#if VFIO_ENABLED

static bool
vfio_enabled(void)
{
	return rte_vfio_is_enabled("vfio_pci");
}

/* Check if IOMMU is enabled on the system */
static bool
has_iommu_groups(void)
{
	int count = 0;
	DIR *dir = opendir("/sys/kernel/iommu_groups");

	if (dir == NULL) {
		return false;
	}

	while (count < 3 && readdir(dir) != NULL) {
		count++;
	}

	closedir(dir);
	/* there will always be ./ and ../ entries */
	return count > 2;
}

static bool
vfio_noiommu_enabled(void)
{
	return rte_vfio_noiommu_is_enabled();
}

static void
vtophys_iommu_device_event(const char *device_name,
			   enum rte_dev_event_type event,
			   void *cb_arg)
{
	struct rte_dev_iterator dev_iter;
	struct rte_device *dev;

	pthread_mutex_lock(&g_vfio.mutex);

	switch (event) {
	default:
	case RTE_DEV_EVENT_ADD:
		RTE_DEV_FOREACH(dev, "bus=pci", &dev_iter) {
			if (strcmp(dev->name, device_name) == 0) {
				struct rte_pci_device *pci_dev = RTE_DEV_TO_PCI(dev);
#if RTE_VERSION < RTE_VERSION_NUM(20, 11, 0, 0)
				if (pci_dev->kdrv == RTE_KDRV_VFIO) {
#else
				if (pci_dev->kdrv == RTE_PCI_KDRV_VFIO) {
#endif
					/* This is a new PCI device using vfio */
					g_vfio.device_ref++;
				}
				break;
			}
		}

		if (g_vfio.device_ref == 1) {
			struct spdk_vfio_dma_map *dma_map;
			int ret;

			/* This is the first device registered. This means that the first
			 * IOMMU group might have been just been added to the DPDK vfio container.
			 * From this point it is certain that the memory can be mapped now.
			 */
			TAILQ_FOREACH(dma_map, &g_vfio.maps, tailq) {
				ret = ioctl(g_vfio.fd, VFIO_IOMMU_MAP_DMA, &dma_map->map);
				if (ret) {
					DEBUG_PRINT("Cannot update DMA mapping, error %d\n", errno);
					break;
				}
			}
		}
		break;
	case RTE_DEV_EVENT_REMOVE:
		RTE_DEV_FOREACH(dev, "bus=pci", &dev_iter) {
			if (strcmp(dev->name, device_name) == 0) {
				struct rte_pci_device *pci_dev = RTE_DEV_TO_PCI(dev);
#if RTE_VERSION < RTE_VERSION_NUM(20, 11, 0, 0)
				if (pci_dev->kdrv == RTE_KDRV_VFIO) {
#else
				if (pci_dev->kdrv == RTE_PCI_KDRV_VFIO) {
#endif
					/* This is a PCI device using vfio */
					g_vfio.device_ref--;
				}
				break;
			}
		}

		if (g_vfio.device_ref == 0) {
			struct spdk_vfio_dma_map *dma_map;
			int ret;

			/* If DPDK doesn't have any additional devices using it's vfio container,
			 * all the mappings will be automatically removed by the Linux vfio driver.
			 * We unmap the memory manually to be able to easily re-map it later regardless
			 * of other, external factors.
			 */
			TAILQ_FOREACH(dma_map, &g_vfio.maps, tailq) {
				struct vfio_iommu_type1_dma_unmap unmap = {};
				unmap.argsz = sizeof(unmap);
				unmap.flags = 0;
				unmap.iova = dma_map->map.iova;
				unmap.size = dma_map->map.size;
				ret = ioctl(g_vfio.fd, VFIO_IOMMU_UNMAP_DMA, &unmap);
				if (ret) {
					DEBUG_PRINT("Cannot unmap DMA memory, error %d\n", errno);
					break;
				}
			}
		}
		break;
	}

	pthread_mutex_unlock(&g_vfio.mutex);
}

static void
vtophys_iommu_init(void)
{
	char proc_fd_path[PATH_MAX + 1];
	char link_path[PATH_MAX + 1];
	const char vfio_path[] = "/dev/vfio/vfio";
	DIR *dir;
	struct dirent *d;
	struct rte_dev_iterator dev_iter;
	struct rte_device *dev;
	int rc;

	if (!vfio_enabled()) {
		return;
	}

	if (vfio_noiommu_enabled()) {
		g_vfio.noiommu_enabled = true;
	} else if (!has_iommu_groups()) {
		return;
	}

	dir = opendir("/proc/self/fd");
	if (!dir) {
		DEBUG_PRINT("Failed to open /proc/self/fd (%d)\n", errno);
		return;
	}

	while ((d = readdir(dir)) != NULL) {
		if (d->d_type != DT_LNK) {
			continue;
		}

		snprintf(proc_fd_path, sizeof(proc_fd_path), "/proc/self/fd/%s", d->d_name);
		if (readlink(proc_fd_path, link_path, sizeof(link_path)) != (sizeof(vfio_path) - 1)) {
			continue;
		}

		if (memcmp(link_path, vfio_path, sizeof(vfio_path) - 1) == 0) {
			sscanf(d->d_name, "%d", &g_vfio.fd);
			break;
		}
	}

	closedir(dir);

	if (g_vfio.fd < 0) {
		DEBUG_PRINT("Failed to discover DPDK VFIO container fd.\n");
		return;
	}

	/* If the IOMMU is enabled, we need to track whether there are any devices present because
	 * it's only valid to perform vfio IOCTLs to the containers when there is at least
	 * one device. The device may be a DPDK device that SPDK doesn't otherwise know about, but
	 * that's ok.
	 */
	RTE_DEV_FOREACH(dev, "bus=pci", &dev_iter) {
		struct rte_pci_device *pci_dev = RTE_DEV_TO_PCI(dev);

#if RTE_VERSION < RTE_VERSION_NUM(20, 11, 0, 0)
		if (pci_dev->kdrv == RTE_KDRV_VFIO) {
#else
		if (pci_dev->kdrv == RTE_PCI_KDRV_VFIO) {
#endif
			/* This is a PCI device using vfio */
			g_vfio.device_ref++;
		}
	}

	if (spdk_process_is_primary()) {
		rc = rte_dev_event_callback_register(NULL, vtophys_iommu_device_event, NULL);
		if (rc) {
			DEBUG_PRINT("Failed to register device event callback\n");
			return;
		}
		rc = rte_dev_event_monitor_start();
		if (rc) {
			DEBUG_PRINT("Failed to start device event monitoring.\n");
			return;
		}
	}

	g_vfio.enabled = true;

	return;
}

static void
vtophys_iommu_fini(void)
{
	if (spdk_process_is_primary()) {
		rte_dev_event_callback_unregister(NULL, vtophys_iommu_device_event, NULL);
		rte_dev_event_monitor_stop();
	}
}

#endif

void
vtophys_pci_device_added(struct rte_pci_device *pci_device)
{
	struct spdk_vtophys_pci_device *vtophys_dev;

	pthread_mutex_lock(&g_vtophys_pci_devices_mutex);

	vtophys_dev = calloc(1, sizeof(*vtophys_dev));
	if (vtophys_dev) {
		vtophys_dev->pci_device = pci_device;
		TAILQ_INSERT_TAIL(&g_vtophys_pci_devices, vtophys_dev, tailq);
	} else {
		DEBUG_PRINT("Memory allocation error\n");
	}
	pthread_mutex_unlock(&g_vtophys_pci_devices_mutex);
}

void
vtophys_pci_device_removed(struct rte_pci_device *pci_device)
{
	struct spdk_vtophys_pci_device *vtophys_dev;

	pthread_mutex_lock(&g_vtophys_pci_devices_mutex);
	TAILQ_FOREACH(vtophys_dev, &g_vtophys_pci_devices, tailq) {
		if (vtophys_dev->pci_device == pci_device) {
			TAILQ_REMOVE(&g_vtophys_pci_devices, vtophys_dev, tailq);
			free(vtophys_dev);
			break;
		}
	}
	pthread_mutex_unlock(&g_vtophys_pci_devices_mutex);
}

int
vtophys_init(void)
{
	const struct spdk_mem_map_ops vtophys_map_ops = {
		.notify_cb = vtophys_notify,
		.are_contiguous = vtophys_check_contiguous_entries,
	};

	const struct spdk_mem_map_ops phys_ref_map_ops = {
		.notify_cb = NULL,
		.are_contiguous = NULL,
	};

#if VFIO_ENABLED
	vtophys_iommu_init();
#endif

	g_phys_ref_map = spdk_mem_map_alloc(0, &phys_ref_map_ops, NULL);
	if (g_phys_ref_map == NULL) {
		DEBUG_PRINT("phys_ref map allocation failed.\n");
		return -ENOMEM;
	}

	g_vtophys_map = spdk_mem_map_alloc(SPDK_VTOPHYS_ERROR, &vtophys_map_ops, NULL);
	if (g_vtophys_map == NULL) {
		DEBUG_PRINT("vtophys map allocation failed\n");
		spdk_mem_map_free(&g_phys_ref_map);
		return -ENOMEM;
	}
	return 0;
}

void
vtophys_fini(void)
{
#if VFIO_ENABLED
	vtophys_iommu_fini();
#endif
}

uint64_t
spdk_vtophys(const void *buf, uint64_t *size)
{
	uint64_t vaddr, paddr_2mb;

	vaddr = (uint64_t)buf;
	paddr_2mb = spdk_mem_map_translate(g_vtophys_map, vaddr, size);

	/*
	 * SPDK_VTOPHYS_ERROR has all bits set, so if the lookup returned SPDK_VTOPHYS_ERROR,
	 * we will still bitwise-or it with the buf offset below, but the result will still be
	 * SPDK_VTOPHYS_ERROR. However now that we do + rather than | (due to PCI vtophys being
	 * unaligned) we must now check the return value before addition.
	 */
	SPDK_STATIC_ASSERT(SPDK_VTOPHYS_ERROR == UINT64_C(-1), "SPDK_VTOPHYS_ERROR should be all 1s");
	if (paddr_2mb == SPDK_VTOPHYS_ERROR) {
		return SPDK_VTOPHYS_ERROR;
	} else {
		return paddr_2mb + (vaddr & MASK_2MB);
	}
}

int
spdk_mem_get_fd_and_offset(void *vaddr, uint64_t *offset)
{
	struct rte_memseg *seg;
	int ret, fd;

	seg = rte_mem_virt2memseg(vaddr, NULL);
	if (!seg) {
		SPDK_ERRLOG("memory %p doesn't exist\n", vaddr);
		return -ENOENT;
	}

	fd = rte_memseg_get_fd_thread_unsafe(seg);
	if (fd < 0) {
		return fd;
	}

	ret = rte_memseg_get_fd_offset_thread_unsafe(seg, offset);
	if (ret < 0) {
		return ret;
	}

	return fd;
}
