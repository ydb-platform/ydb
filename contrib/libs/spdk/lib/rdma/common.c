#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
 *   Copyright (c) 2020, 2021 Mellanox Technologies LTD. All rights reserved.
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

#include <rdma/rdma_cma.h>

#include "spdk/log.h"
#include "spdk/env.h"
#include "spdk/string.h"
#include "spdk/likely.h"

#include "spdk_internal/rdma.h"
#include "spdk_internal/assert.h"

struct spdk_rdma_mem_map {
	struct spdk_mem_map		*map;
	struct ibv_pd			*pd;
	struct spdk_nvme_rdma_hooks	*hooks;
	uint32_t ref_count;
	LIST_ENTRY(spdk_rdma_mem_map) link;
};

static LIST_HEAD(, spdk_rdma_mem_map) g_rdma_mr_maps = LIST_HEAD_INITIALIZER(&g_rdma_mr_maps);
static pthread_mutex_t g_rdma_mr_maps_mutex = PTHREAD_MUTEX_INITIALIZER;

static int
rdma_mem_notify(void *cb_ctx, struct spdk_mem_map *map,
		enum spdk_mem_map_notify_action action,
		void *vaddr, size_t size)
{
	struct spdk_rdma_mem_map *rmap = cb_ctx;
	struct ibv_pd *pd = rmap->pd;
	struct ibv_mr *mr;
	int rc;

	switch (action) {
	case SPDK_MEM_MAP_NOTIFY_REGISTER:
		if (rmap->hooks && rmap->hooks->get_rkey) {
			rc = spdk_mem_map_set_translation(map, (uint64_t)vaddr, size, rmap->hooks->get_rkey(pd, vaddr,
							  size));
		} else {
			mr = ibv_reg_mr(pd, vaddr, size,
					IBV_ACCESS_LOCAL_WRITE |
					IBV_ACCESS_REMOTE_READ |
					IBV_ACCESS_REMOTE_WRITE);
			if (mr == NULL) {
				SPDK_ERRLOG("ibv_reg_mr() failed\n");
				return -1;
			} else {
				rc = spdk_mem_map_set_translation(map, (uint64_t)vaddr, size, (uint64_t)mr);
			}
		}
		break;
	case SPDK_MEM_MAP_NOTIFY_UNREGISTER:
		if (rmap->hooks == NULL || rmap->hooks->get_rkey == NULL) {
			mr = (struct ibv_mr *)spdk_mem_map_translate(map, (uint64_t)vaddr, NULL);
			if (mr) {
				ibv_dereg_mr(mr);
			}
		}
		rc = spdk_mem_map_clear_translation(map, (uint64_t)vaddr, size);
		break;
	default:
		SPDK_UNREACHABLE();
	}

	return rc;
}

static int
rdma_check_contiguous_entries(uint64_t addr_1, uint64_t addr_2)
{
	/* Two contiguous mappings will point to the same address which is the start of the RDMA MR. */
	return addr_1 == addr_2;
}

const struct spdk_mem_map_ops g_rdma_map_ops = {
	.notify_cb = rdma_mem_notify,
	.are_contiguous = rdma_check_contiguous_entries
};

static void
_rdma_free_mem_map(struct spdk_rdma_mem_map *map)
{
	assert(map);

	if (map->hooks) {
		spdk_free(map);
	} else {
		free(map);
	}
}

struct spdk_rdma_mem_map *
spdk_rdma_create_mem_map(struct ibv_pd *pd, struct spdk_nvme_rdma_hooks *hooks)
{
	struct spdk_rdma_mem_map *map;

	pthread_mutex_lock(&g_rdma_mr_maps_mutex);
	/* Look up existing mem map registration for this pd */
	LIST_FOREACH(map, &g_rdma_mr_maps, link) {
		if (map->pd == pd) {
			map->ref_count++;
			pthread_mutex_unlock(&g_rdma_mr_maps_mutex);
			return map;
		}
	}

	if (hooks) {
		map = spdk_zmalloc(sizeof(*map), 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
	} else {
		map = calloc(1, sizeof(*map));
	}
	if (!map) {
		pthread_mutex_unlock(&g_rdma_mr_maps_mutex);
		SPDK_ERRLOG("Memory allocation failed\n");
		return NULL;
	}
	map->pd = pd;
	map->ref_count = 1;
	map->hooks = hooks;
	map->map = spdk_mem_map_alloc(0, &g_rdma_map_ops, map);
	if (!map->map) {
		SPDK_ERRLOG("Unable to create memory map\n");
		_rdma_free_mem_map(map);
		pthread_mutex_unlock(&g_rdma_mr_maps_mutex);
		return NULL;
	}
	LIST_INSERT_HEAD(&g_rdma_mr_maps, map, link);

	pthread_mutex_unlock(&g_rdma_mr_maps_mutex);

	return map;
}

void
spdk_rdma_free_mem_map(struct spdk_rdma_mem_map **_map)
{
	struct spdk_rdma_mem_map *map;

	if (!_map) {
		return;
	}

	map = *_map;
	if (!map) {
		return;
	}
	*_map = NULL;

	pthread_mutex_lock(&g_rdma_mr_maps_mutex);
	assert(map->ref_count > 0);
	map->ref_count--;
	if (map->ref_count != 0) {
		pthread_mutex_unlock(&g_rdma_mr_maps_mutex);
		return;
	}

	LIST_REMOVE(map, link);
	pthread_mutex_unlock(&g_rdma_mr_maps_mutex);
	if (map->map) {
		spdk_mem_map_free(&map->map);
	}
	_rdma_free_mem_map(map);
}

int
spdk_rdma_get_translation(struct spdk_rdma_mem_map *map, void *address,
			  size_t length, struct spdk_rdma_memory_translation *translation)
{
	uint64_t real_length = length;

	assert(map);
	assert(address);
	assert(translation);

	if (map->hooks && map->hooks->get_rkey) {
		translation->translation_type = SPDK_RDMA_TRANSLATION_KEY;
		translation->mr_or_key.key = spdk_mem_map_translate(map->map, (uint64_t)address, &real_length);
	} else {
		translation->translation_type = SPDK_RDMA_TRANSLATION_MR;
		translation->mr_or_key.mr = (struct ibv_mr *)spdk_mem_map_translate(map->map, (uint64_t)address,
					    &real_length);
		if (spdk_unlikely(!translation->mr_or_key.mr)) {
			SPDK_ERRLOG("No translation for ptr %p, size %zu\n", address, length);
			return -EINVAL;
		}
	}

	assert(real_length >= length);

	return 0;
}

struct spdk_rdma_srq *
spdk_rdma_srq_create(struct spdk_rdma_srq_init_attr *init_attr)
{
	assert(init_attr);
	assert(init_attr->pd);

	struct spdk_rdma_srq *rdma_srq = calloc(1, sizeof(*rdma_srq));

	if (!rdma_srq) {
		SPDK_ERRLOG("Can't allocate memory for SRQ handle\n");
		return NULL;
	}

	if (init_attr->stats) {
		rdma_srq->stats = init_attr->stats;
		rdma_srq->shared_stats = true;
	} else {
		rdma_srq->stats = calloc(1, sizeof(*rdma_srq->stats));
		if (!rdma_srq->stats) {
			SPDK_ERRLOG("SRQ statistics memory allocation failed");
			free(rdma_srq);
			return NULL;
		}
	}

	rdma_srq->srq = ibv_create_srq(init_attr->pd, &init_attr->srq_init_attr);
	if (!rdma_srq->srq) {
		if (!init_attr->stats) {
			free(rdma_srq->stats);
		}
		SPDK_ERRLOG("Unable to create SRQ, errno %d (%s)\n", errno, spdk_strerror(errno));
		free(rdma_srq);
		return NULL;
	}

	return rdma_srq;
}

int
spdk_rdma_srq_destroy(struct spdk_rdma_srq *rdma_srq)
{
	int rc;

	if (!rdma_srq) {
		return 0;
	}

	assert(rdma_srq->srq);

	if (rdma_srq->recv_wrs.first != NULL) {
		SPDK_WARNLOG("Destroying RDMA SRQ with queued recv WRs\n");
	}

	rc = ibv_destroy_srq(rdma_srq->srq);
	if (rc) {
		SPDK_ERRLOG("SRQ destroy failed with %d\n", rc);
	}

	if (!rdma_srq->shared_stats) {
		free(rdma_srq->stats);
	}

	free(rdma_srq);

	return rc;
}

static inline bool
rdma_queue_recv_wrs(struct spdk_rdma_recv_wr_list *recv_wrs, struct ibv_recv_wr *first,
		    struct spdk_rdma_wr_stats *recv_stats)
{
	struct ibv_recv_wr *last;

	recv_stats->num_submitted_wrs++;
	last = first;
	while (last->next != NULL) {
		last = last->next;
		recv_stats->num_submitted_wrs++;
	}

	if (recv_wrs->first == NULL) {
		recv_wrs->first = first;
		recv_wrs->last = last;
		return true;
	} else {
		recv_wrs->last->next = first;
		recv_wrs->last = last;
		return false;
	}
}

bool
spdk_rdma_srq_queue_recv_wrs(struct spdk_rdma_srq *rdma_srq, struct ibv_recv_wr *first)
{
	assert(rdma_srq);
	assert(first);

	return rdma_queue_recv_wrs(&rdma_srq->recv_wrs, first, rdma_srq->stats);
}

int
spdk_rdma_srq_flush_recv_wrs(struct spdk_rdma_srq *rdma_srq, struct ibv_recv_wr **bad_wr)
{
	int rc;

	if (spdk_unlikely(rdma_srq->recv_wrs.first == NULL)) {
		return 0;
	}

	rc = ibv_post_srq_recv(rdma_srq->srq, rdma_srq->recv_wrs.first, bad_wr);

	rdma_srq->recv_wrs.first = NULL;
	rdma_srq->stats->doorbell_updates++;

	return rc;
}

bool
spdk_rdma_qp_queue_recv_wrs(struct spdk_rdma_qp *spdk_rdma_qp, struct ibv_recv_wr *first)
{
	assert(spdk_rdma_qp);
	assert(first);

	return rdma_queue_recv_wrs(&spdk_rdma_qp->recv_wrs, first, &spdk_rdma_qp->stats->recv);
}

int
spdk_rdma_qp_flush_recv_wrs(struct spdk_rdma_qp *spdk_rdma_qp, struct ibv_recv_wr **bad_wr)
{
	int rc;

	if (spdk_unlikely(spdk_rdma_qp->recv_wrs.first == NULL)) {
		return 0;
	}

	rc = ibv_post_recv(spdk_rdma_qp->qp, spdk_rdma_qp->recv_wrs.first, bad_wr);

	spdk_rdma_qp->recv_wrs.first = NULL;
	spdk_rdma_qp->stats->recv.doorbell_updates++;

	return rc;
}
