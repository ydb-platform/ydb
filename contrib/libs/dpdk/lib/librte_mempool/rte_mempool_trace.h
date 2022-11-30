/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(C) 2020 Marvell International Ltd.
 */

#ifndef _RTE_MEMPOOL_TRACE_H_
#define _RTE_MEMPOOL_TRACE_H_

/**
 * @file
 *
 * APIs for mempool trace support
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "rte_mempool.h"

#include <rte_memzone.h>
#include <rte_trace_point.h>

RTE_TRACE_POINT(
	rte_mempool_trace_create,
	RTE_TRACE_POINT_ARGS(const char *name, uint32_t nb_elts,
		uint32_t elt_size, uint32_t cache_size,
		uint32_t private_data_size, void *mp_init, void *mp_init_arg,
		void *obj_init, void *obj_init_arg, uint32_t flags,
		struct rte_mempool *mempool),
	rte_trace_point_emit_string(name);
	rte_trace_point_emit_u32(nb_elts);
	rte_trace_point_emit_u32(elt_size);
	rte_trace_point_emit_u32(cache_size);
	rte_trace_point_emit_u32(private_data_size);
	rte_trace_point_emit_ptr(mp_init);
	rte_trace_point_emit_ptr(mp_init_arg);
	rte_trace_point_emit_ptr(obj_init);
	rte_trace_point_emit_ptr(obj_init_arg);
	rte_trace_point_emit_u32(flags);
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_i32(mempool->ops_index);
)

RTE_TRACE_POINT(
	rte_mempool_trace_create_empty,
	RTE_TRACE_POINT_ARGS(const char *name, uint32_t nb_elts,
		uint32_t elt_size, uint32_t cache_size,
		uint32_t private_data_size, uint32_t flags,
		struct rte_mempool *mempool),
	rte_trace_point_emit_string(name);
	rte_trace_point_emit_u32(nb_elts);
	rte_trace_point_emit_u32(elt_size);
	rte_trace_point_emit_u32(cache_size);
	rte_trace_point_emit_u32(private_data_size);
	rte_trace_point_emit_u32(flags);
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_i32(mempool->ops_index);
)

RTE_TRACE_POINT(
	rte_mempool_trace_free,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
)

RTE_TRACE_POINT(
	rte_mempool_trace_populate_iova,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool, void *vaddr,
		rte_iova_t iova, size_t len, void *free_cb, void *opaque),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
	rte_trace_point_emit_ptr(vaddr);
	rte_trace_point_emit_u64(iova);
	rte_trace_point_emit_size_t(len);
	rte_trace_point_emit_ptr(free_cb);
	rte_trace_point_emit_ptr(opaque);
)

RTE_TRACE_POINT(
	rte_mempool_trace_populate_virt,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool, void *addr,
		size_t len, size_t pg_sz, void *free_cb, void *opaque),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
	rte_trace_point_emit_ptr(addr);
	rte_trace_point_emit_size_t(len);
	rte_trace_point_emit_size_t(pg_sz);
	rte_trace_point_emit_ptr(free_cb);
	rte_trace_point_emit_ptr(opaque);
)

RTE_TRACE_POINT(
	rte_mempool_trace_populate_default,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
)

RTE_TRACE_POINT(
	rte_mempool_trace_populate_anon,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
)

RTE_TRACE_POINT(
	rte_mempool_trace_cache_create,
	RTE_TRACE_POINT_ARGS(uint32_t size, int socket_id,
		struct rte_mempool_cache *cache),
	rte_trace_point_emit_u32(size);
	rte_trace_point_emit_i32(socket_id);
	rte_trace_point_emit_ptr(cache);
	rte_trace_point_emit_u32(cache->len);
	rte_trace_point_emit_u32(cache->flushthresh);
)

RTE_TRACE_POINT(
	rte_mempool_trace_cache_free,
	RTE_TRACE_POINT_ARGS(void *cache),
	rte_trace_point_emit_ptr(cache);
)

RTE_TRACE_POINT(
	rte_mempool_trace_get_page_size,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool, size_t pg_sz),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
	rte_trace_point_emit_size_t(pg_sz);
)

RTE_TRACE_POINT(
	rte_mempool_trace_ops_populate,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool, uint32_t max_objs,
		void *vaddr, uint64_t iova, size_t len, void *obj_cb,
		void *obj_cb_arg),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
	rte_trace_point_emit_u32(max_objs);
	rte_trace_point_emit_ptr(vaddr);
	rte_trace_point_emit_u64(iova);
	rte_trace_point_emit_size_t(len);
	rte_trace_point_emit_ptr(obj_cb);
	rte_trace_point_emit_ptr(obj_cb_arg);
)

RTE_TRACE_POINT(
	rte_mempool_trace_ops_alloc,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
)

RTE_TRACE_POINT(
	rte_mempool_trace_ops_free,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
)

RTE_TRACE_POINT(
	rte_mempool_trace_set_ops_byname,
	RTE_TRACE_POINT_ARGS(struct rte_mempool *mempool, const char *name,
		void *pool_config),
	rte_trace_point_emit_ptr(mempool);
	rte_trace_point_emit_string(mempool->name);
	rte_trace_point_emit_string(name);
	rte_trace_point_emit_ptr(pool_config);
)

#ifdef __cplusplus
}
#endif

#endif /* _RTE_MEMPOOL_TRACE_H_ */
