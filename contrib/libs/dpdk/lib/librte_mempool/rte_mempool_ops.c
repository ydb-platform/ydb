#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2016 Intel Corporation.
 * Copyright(c) 2016 6WIND S.A.
 */

#include <stdio.h>
#include <string.h>

#include <rte_string_fns.h>
#include <rte_mempool.h>
#include <rte_errno.h>
#include <rte_dev.h>

#include "rte_mempool_trace.h"

/* indirect jump table to support external memory pools. */
struct rte_mempool_ops_table rte_mempool_ops_table = {
	.sl =  RTE_SPINLOCK_INITIALIZER,
	.num_ops = 0
};

/* add a new ops struct in rte_mempool_ops_table, return its index. */
int
rte_mempool_register_ops(const struct rte_mempool_ops *h)
{
	struct rte_mempool_ops *ops;
	int16_t ops_index;

	rte_spinlock_lock(&rte_mempool_ops_table.sl);

	if (rte_mempool_ops_table.num_ops >=
			RTE_MEMPOOL_MAX_OPS_IDX) {
		rte_spinlock_unlock(&rte_mempool_ops_table.sl);
		RTE_LOG(ERR, MEMPOOL,
			"Maximum number of mempool ops structs exceeded\n");
		return -ENOSPC;
	}

	if (h->alloc == NULL || h->enqueue == NULL ||
			h->dequeue == NULL || h->get_count == NULL) {
		rte_spinlock_unlock(&rte_mempool_ops_table.sl);
		RTE_LOG(ERR, MEMPOOL,
			"Missing callback while registering mempool ops\n");
		return -EINVAL;
	}

	if (strlen(h->name) >= sizeof(ops->name) - 1) {
		rte_spinlock_unlock(&rte_mempool_ops_table.sl);
		RTE_LOG(DEBUG, EAL, "%s(): mempool_ops <%s>: name too long\n",
				__func__, h->name);
		rte_errno = EEXIST;
		return -EEXIST;
	}

	ops_index = rte_mempool_ops_table.num_ops++;
	ops = &rte_mempool_ops_table.ops[ops_index];
	strlcpy(ops->name, h->name, sizeof(ops->name));
	ops->alloc = h->alloc;
	ops->free = h->free;
	ops->enqueue = h->enqueue;
	ops->dequeue = h->dequeue;
	ops->get_count = h->get_count;
	ops->calc_mem_size = h->calc_mem_size;
	ops->populate = h->populate;
	ops->get_info = h->get_info;
	ops->dequeue_contig_blocks = h->dequeue_contig_blocks;

	rte_spinlock_unlock(&rte_mempool_ops_table.sl);

	return ops_index;
}

/* wrapper to allocate an external mempool's private (pool) data. */
int
rte_mempool_ops_alloc(struct rte_mempool *mp)
{
	struct rte_mempool_ops *ops;

	rte_mempool_trace_ops_alloc(mp);
	ops = rte_mempool_get_ops(mp->ops_index);
	return ops->alloc(mp);
}

/* wrapper to free an external pool ops. */
void
rte_mempool_ops_free(struct rte_mempool *mp)
{
	struct rte_mempool_ops *ops;

	rte_mempool_trace_ops_free(mp);
	ops = rte_mempool_get_ops(mp->ops_index);
	if (ops->free == NULL)
		return;
	ops->free(mp);
}

/* wrapper to get available objects in an external mempool. */
unsigned int
rte_mempool_ops_get_count(const struct rte_mempool *mp)
{
	struct rte_mempool_ops *ops;

	ops = rte_mempool_get_ops(mp->ops_index);
	return ops->get_count(mp);
}

/* wrapper to calculate the memory size required to store given number
 * of objects
 */
ssize_t
rte_mempool_ops_calc_mem_size(const struct rte_mempool *mp,
				uint32_t obj_num, uint32_t pg_shift,
				size_t *min_chunk_size, size_t *align)
{
	struct rte_mempool_ops *ops;

	ops = rte_mempool_get_ops(mp->ops_index);

	if (ops->calc_mem_size == NULL)
		return rte_mempool_op_calc_mem_size_default(mp, obj_num,
				pg_shift, min_chunk_size, align);

	return ops->calc_mem_size(mp, obj_num, pg_shift, min_chunk_size, align);
}

/* wrapper to populate memory pool objects using provided memory chunk */
int
rte_mempool_ops_populate(struct rte_mempool *mp, unsigned int max_objs,
				void *vaddr, rte_iova_t iova, size_t len,
				rte_mempool_populate_obj_cb_t *obj_cb,
				void *obj_cb_arg)
{
	struct rte_mempool_ops *ops;

	ops = rte_mempool_get_ops(mp->ops_index);

	rte_mempool_trace_ops_populate(mp, max_objs, vaddr, iova, len, obj_cb,
		obj_cb_arg);
	if (ops->populate == NULL)
		return rte_mempool_op_populate_default(mp, max_objs, vaddr,
						       iova, len, obj_cb,
						       obj_cb_arg);

	return ops->populate(mp, max_objs, vaddr, iova, len, obj_cb,
			     obj_cb_arg);
}

/* wrapper to get additional mempool info */
int
rte_mempool_ops_get_info(const struct rte_mempool *mp,
			 struct rte_mempool_info *info)
{
	struct rte_mempool_ops *ops;

	ops = rte_mempool_get_ops(mp->ops_index);

	RTE_FUNC_PTR_OR_ERR_RET(ops->get_info, -ENOTSUP);
	return ops->get_info(mp, info);
}


/* sets mempool ops previously registered by rte_mempool_register_ops. */
int
rte_mempool_set_ops_byname(struct rte_mempool *mp, const char *name,
	void *pool_config)
{
	struct rte_mempool_ops *ops = NULL;
	unsigned i;

	/* too late, the mempool is already populated. */
	if (mp->flags & MEMPOOL_F_POOL_CREATED)
		return -EEXIST;

	for (i = 0; i < rte_mempool_ops_table.num_ops; i++) {
		if (!strcmp(name,
				rte_mempool_ops_table.ops[i].name)) {
			ops = &rte_mempool_ops_table.ops[i];
			break;
		}
	}

	if (ops == NULL)
		return -EINVAL;

	mp->ops_index = i;
	mp->pool_config = pool_config;
	rte_mempool_trace_set_ops_byname(mp, name, pool_config);
	return 0;
}
