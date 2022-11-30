/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2015 Intel Corporation
 */

#ifndef _RTE_RWLOCK_X86_64_H_
#define _RTE_RWLOCK_X86_64_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "generic/rte_rwlock.h"
#include "rte_spinlock.h"

static inline void
rte_rwlock_read_lock_tm(rte_rwlock_t *rwl)
{
	if (likely(rte_try_tm(&rwl->cnt)))
		return;
	rte_rwlock_read_lock(rwl);
}

static inline void
rte_rwlock_read_unlock_tm(rte_rwlock_t *rwl)
{
	if (unlikely(rwl->cnt))
		rte_rwlock_read_unlock(rwl);
	else
		rte_xend();
}

static inline void
rte_rwlock_write_lock_tm(rte_rwlock_t *rwl)
{
	if (likely(rte_try_tm(&rwl->cnt)))
		return;
	rte_rwlock_write_lock(rwl);
}

static inline void
rte_rwlock_write_unlock_tm(rte_rwlock_t *rwl)
{
	if (unlikely(rwl->cnt))
		rte_rwlock_write_unlock(rwl);
	else
		rte_xend();
}

#ifdef __cplusplus
}
#endif

#endif /* _RTE_RWLOCK_X86_64_H_ */
