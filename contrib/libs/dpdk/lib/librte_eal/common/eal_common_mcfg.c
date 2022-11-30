#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2019 Intel Corporation
 */

#include <rte_eal_memconfig.h>
#include <rte_version.h>

#include "eal_internal_cfg.h"
#include "eal_memcfg.h"
#include "eal_private.h"

void
eal_mcfg_complete(void)
{
	struct rte_config *cfg = rte_eal_get_configuration();
	struct rte_mem_config *mcfg = cfg->mem_config;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	/* ALL shared mem_config related INIT DONE */
	if (cfg->process_type == RTE_PROC_PRIMARY)
		mcfg->magic = RTE_MAGIC;

	internal_conf->init_complete = 1;
}

void
eal_mcfg_wait_complete(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;

	/* wait until shared mem_config finish initialising */
	while (mcfg->magic != RTE_MAGIC)
		rte_pause();
}

int
eal_mcfg_check_version(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;

	/* check if version from memconfig matches compiled in macro */
	if (mcfg->version != RTE_VERSION)
		return -1;

	return 0;
}

void
eal_mcfg_update_internal(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	struct internal_config *internal_conf =
		eal_get_internal_configuration();

	internal_conf->legacy_mem = mcfg->legacy_mem;
	internal_conf->single_file_segments = mcfg->single_file_segments;
}

void
eal_mcfg_update_from_internal(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	const struct internal_config *internal_conf =
		eal_get_internal_configuration();

	mcfg->legacy_mem = internal_conf->legacy_mem;
	mcfg->single_file_segments = internal_conf->single_file_segments;
	/* record current DPDK version */
	mcfg->version = RTE_VERSION;
}

void
rte_mcfg_mem_read_lock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_read_lock(&mcfg->memory_hotplug_lock);
}

void
rte_mcfg_mem_read_unlock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_read_unlock(&mcfg->memory_hotplug_lock);
}

void
rte_mcfg_mem_write_lock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_write_lock(&mcfg->memory_hotplug_lock);
}

void
rte_mcfg_mem_write_unlock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_write_unlock(&mcfg->memory_hotplug_lock);
}

void
rte_mcfg_tailq_read_lock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_read_lock(&mcfg->qlock);
}

void
rte_mcfg_tailq_read_unlock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_read_unlock(&mcfg->qlock);
}

void
rte_mcfg_tailq_write_lock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_write_lock(&mcfg->qlock);
}

void
rte_mcfg_tailq_write_unlock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_write_unlock(&mcfg->qlock);
}

void
rte_mcfg_mempool_read_lock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_read_lock(&mcfg->mplock);
}

void
rte_mcfg_mempool_read_unlock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_read_unlock(&mcfg->mplock);
}

void
rte_mcfg_mempool_write_lock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_write_lock(&mcfg->mplock);
}

void
rte_mcfg_mempool_write_unlock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_rwlock_write_unlock(&mcfg->mplock);
}

void
rte_mcfg_timer_lock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_spinlock_lock(&mcfg->tlock);
}

void
rte_mcfg_timer_unlock(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	rte_spinlock_unlock(&mcfg->tlock);
}

bool
rte_mcfg_get_single_file_segments(void)
{
	struct rte_mem_config *mcfg = rte_eal_get_configuration()->mem_config;
	return (bool)mcfg->single_file_segments;
}
