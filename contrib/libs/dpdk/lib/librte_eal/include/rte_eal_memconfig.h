/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_EAL_MEMCONFIG_H_
#define _RTE_EAL_MEMCONFIG_H_

#include <stdbool.h>

#include <rte_compat.h>

/**
 * @file
 *
 * This API allows access to EAL shared memory configuration through an API.
 */

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Lock the internal EAL shared memory configuration for shared access.
 */
void
rte_mcfg_mem_read_lock(void);

/**
 * Unlock the internal EAL shared memory configuration for shared access.
 */
void
rte_mcfg_mem_read_unlock(void);

/**
 * Lock the internal EAL shared memory configuration for exclusive access.
 */
void
rte_mcfg_mem_write_lock(void);

/**
 * Unlock the internal EAL shared memory configuration for exclusive access.
 */
void
rte_mcfg_mem_write_unlock(void);

/**
 * Lock the internal EAL TAILQ list for shared access.
 */
void
rte_mcfg_tailq_read_lock(void);

/**
 * Unlock the internal EAL TAILQ list for shared access.
 */
void
rte_mcfg_tailq_read_unlock(void);

/**
 * Lock the internal EAL TAILQ list for exclusive access.
 */
void
rte_mcfg_tailq_write_lock(void);

/**
 * Unlock the internal EAL TAILQ list for exclusive access.
 */
void
rte_mcfg_tailq_write_unlock(void);

/**
 * Lock the internal EAL Mempool list for shared access.
 */
void
rte_mcfg_mempool_read_lock(void);

/**
 * Unlock the internal EAL Mempool list for shared access.
 */
void
rte_mcfg_mempool_read_unlock(void);

/**
 * Lock the internal EAL Mempool list for exclusive access.
 */
void
rte_mcfg_mempool_write_lock(void);

/**
 * Unlock the internal EAL Mempool list for exclusive access.
 */
void
rte_mcfg_mempool_write_unlock(void);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Lock the internal EAL Timer Library lock for exclusive access.
 */
__rte_experimental
void
rte_mcfg_timer_lock(void);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Unlock the internal EAL Timer Library lock for exclusive access.
 */
__rte_experimental
void
rte_mcfg_timer_unlock(void);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * If true, pages are put in single files (per memseg list),
 * as opposed to creating a file per page.
 */
__rte_experimental
bool
rte_mcfg_get_single_file_segments(void);

#ifdef __cplusplus
}
#endif

#endif /*__RTE_EAL_MEMCONFIG_H_*/
