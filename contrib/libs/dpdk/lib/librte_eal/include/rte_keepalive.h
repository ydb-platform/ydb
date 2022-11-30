/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright 2015-2016 Intel Corporation.
 */

/**
 * @file rte_keepalive.h
 * DPDK RTE LCore Keepalive Monitor.
 *
 **/

#ifndef _KEEPALIVE_H_
#define _KEEPALIVE_H_

#include <rte_config.h>
#include <rte_memory.h>

#ifndef RTE_KEEPALIVE_MAXCORES
/**
 * Number of cores to track.
 * @note Must be larger than the highest core id. */
#define RTE_KEEPALIVE_MAXCORES RTE_MAX_LCORE
#endif

enum rte_keepalive_state {
	RTE_KA_STATE_UNUSED = 0,
	RTE_KA_STATE_ALIVE = 1,
	RTE_KA_STATE_MISSING = 4,
	RTE_KA_STATE_DEAD = 2,
	RTE_KA_STATE_GONE = 3,
	RTE_KA_STATE_DOZING = 5,
	RTE_KA_STATE_SLEEP = 6
};

/**
 * Keepalive failure callback.
 *
 *  Receives a data pointer passed to rte_keepalive_create() and the id of the
 *  failed core.
 *  @param data Data pointer passed to rte_keepalive_create()
 *  @param id_core ID of the core that has failed
 */
typedef void (*rte_keepalive_failure_callback_t)(
	void *data,
	const int id_core);

/**
 * Keepalive relay callback.
 *
 *  Receives a data pointer passed to rte_keepalive_register_relay_callback(),
 *  the id of the core for which state is to be forwarded, and details of the
 *  current core state.
 *  @param data Data pointer passed to rte_keepalive_register_relay_callback()
 *  @param id_core ID of the core for which state is being reported
 *  @param core_state The current state of the core
 *  @param Timestamp of when core was last seen alive
 */
typedef void (*rte_keepalive_relay_callback_t)(
	void *data,
	const int id_core,
	enum rte_keepalive_state core_state,
	uint64_t last_seen
	);

/**
 * Keepalive state structure.
 * @internal
 */
struct rte_keepalive;

/**
 * Initialise keepalive sub-system.
 * @param callback
 *   Function called upon detection of a dead core.
 * @param data
 *   Data pointer to be passed to function callback.
 * @return
 *   Keepalive structure success, NULL on failure.
 */
struct rte_keepalive *rte_keepalive_create(
	rte_keepalive_failure_callback_t callback,
	void *data);

/**
 * Checks & handles keepalive state of monitored cores.
 * @param *ptr_timer Triggering timer (unused)
 * @param *ptr_data  Data pointer (keepalive structure)
 */
void rte_keepalive_dispatch_pings(void *ptr_timer, void *ptr_data);

/**
 * Registers a core for keepalive checks.
 * @param *keepcfg
 *   Keepalive structure pointer
 * @param id_core
 *   ID number of core to register.
 */
void rte_keepalive_register_core(struct rte_keepalive *keepcfg,
	const int id_core);

/**
 * Per-core keepalive check.
 * @param *keepcfg
 *   Keepalive structure pointer
 *
 * This function needs to be called from within the main process loop of
 * the LCore to be checked.
 */
void
rte_keepalive_mark_alive(struct rte_keepalive *keepcfg);

/**
 * Per-core sleep-time indication.
 * @param *keepcfg
 *   Keepalive structure pointer
 *
 * If CPU idling is enabled, this function needs to be called from within
 * the main process loop of the LCore going to sleep, in order to avoid
 * the LCore being mis-detected as dead.
 */
void
rte_keepalive_mark_sleep(struct rte_keepalive *keepcfg);

/**
 * Registers a 'live core' callback.
 *
 * The complement of the 'dead core' callback. This is called when a
 * core is known to be alive, and is intended for cases when an app
 * needs to know 'liveness' beyond just knowing when a core has died.
 *
 * @param *keepcfg
 *   Keepalive structure pointer
 * @param callback
 *   Function called upon detection of a dead core.
 * @param data
 *   Data pointer to be passed to function callback.
 */
void
rte_keepalive_register_relay_callback(struct rte_keepalive *keepcfg,
	rte_keepalive_relay_callback_t callback,
	void *data);

#endif /* _KEEPALIVE_H_ */
