/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_INTERRUPTS_H_
#define _RTE_INTERRUPTS_H_

#include <rte_common.h>
#include <rte_compat.h>

/**
 * @file
 *
 * The RTE interrupt interface provides functions to register/unregister
 * callbacks for a specific interrupt.
 */

#ifdef __cplusplus
extern "C" {
#endif

/** Interrupt handle */
struct rte_intr_handle;

/** Function to be registered for the specific interrupt */
typedef void (*rte_intr_callback_fn)(void *cb_arg);

/**
 * Function to call after a callback is unregistered.
 * Can be used to close fd and free cb_arg.
 */
typedef void (*rte_intr_unregister_callback_fn)(struct rte_intr_handle *intr_handle,
						void *cb_arg);

#include "rte_eal_interrupts.h"

/**
 * It registers the callback for the specific interrupt. Multiple
 * callbacks can be registered at the same time.
 * @param intr_handle
 *  Pointer to the interrupt handle.
 * @param cb
 *  callback address.
 * @param cb_arg
 *  address of parameter for callback.
 *
 * @return
 *  - On success, zero.
 *  - On failure, a negative value.
 */
int rte_intr_callback_register(const struct rte_intr_handle *intr_handle,
				rte_intr_callback_fn cb, void *cb_arg);

/**
 * It unregisters the callback according to the specified interrupt handle.
 *
 * @param intr_handle
 *  pointer to the interrupt handle.
 * @param cb
 *  callback address.
 * @param cb_arg
 *  address of parameter for callback, (void *)-1 means to remove all
 *  registered which has the same callback address.
 *
 * @return
 *  - On success, return the number of callback entities removed.
 *  - On failure, a negative value.
 */
int rte_intr_callback_unregister(const struct rte_intr_handle *intr_handle,
				rte_intr_callback_fn cb, void *cb_arg);

/**
 * Unregister the callback according to the specified interrupt handle,
 * after it's no longer active. Fail if source is not active.
 *
 * @param intr_handle
 *  pointer to the interrupt handle.
 * @param cb_fn
 *  callback address.
 * @param cb_arg
 *  address of parameter for callback, (void *)-1 means to remove all
 *  registered which has the same callback address.
 * @param ucb_fn
 *  callback to call before cb is unregistered (optional).
 *  can be used to close fd and free cb_arg.
 *
 * @return
 *  - On success, return the number of callback entities marked for remove.
 *  - On failure, a negative value.
 */
__rte_experimental
int
rte_intr_callback_unregister_pending(const struct rte_intr_handle *intr_handle,
				rte_intr_callback_fn cb_fn, void *cb_arg,
				rte_intr_unregister_callback_fn ucb_fn);

/**
 * It enables the interrupt for the specified handle.
 *
 * @param intr_handle
 *  pointer to the interrupt handle.
 *
 * @return
 *  - On success, zero.
 *  - On failure, a negative value.
 */
int rte_intr_enable(const struct rte_intr_handle *intr_handle);

/**
 * It disables the interrupt for the specified handle.
 *
 * @param intr_handle
 *  pointer to the interrupt handle.
 *
 * @return
 *  - On success, zero.
 *  - On failure, a negative value.
 */
int rte_intr_disable(const struct rte_intr_handle *intr_handle);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * It acknowledges an interrupt raised for the specified handle.
 *
 * This function should be called at the end of each interrupt handler either
 * from application or driver, so that currently raised interrupt is acked and
 * further new interrupts are raised.
 *
 * @param intr_handle
 *  pointer to the interrupt handle.
 *
 * @return
 *  - On success, zero.
 *  - On failure, a negative value.
 */
__rte_experimental
int rte_intr_ack(const struct rte_intr_handle *intr_handle);

#ifdef __cplusplus
}
#endif

#endif
