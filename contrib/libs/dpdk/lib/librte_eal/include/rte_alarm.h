/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_ALARM_H_
#define _RTE_ALARM_H_

/**
 * @file
 *
 * Alarm functions
 *
 * Simple alarm-clock functionality supplied by eal.
 * Does not require hpet support.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

/**
 * Signature of callback back function called when an alarm goes off.
 */
typedef void (*rte_eal_alarm_callback)(void *arg);

/**
 * Function to set a callback to be triggered when us microseconds
 * have expired. Accuracy of timing to the microsecond is not guaranteed. The
 * alarm function will not be called *before* the requested time, but may
 * be called a short period of time afterwards.
 * The alarm handler will be called only once. There is no need to call
 * "rte_eal_alarm_cancel" from within the callback function.
 *
 * @param us
 *   The time in microseconds before the callback is called
 * @param cb
 *   The function to be called when the alarm expires
 * @param cb_arg
 *   Pointer parameter to be passed to the callback function
 *
 * @return
 *   On success, zero.
 *   On failure, a negative error number
 */
int rte_eal_alarm_set(uint64_t us, rte_eal_alarm_callback cb, void *cb_arg);

/**
 * Function to cancel an alarm callback which has been registered before. If
 * used outside alarm callback it wait for all callbacks to finish execution.
 *
 * @param cb_fn
 *  alarm callback
 * @param cb_arg
 *  Pointer parameter to be passed to the callback function. To remove all
 *  copies of a given callback function, irrespective of parameter, (void *)-1
 *  can be used here.
 *
 * @return
 *    - value greater than 0 and rte_errno not changed - returned value is
 *      the number of canceled alarm callback functions
 *    - value greater or equal 0 and rte_errno set to EINPROGRESS, at least one
 *      alarm could not be canceled because cancellation was requested from alarm
 *      callback context. Returned value is the number of successfully canceled
 *      alarm callbacks
 *    -  0 and rte_errno set to ENOENT - no alarm found
 *    - -1 and rte_errno set to EINVAL - invalid parameter (NULL callback)
 */
int rte_eal_alarm_cancel(rte_eal_alarm_callback cb_fn, void *cb_arg);

#ifdef __cplusplus
}
#endif


#endif /* _RTE_ALARM_H_ */
