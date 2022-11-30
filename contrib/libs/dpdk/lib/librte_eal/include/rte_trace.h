/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(C) 2020 Marvell International Ltd.
 */

#ifndef _RTE_TRACE_H_
#define _RTE_TRACE_H_

/**
 * @file
 *
 * RTE Trace API
 *
 * This file provides the trace API to RTE applications.
 *
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdio.h>

#include <rte_common.h>
#include <rte_compat.h>

/**
 *  Test if trace is enabled.
 *
 *  @return
 *     true if trace is enabled, false otherwise.
 */
__rte_experimental
bool rte_trace_is_enabled(void);

/**
 * Enumerate trace mode operation.
 */
enum rte_trace_mode {
	/**
	 * In this mode, when no space is left in the trace buffer, the
	 * subsequent events overwrite the old events.
	 */
	RTE_TRACE_MODE_OVERWRITE,
	/**
	 * In this mode, when no space is left in the trace buffer, the
	 * subsequent events shall not be recorded.
	 */
	RTE_TRACE_MODE_DISCARD,
};

/**
 * Set the trace mode.
 *
 * @param mode
 *   Trace mode.
 */
__rte_experimental
void rte_trace_mode_set(enum rte_trace_mode mode);

/**
 * Get the trace mode.
 *
 * @return
 *   The current trace mode.
 */
__rte_experimental
enum rte_trace_mode rte_trace_mode_get(void);

/**
 * Enable/Disable a set of tracepoints based on globbing pattern.
 *
 * @param pattern
 *   The globbing pattern identifying the tracepoint.
 * @param enable
 *   true to enable tracepoint, false to disable the tracepoint, upon match.
 * @return
 *   - 0: Success and no pattern match.
 *   - 1: Success and found pattern match.
 *   - (-ERANGE): Tracepoint object is not registered.
 */
__rte_experimental
int rte_trace_pattern(const char *pattern, bool enable);

/**
 * Enable/Disable a set of tracepoints based on regular expression.
 *
 * @param regex
 *   A regular expression identifying the tracepoint.
 * @param enable
 *   true to enable tracepoint, false to disable the tracepoint, upon match.
 * @return
 *   - 0: Success and no pattern match.
 *   - 1: Success and found pattern match.
 *   - (-ERANGE): Tracepoint object is not registered.
 *   - (-EINVAL): Invalid regular expression rule.
 */
__rte_experimental
int rte_trace_regexp(const char *regex, bool enable);

/**
 * Save the trace buffer to the trace directory.
 *
 * By default, trace directory will be created at $HOME directory and this can
 * be overridden by --trace-dir EAL parameter.
 *
 * @return
 *   - 0: Success.
 *   - <0 : Failure.
 */
__rte_experimental
int rte_trace_save(void);

/**
 * Dump the trace metadata to a file.
 *
 * @param f
 *   A pointer to a file for output
 * @return
 *   - 0: Success.
 *   - <0 : Failure.
 */
__rte_experimental
int rte_trace_metadata_dump(FILE *f);

/**
 * Dump the trace subsystem status to a file.
 *
 * @param f
 *   A pointer to a file for output
 */
__rte_experimental
void rte_trace_dump(FILE *f);

#ifdef __cplusplus
}
#endif

#endif /* _RTE_TRACE_H_ */
