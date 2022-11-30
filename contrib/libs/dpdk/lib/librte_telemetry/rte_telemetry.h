/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2018 Intel Corporation
 */

#include <stdint.h>
#include <sched.h>
#include <rte_compat.h>

#ifndef _RTE_TELEMETRY_H_
#define _RTE_TELEMETRY_H_

/** Maximum number of telemetry callbacks. */
#define TELEMETRY_MAX_CALLBACKS 64
/** Maximum length for string used in object. */
#define RTE_TEL_MAX_STRING_LEN 64
/** Maximum length of string. */
#define RTE_TEL_MAX_SINGLE_STRING_LEN 8192
/** Maximum number of dictionary entries. */
#define RTE_TEL_MAX_DICT_ENTRIES 256
/** Maximum number of array entries. */
#define RTE_TEL_MAX_ARRAY_ENTRIES 512

/**
 * @file
 *
 * RTE Telemetry.
 *
 * @warning
 * @b EXPERIMENTAL:
 * All functions in this file may be changed or removed without prior notice.
 *
 * The telemetry library provides a method to retrieve statistics from
 * DPDK by sending a request message over a socket. DPDK will send
 * a JSON encoded response containing telemetry data.
 ***/

/** opaque structure used internally for managing data from callbacks */
struct rte_tel_data;

/**
 * The types of data that can be managed in arrays or dicts.
 * For arrays, this must be specified at creation time, while for
 * dicts this is specified implicitly each time an element is added
 * via calling a type-specific function.
 */
enum rte_tel_value_type {
	RTE_TEL_STRING_VAL, /** a string value */
	RTE_TEL_INT_VAL,    /** a signed 32-bit int value */
	RTE_TEL_U64_VAL,    /** an unsigned 64-bit int value */
	RTE_TEL_CONTAINER, /** a container struct */
};

/**
 * Start an array of the specified type for returning from a callback
 *
 * @param d
 *   The data structure passed to the callback
 * @param type
 *   The type of the array of data
 * @return
 *   0 on success, negative errno on error
 */
__rte_experimental
int
rte_tel_data_start_array(struct rte_tel_data *d, enum rte_tel_value_type type);

/**
 * Start a dictionary of values for returning from a callback
 *
 * @param d
 *   The data structure passed to the callback
 * @return
 *   0 on success, negative errno on error
 */
__rte_experimental
int
rte_tel_data_start_dict(struct rte_tel_data *d);

/**
 * Set a string for returning from a callback
 *
 * @param d
 *   The data structure passed to the callback
 * @param str
 *   The string to be returned in the data structure
 * @return
 *   0 on success, negative errno on error, E2BIG on string truncation
 */
__rte_experimental
int
rte_tel_data_string(struct rte_tel_data *d, const char *str);

/**
 * Add a string to an array.
 * The array must have been started by rte_tel_data_start_array() with
 * RTE_TEL_STRING_VAL as the type parameter.
 *
 * @param d
 *   The data structure passed to the callback
 * @param str
 *   The string to be returned in the array
 * @return
 *   0 on success, negative errno on error, E2BIG on string truncation
 */
__rte_experimental
int
rte_tel_data_add_array_string(struct rte_tel_data *d, const char *str);

/**
 * Add an int to an array.
 * The array must have been started by rte_tel_data_start_array() with
 * RTE_TEL_INT_VAL as the type parameter.
 *
 * @param d
 *   The data structure passed to the callback
 * @param x
 *   The number to be returned in the array
 * @return
 *   0 on success, negative errno on error
 */
__rte_experimental
int
rte_tel_data_add_array_int(struct rte_tel_data *d, int x);

/**
 * Add a uint64_t to an array.
 * The array must have been started by rte_tel_data_start_array() with
 * RTE_TEL_U64_VAL as the type parameter.
 *
 * @param d
 *   The data structure passed to the callback
 * @param x
 *   The number to be returned in the array
 * @return
 *   0 on success, negative errno on error
 */
__rte_experimental
int
rte_tel_data_add_array_u64(struct rte_tel_data *d, uint64_t x);

/**
 * Add a container to an array. A container is an existing telemetry data
 * array. The array the container is to be added to must have been started by
 * rte_tel_data_start_array() with RTE_TEL_CONTAINER as the type parameter.
 * The container type must be an array of type uint64_t/int/string.
 *
 * @param d
 *   The data structure passed to the callback
 * @param val
 *   The pointer to the container to be stored in the array.
 * @param keep
 *   Flag to indicate that the container memory should not be automatically
 *   freed by the telemetry library once it has finished with the data.
 *   1 = keep, 0 = free.
 * @return
 *   0 on success, negative errno on error
 */
__rte_experimental
int
rte_tel_data_add_array_container(struct rte_tel_data *d,
		struct rte_tel_data *val, int keep);

/**
 * Add a string value to a dictionary.
 * The dict must have been started by rte_tel_data_start_dict().
 *
 * @param d
 *   The data structure passed to the callback
 * @param name
 *   The name the value is to be stored under in the dict
 * @param val
 *   The string to be stored in the dict
 * @return
 *   0 on success, negative errno on error, E2BIG on string truncation of
 *   either name or value.
 */
__rte_experimental
int
rte_tel_data_add_dict_string(struct rte_tel_data *d, const char *name,
		const char *val);

/**
 * Add an int value to a dictionary.
 * The dict must have been started by rte_tel_data_start_dict().
 *
 * @param d
 *   The data structure passed to the callback
 * @param name
 *   The name the value is to be stored under in the dict
 * @param val
 *   The number to be stored in the dict
 * @return
 *   0 on success, negative errno on error, E2BIG on string truncation of name.
 */
__rte_experimental
int
rte_tel_data_add_dict_int(struct rte_tel_data *d, const char *name, int val);

/**
 * Add a uint64_t value to a dictionary.
 * The dict must have been started by rte_tel_data_start_dict().
 *
 * @param d
 *   The data structure passed to the callback
 * @param name
 *   The name the value is to be stored under in the dict
 * @param val
 *   The number to be stored in the dict
 * @return
 *   0 on success, negative errno on error, E2BIG on string truncation of name.
 */
__rte_experimental
int
rte_tel_data_add_dict_u64(struct rte_tel_data *d,
		const char *name, uint64_t val);

/**
 * Add a container to a dictionary. A container is an existing telemetry data
 * array. The dict the container is to be added to must have been started by
 * rte_tel_data_start_dict(). The container must be an array of type
 * uint64_t/int/string.
 *
 * @param d
 *   The data structure passed to the callback
 * @param name
 *   The name the value is to be stored under in the dict.
 * @param val
 *   The pointer to the container to be stored in the dict.
 * @param keep
 *   Flag to indicate that the container memory should not be automatically
 *   freed by the telemetry library once it has finished with the data.
 *   1 = keep, 0 = free.
 * @return
 *   0 on success, negative errno on error
 */
__rte_experimental
int
rte_tel_data_add_dict_container(struct rte_tel_data *d, const char *name,
		struct rte_tel_data *val, int keep);

/**
 * This telemetry callback is used when registering a telemetry command.
 * It handles getting and formatting information to be returned to telemetry
 * when requested.
 *
 * @param cmd
 * The cmd that was requested by the client.
 * @param params
 * Contains data required by the callback function.
 * @param info
 * The information to be returned to the caller.
 *
 * @return
 * Length of buffer used on success.
 * @return
 * Negative integer on error.
 */
typedef int (*telemetry_cb)(const char *cmd, const char *params,
		struct rte_tel_data *info);

/**
 * Used for handling data received over a telemetry socket.
 *
 * @param sock_id
 * ID for the socket to be used by the handler.
 *
 * @return
 * Void.
 */
typedef void * (*handler)(void *sock_id);

/**
 * Used when registering a command and callback function with telemetry.
 *
 * @param cmd
 * The command to register with telemetry.
 * @param fn
 * Callback function to be called when the command is requested.
 * @param help
 * Help text for the command.
 *
 * @return
 *  0 on success.
 * @return
 *  -EINVAL for invalid parameters failure.
 *  @return
 *  -ENOENT if max callbacks limit has been reached.
 */
__rte_experimental
int
rte_telemetry_register_cmd(const char *cmd, telemetry_cb fn, const char *help);

/**
 * @internal
 * Initialize Telemetry.
 *
 * @param runtime_dir
 * The runtime directory of DPDK.
 * @param cpuset
 * The CPU set to be used for setting the thread affinity.
 * @param err_str
 * This err_str pointer should point to NULL on entry. In the case of an error
 * or warning, it will be non-NULL on exit.
 *
 * @return
 *  0 on success.
 * @return
 *  -1 on failure.
 */
__rte_experimental
int
rte_telemetry_init(const char *runtime_dir, rte_cpuset_t *cpuset,
		const char **err_str);

/**
 * Get a pointer to a container with memory allocated. The container is to be
 * used embedded within an existing telemetry dict/array.
 *
 * @return
 *  Pointer to a container.
 */
__rte_experimental
struct rte_tel_data *
rte_tel_data_alloc(void);

/**
 * @internal
 * Free a container that has memory allocated.
 *
 * @param data
 *  Pointer to container.
 *.
 */
__rte_experimental
void
rte_tel_data_free(struct rte_tel_data *data);

#endif
