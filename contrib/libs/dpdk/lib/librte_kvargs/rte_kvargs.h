/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2013 Intel Corporation.
 * Copyright(c) 2014 6WIND S.A.
 */

#ifndef _RTE_KVARGS_H_
#define _RTE_KVARGS_H_

/**
 * @file
 * RTE Argument parsing
 *
 * This module can be used to parse arguments whose format is
 * key1=value1,key2=value2,key3=value3,...
 *
 * The same key can appear several times with the same or a different
 * value. Indeed, the arguments are stored as a list of key/values
 * associations and not as a dictionary.
 *
 * This file provides some helpers that are especially used by virtual
 * ethernet devices at initialization for arguments parsing.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_compat.h>

/** Maximum number of key/value associations */
#define RTE_KVARGS_MAX 32

/** separator character used between each pair */
#define RTE_KVARGS_PAIRS_DELIM	","

/** separator character used between key and value */
#define RTE_KVARGS_KV_DELIM	"="

/** Type of callback function used by rte_kvargs_process() */
typedef int (*arg_handler_t)(const char *key, const char *value, void *opaque);

/** A key/value association */
struct rte_kvargs_pair {
	char *key;      /**< the name (key) of the association  */
	char *value;    /**< the value associated to that key */
};

/** Store a list of key/value associations */
struct rte_kvargs {
	char *str;      /**< copy of the argument string */
	unsigned count; /**< number of entries in the list */
	struct rte_kvargs_pair pairs[RTE_KVARGS_MAX]; /**< list of key/values */
};

/**
 * Allocate a rte_kvargs and store key/value associations from a string
 *
 * The function allocates and fills a rte_kvargs structure from a given
 * string whose format is key1=value1,key2=value2,...
 *
 * The structure can be freed with rte_kvargs_free().
 *
 * @param args
 *   The input string containing the key/value associations
 * @param valid_keys
 *   A list of valid keys (table of const char *, the last must be NULL).
 *   This argument is ignored if NULL
 *
 * @return
 *   - A pointer to an allocated rte_kvargs structure on success
 *   - NULL on error
 */
struct rte_kvargs *rte_kvargs_parse(const char *args,
		const char *const valid_keys[]);

/**
 * Allocate a rte_kvargs and store key/value associations from a string.
 * This version will consider any byte from valid_ends as a possible
 * terminating character, and will not parse beyond any of their occurrence.
 *
 * The function allocates and fills an rte_kvargs structure from a given
 * string whose format is key1=value1,key2=value2,...
 *
 * The structure can be freed with rte_kvargs_free().
 *
 * @param args
 *   The input string containing the key/value associations
 *
 * @param valid_keys
 *   A list of valid keys (table of const char *, the last must be NULL).
 *   This argument is ignored if NULL
 *
 * @param valid_ends
 *   Acceptable terminating characters.
 *   If NULL, the behavior is the same as ``rte_kvargs_parse``.
 *
 * @return
 *   - A pointer to an allocated rte_kvargs structure on success
 *   - NULL on error
 */
__rte_experimental
struct rte_kvargs *rte_kvargs_parse_delim(const char *args,
		const char *const valid_keys[],
		const char *valid_ends);

/**
 * Free a rte_kvargs structure
 *
 * Free a rte_kvargs structure previously allocated with
 * rte_kvargs_parse().
 *
 * @param kvlist
 *   The rte_kvargs structure. No error if NULL.
 */
void rte_kvargs_free(struct rte_kvargs *kvlist);

/**
 * Call a handler function for each key/value matching the key
 *
 * For each key/value association that matches the given key, calls the
 * handler function with the for a given arg_name passing the value on the
 * dictionary for that key and a given extra argument.
 *
 * @param kvlist
 *   The rte_kvargs structure. No error if NULL.
 * @param key_match
 *   The key on which the handler should be called, or NULL to process handler
 *   on all associations
 * @param handler
 *   The function to call for each matching key
 * @param opaque_arg
 *   A pointer passed unchanged to the handler
 *
 * @return
 *   - 0 on success
 *   - Negative on error
 */
int rte_kvargs_process(const struct rte_kvargs *kvlist,
	const char *key_match, arg_handler_t handler, void *opaque_arg);

/**
 * Count the number of associations matching the given key
 *
 * @param kvlist
 *   The rte_kvargs structure
 * @param key_match
 *   The key that should match, or NULL to count all associations

 * @return
 *   The number of entries
 */
unsigned rte_kvargs_count(const struct rte_kvargs *kvlist,
	const char *key_match);

/**
 * Generic kvarg handler for string comparison.
 *
 * This function can be used for a generic string comparison processing
 * on a list of kvargs.
 *
 * @param key
 *   kvarg pair key.
 *
 * @param value
 *   kvarg pair value.
 *
 * @param opaque
 *   Opaque pointer to a string.
 *
 * @return
 *   0 if the strings match.
 *   !0 otherwise or on error.
 *
 *   Unlike strcmp, comparison ordering is not kept.
 *   In order for rte_kvargs_process to stop processing on match error,
 *   a negative value is returned even if strcmp had returned a positive one.
 */
__rte_experimental
int rte_kvargs_strcmp(const char *key, const char *value, void *opaque);

#ifdef __cplusplus
}
#endif

#endif
