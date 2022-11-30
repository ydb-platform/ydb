/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (C) 1996, 1997, 1998 Theodore Ts'o.
 */
/**
 * @file
 *
 * UUID related functions originally from libuuid
 */

#ifndef _RTE_UUID_H_
#define _RTE_UUID_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

/**
 * Struct describing a Universal Unique Identifier
 */
typedef unsigned char rte_uuid_t[16];

/**
 * Helper for defining UUID values for id tables.
 */
#define RTE_UUID_INIT(a, b, c, d, e) {		\
	((a) >> 24) & 0xff, ((a) >> 16) & 0xff,	\
	((a) >> 8) & 0xff, (a) & 0xff,		\
	((b) >> 8) & 0xff, (b) & 0xff,		\
	((c) >> 8) & 0xff, (c) & 0xff,		\
	((d) >> 8) & 0xff, (d) & 0xff,		\
	((e) >> 40) & 0xff, ((e) >> 32) & 0xff, \
	((e) >> 24) & 0xff, ((e) >> 16) & 0xff, \
	((e) >> 8) & 0xff, (e) & 0xff		\
}

/**
 * Test if UUID is all zeros.
 *
 * @param uu
 *    The uuid to check.
 * @return
 *    true if uuid is NULL value, false otherwise
 */
bool rte_uuid_is_null(const rte_uuid_t uu);

/**
 * Copy uuid.
 *
 * @param dst
 *    Destination uuid
 * @param src
 *    Source uuid
 */
static inline void rte_uuid_copy(rte_uuid_t dst, const rte_uuid_t src)
{
	memcpy(dst, src, sizeof(rte_uuid_t));
}

/**
 * Compare two UUID's
 *
 * @param a
 *    A UUID to compare
 * @param b
 *    A UUID to compare
 * @return
 *   returns an integer less than, equal to, or greater than zero if UUID a is
 *   is less than, equal, or greater than UUID b.
 */
int	rte_uuid_compare(const rte_uuid_t a, const rte_uuid_t b);

/**
 * Extract UUID from string
 *
 * @param in
 *    Pointer to string of characters to convert
 * @param uu
 *    Destination UUID
 * @return
 *    Returns 0 on success, and -1 if string is not a valid UUID.
 */
int	rte_uuid_parse(const char *in, rte_uuid_t uu);

/**
 * Convert UUID to string
 *
 * @param uu
 *    UUID to format
 * @param out
 *    Resulting string buffer
 * @param len
 *    Sizeof the available string buffer
 */
#define RTE_UUID_STRLEN	(36 + 1)
void	rte_uuid_unparse(const rte_uuid_t uu, char *out, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* RTE_UUID_H */
