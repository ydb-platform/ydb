/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2019 Intel Corporation
 */

/**
 * @file
 *
 * String-related functions as replacement for libc equivalents
 */

#ifndef _RTE_STRING_FNS_H_
#define _RTE_STRING_FNS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <string.h>

#include <rte_common.h>

/**
 * Takes string "string" parameter and splits it at character "delim"
 * up to maxtokens-1 times - to give "maxtokens" resulting tokens. Like
 * strtok or strsep functions, this modifies its input string, by replacing
 * instances of "delim" with '\\0'. All resultant tokens are returned in the
 * "tokens" array which must have enough entries to hold "maxtokens".
 *
 * @param string
 *   The input string to be split into tokens
 *
 * @param stringlen
 *   The max length of the input buffer
 *
 * @param tokens
 *   The array to hold the pointers to the tokens in the string
 *
 * @param maxtokens
 *   The number of elements in the tokens array. At most, maxtokens-1 splits
 *   of the string will be done.
 *
 * @param delim
 *   The character on which the split of the data will be done
 *
 * @return
 *   The number of tokens in the tokens array.
 */
int
rte_strsplit(char *string, int stringlen,
             char **tokens, int maxtokens, char delim);

/**
 * @internal
 * DPDK-specific version of strlcpy for systems without
 * libc or libbsd copies of the function
 */
static inline size_t
rte_strlcpy(char *dst, const char *src, size_t size)
{
	return (size_t)snprintf(dst, size, "%s", src);
}

/**
 * @internal
 * DPDK-specific version of strlcat for systems without
 * libc or libbsd copies of the function
 */
static inline size_t
rte_strlcat(char *dst, const char *src, size_t size)
{
	size_t l = strnlen(dst, size);
	if (l < size)
		return l + rte_strlcpy(&dst[l], src, size - l);
	return l + strlen(src);
}

/* pull in a strlcpy function */
#ifdef RTE_EXEC_ENV_FREEBSD
#ifndef __BSD_VISIBLE /* non-standard functions are hidden */
#define strlcpy(dst, src, size) rte_strlcpy(dst, src, size)
#define strlcat(dst, src, size) rte_strlcat(dst, src, size)
#endif

#else /* non-BSD platforms */
#ifdef RTE_USE_LIBBSD
#error #include <bsd/string.h>

#else /* no BSD header files, create own */
#define strlcpy(dst, src, size) rte_strlcpy(dst, src, size)
#define strlcat(dst, src, size) rte_strlcat(dst, src, size)

#endif /* RTE_USE_LIBBSD */
#endif /* FREEBSD */

/**
 * Copy string src to buffer dst of size dsize.
 * At most dsize-1 chars will be copied.
 * Always NUL-terminates, unless (dsize == 0).
 * Returns number of bytes copied (terminating NUL-byte excluded) on success ;
 * negative errno on error.
 *
 * @param dst
 *   The destination string.
 *
 * @param src
 *   The input string to be copied.
 *
 * @param dsize
 *   Length in bytes of the destination buffer.
 *
 * @return
 *   The number of bytes copied on success
 *   -E2BIG if the destination buffer is too small.
 */
ssize_t
rte_strscpy(char *dst, const char *src, size_t dsize);

#ifdef __cplusplus
}
#endif

#endif /* RTE_STRING_FNS_H */
