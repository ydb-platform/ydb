#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <errno.h>

#include <rte_string_fns.h>

/* split string into tokens */
int
rte_strsplit(char *string, int stringlen,
	     char **tokens, int maxtokens, char delim)
{
	int i, tok = 0;
	int tokstart = 1; /* first token is right at start of string */

	if (string == NULL || tokens == NULL)
		goto einval_error;

	for (i = 0; i < stringlen; i++) {
		if (string[i] == '\0' || tok >= maxtokens)
			break;
		if (tokstart) {
			tokstart = 0;
			tokens[tok++] = &string[i];
		}
		if (string[i] == delim) {
			string[i] = '\0';
			tokstart = 1;
		}
	}
	return tok;

einval_error:
	errno = EINVAL;
	return -1;
}

/* Copy src string into dst.
 *
 * Return negative value and NUL-terminate if dst is too short,
 * Otherwise return number of bytes copied.
 */
ssize_t
rte_strscpy(char *dst, const char *src, size_t dsize)
{
	size_t nleft = dsize;
	size_t res = 0;

	/* Copy as many bytes as will fit. */
	while (nleft != 0) {
		dst[res] = src[res];
		if (src[res] == '\0')
			return res;
		res++;
		nleft--;
	}

	/* Not enough room in dst, set NUL and return error. */
	if (res != 0)
		dst[res - 1] = '\0';
	return -E2BIG;
}
