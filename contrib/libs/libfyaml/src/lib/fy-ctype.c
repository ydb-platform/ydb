/*
 * fy-ctype.c - ctype utilities
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 *
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <libfyaml.h>

#include "fy-ctype.h"

const char *fy_uri_esc(const char *s, size_t len, uint8_t *code, int *code_len)
{
	const char *e = s + len;
	int j, k, width;
	uint8_t octet;
	char c;

	width = 0;
	k = 0;
	do {
		/* check for enough space for %XX */
		if ((e - s) < 3)
			return NULL;

		/* if more than one run, expect '%' */
		if (s[0] != '%')
			return NULL;

		octet = 0;
		for (j = 0; j < 2; j++) {
			c = s[1 + j];
			octet <<= 4;
			if (c >= '0' && c <= '9')
				octet |= c - '0';
			else if (c >= 'a' && c <= 'f')
				octet |= 10 + c - 'a';
			else
				octet |= 10 + c - 'A';
		}
		if (!width) {
			width = fy_utf8_width_by_first_octet(octet);
			if (!width)
				return NULL;
			k = 0;
		}
		if (k >= *code_len)
			return NULL;

		code[k++] = octet;

		/* skip over the 3 character escape */
		s += 3;

	} while (--width > 0);

	*code_len = k;

	return s;
}

