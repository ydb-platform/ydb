#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"

#include "spdk/string.h"

char *
spdk_vsprintf_append_realloc(char *buffer, const char *format, va_list args)
{
	va_list args_copy;
	char *new_buffer;
	int orig_size = 0, new_size;

	/* Original buffer size */
	if (buffer) {
		orig_size = strlen(buffer);
	}

	/* Necessary buffer size */
	va_copy(args_copy, args);
	new_size = vsnprintf(NULL, 0, format, args_copy);
	va_end(args_copy);

	if (new_size < 0) {
		return NULL;
	}
	new_size += orig_size + 1;

	new_buffer = realloc(buffer, new_size);
	if (new_buffer == NULL) {
		return NULL;
	}

	vsnprintf(new_buffer + orig_size, new_size - orig_size, format, args);

	return new_buffer;
}

char *
spdk_sprintf_append_realloc(char *buffer, const char *format, ...)
{
	va_list args;
	char *ret;

	va_start(args, format);
	ret = spdk_vsprintf_append_realloc(buffer, format, args);
	va_end(args);

	return ret;
}

char *
spdk_vsprintf_alloc(const char *format, va_list args)
{
	return spdk_vsprintf_append_realloc(NULL, format, args);
}

char *
spdk_sprintf_alloc(const char *format, ...)
{
	va_list args;
	char *ret;

	va_start(args, format);
	ret = spdk_vsprintf_alloc(format, args);
	va_end(args);

	return ret;
}

char *
spdk_strlwr(char *s)
{
	char *p;

	if (s == NULL) {
		return NULL;
	}

	p = s;
	while (*p != '\0') {
		*p = tolower(*p);
		p++;
	}

	return s;
}

char *
spdk_strsepq(char **stringp, const char *delim)
{
	char *p, *q, *r;
	int quoted = 0, bslash = 0;

	p = *stringp;
	if (p == NULL) {
		return NULL;
	}

	r = q = p;
	while (*q != '\0' && *q != '\n') {
		/* eat quoted characters */
		if (bslash) {
			bslash = 0;
			*r++ = *q++;
			continue;
		} else if (quoted) {
			if (quoted == '"' && *q == '\\') {
				bslash = 1;
				q++;
				continue;
			} else if (*q == quoted) {
				quoted = 0;
				q++;
				continue;
			}
			*r++ = *q++;
			continue;
		} else if (*q == '\\') {
			bslash = 1;
			q++;
			continue;
		} else if (*q == '"' || *q == '\'') {
			quoted = *q;
			q++;
			continue;
		}

		/* separator? */
		if (strchr(delim, *q) == NULL) {
			*r++ = *q++;
			continue;
		}

		/* new string */
		q++;
		break;
	}
	*r = '\0';

	/* skip tailer */
	while (*q != '\0' && strchr(delim, *q) != NULL) {
		q++;
	}
	if (*q != '\0') {
		*stringp = q;
	} else {
		*stringp = NULL;
	}

	return p;
}

char *
spdk_str_trim(char *s)
{
	char *p, *q;

	if (s == NULL) {
		return NULL;
	}

	/* remove header */
	p = s;
	while (*p != '\0' && isspace(*p)) {
		p++;
	}

	/* remove tailer */
	q = p + strlen(p);
	while (q - 1 >= p && isspace(*(q - 1))) {
		q--;
		*q = '\0';
	}

	/* if remove header, move */
	if (p != s) {
		q = s;
		while (*p != '\0') {
			*q++ = *p++;
		}
		*q = '\0';
	}

	return s;
}

void
spdk_strcpy_pad(void *dst, const char *src, size_t size, int pad)
{
	size_t len;

	len = strlen(src);
	if (len < size) {
		memcpy(dst, src, len);
		memset((char *)dst + len, pad, size - len);
	} else {
		memcpy(dst, src, size);
	}
}

size_t
spdk_strlen_pad(const void *str, size_t size, int pad)
{
	const uint8_t *start;
	const uint8_t *iter;
	uint8_t pad_byte;

	pad_byte = (uint8_t)pad;
	start = (const uint8_t *)str;

	if (size == 0) {
		return 0;
	}

	iter = start + size - 1;
	while (1) {
		if (*iter != pad_byte) {
			return iter - start + 1;
		}

		if (iter == start) {
			/* Hit the start of the string finding only pad_byte. */
			return 0;
		}
		iter--;
	}
}

int
spdk_parse_ip_addr(char *ip, char **host, char **port)
{
	char *p;

	if (ip == NULL) {
		return -EINVAL;
	}

	*host = NULL;
	*port = NULL;

	if (ip[0] == '[') {
		/* IPv6 */
		p = strchr(ip, ']');
		if (p == NULL) {
			return -EINVAL;
		}
		*host = &ip[1];
		*p = '\0';

		p++;
		if (*p == '\0') {
			return 0;
		} else if (*p != ':') {
			return -EINVAL;
		}

		p++;
		if (*p == '\0') {
			return 0;
		}

		*port = p;
	} else {
		/* IPv4 */
		p = strchr(ip, ':');
		if (p == NULL) {
			*host = ip;
			return 0;
		}

		*host = ip;
		*p = '\0';

		p++;
		if (*p == '\0') {
			return 0;
		}

		*port = p;
	}

	return 0;
}

size_t
spdk_str_chomp(char *s)
{
	size_t len = strlen(s);
	size_t removed = 0;

	while (len > 0) {
		if (s[len - 1] != '\r' && s[len - 1] != '\n') {
			break;
		}

		s[len - 1] = '\0';
		len--;
		removed++;
	}

	return removed;
}

void
spdk_strerror_r(int errnum, char *buf, size_t buflen)
{
	int rc;

#if defined(__USE_GNU)
	char *new_buffer;
	new_buffer = strerror_r(errnum, buf, buflen);
	if (new_buffer == buf) {
		rc = 0;
	} else if (new_buffer != NULL) {
		snprintf(buf, buflen, "%s", new_buffer);
		rc = 0;
	} else {
		rc = 1;
	}
#else
	rc = strerror_r(errnum, buf, buflen);
#endif

	if (rc != 0) {
		snprintf(buf, buflen, "Unknown error %d", errnum);
	}
}

int
spdk_parse_capacity(const char *cap_str, uint64_t *cap, bool *has_prefix)
{
	int rc;
	char bin_prefix;

	rc = sscanf(cap_str, "%"SCNu64"%c", cap, &bin_prefix);
	if (rc == 1) {
		*has_prefix = false;
		return 0;
	} else if (rc == 0) {
		if (errno == 0) {
			/* No scanf matches - the string does not start with a digit */
			return -EINVAL;
		} else {
			/* Parsing error */
			return -errno;
		}
	}

	*has_prefix = true;
	switch (bin_prefix) {
	case 'k':
	case 'K':
		*cap *= 1024;
		break;
	case 'm':
	case 'M':
		*cap *= 1024 * 1024;
		break;
	case 'g':
	case 'G':
		*cap *= 1024 * 1024 * 1024;
		break;
	default:
		return -EINVAL;
	}

	return 0;
}

bool
spdk_mem_all_zero(const void *data, size_t size)
{
	const uint8_t *buf = data;

	while (size--) {
		if (*buf++ != 0) {
			return false;
		}
	}

	return true;
}

long int
spdk_strtol(const char *nptr, int base)
{
	long val;
	char *endptr;

	/* Since strtoll() can legitimately return 0, LONG_MAX, or LONG_MIN
	 * on both success and failure, the calling program should set errno
	 * to 0 before the call.
	 */
	errno = 0;

	val = strtol(nptr, &endptr, base);

	if (!errno && *endptr != '\0') {
		/* Non integer character was found. */
		return -EINVAL;
	} else if (errno == ERANGE && (val == LONG_MAX || val == LONG_MIN)) {
		/* Overflow occurred. */
		return -ERANGE;
	} else if (errno != 0 && val == 0) {
		/* Other error occurred. */
		return -errno;
	} else if (val < 0) {
		/* Input string was negative number. */
		return -ERANGE;
	}

	return val;
}

long long int
spdk_strtoll(const char *nptr, int base)
{
	long long val;
	char *endptr;

	/* Since strtoll() can legitimately return 0, LLONG_MAX, or LLONG_MIN
	 * on both success and failure, the calling program should set errno
	 * to 0 before the call.
	 */
	errno = 0;

	val = strtoll(nptr, &endptr, base);

	if (!errno && *endptr != '\0') {
		/* Non integer character was found. */
		return -EINVAL;
	} else if (errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN)) {
		/* Overflow occurred. */
		return -ERANGE;
	} else if (errno != 0 && val == 0) {
		/* Other error occurred. */
		return -errno;
	} else if (val < 0) {
		/* Input string was negative number. */
		return -ERANGE;
	}

	return val;
}
