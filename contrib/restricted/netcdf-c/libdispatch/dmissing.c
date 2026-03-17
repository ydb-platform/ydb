/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *      See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

/**
 * Provide local alternatives for unix functions
 * not available on all machines.
 * Currently, this defines:
 *   strdup
 *   strcpy
 *   strlcpy
 *   strlcat
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#ifdef HAVE_STDIO_H
#include <stdio.h>
#endif
#ifdef HAVE_STRING_H
#include <string.h>
#endif

/**************************************************/

#ifndef HAVE_STRDUP
char*
strdup(const char* s)
{
    char* dup;
    size_t len;
    if(s == NULL) return NULL;
    len = strlen(s);
    dup = (char*)malloc(len+1);
    memcpy(dup,s,len);
    dup[len] = '\0';
    return dup;
}
#endif

#ifndef HAVE_STRNDUP
char*
strndup(const char* s, size_t len)
{
    char* dup;
    if(s == NULL) return NULL;
    dup = (char*)malloc(len+1);
    if(dup == NULL) return NULL;
    memcpy((void*)dup,s,len);
    dup[len] = '\0';
    return dup;
}
#endif

#if !defined(_MSC_VER) && !defined(WIN32)

#ifndef HAVE_STRLCAT
/* strlcat */
/*
 * Copyright (c) 1998, 2015 Todd C. Miller <Todd.Miller@courtesan.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/*
 * Appends src to string dst of size dsize (unlike strncat, dsize is the
 * full size of dst, not space left).  At most dsize-1 characters
 * will be copied.  Always NUL terminates (unless dsize <= strlen(dst)).
 * Returns strlen(src) + MIN(dsize, strlen(initial dst)).
 * If retval >= dsize, truncation occurred.
 */
size_t
nc_strlcat(char* dst, const char* src, size_t dsize)
{
	const char *odst = dst;
	const char *osrc = src;
	size_t n = dsize;
	size_t dlen;

	/* Find the end of dst and adjust bytes left but don't go past end. */
	while (n-- != 0 && *dst != '\0')
		dst++;
	dlen = dst - odst;
	n = dsize - dlen;

	if (n-- == 0)
		return(dlen + strlen(src));
	while (*src != '\0') {
		if (n != 0) {
			*dst++ = *src;
			n--;
		}
		src++;
	}
	*dst = '\0';

	return(dlen + (src - osrc));	/* count does not include NUL */
}
#endif /*!HAVE_STRLCAT*/

#endif /*WIN32*/

#if 0
Not currently used
/* Define an version of strcasestr renamed to avoid any system definition */
/* See https://android.googlesource.com/platform/bionic/+/a27d2baa/libc/string/strcasestr.c */
/*	$OpenBSD: strcasestr.c,v 1.3 2006/03/31 05:34:55 deraadt Exp $	*/
/*	$NetBSD: strcasestr.c,v 1.2 2005/02/09 21:35:47 kleink Exp $	*/
/*-
 * Copyright (c) 1990, 1993
 *	The Regents of the University of California.  All rights reserved.
 *
 * This code is derived from software contributed to Berkeley by
 * Chris Torek.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/*
 * Find the first occurrence of find in s, ignore case.
 */
const char *
NC_strcasestr(const char *s, const char *find)
{
    char c, sc;
    size_t len;
    if ((c = *find++) != 0) {
        c = (char)tolower((unsigned char)c);
        len = strlen(find);
        do {
            do {
                if ((sc = *s++) == 0) return (NULL);
            } while ((char)tolower((unsigned char)sc) != c);
        } while (strncasecmp(s, find, len) != 0);
        s--;
    }
    return ((char *)s);
}
#endif

#ifndef HAVE_MEMMOVE
/**
Define an implementation of memmove
in the event that it is not available.
@param dst target of the move
@param src source of the move
@param count number of bytes to move.
Note: dst and src can overlap
*/
void*
memmove(void *dst, const void *src, size_t count)
{
    char *d = dst;
    const char *s = src;

    if (d < s) {
        while (count--) {*d++ = *s++;}
    } else {
        d += count; /* Point to one past the end of destination */
        s += count; /* Point to one past the end of source */
        while (count--) {*(--d) = *(--s);} /* Decrement pointers and copy */
    }
    return dst;
}

#endif /*HAVE_MEMMOVE*/

