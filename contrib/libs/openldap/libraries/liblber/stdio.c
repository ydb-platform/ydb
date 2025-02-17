/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2024 The OpenLDAP Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#include "portable.h"

#include <stdio.h>
#include <ac/stdarg.h>
#include <ac/string.h>
#include <ac/ctype.h>
#include <lutil.h>
#include <unistd.h>

#if !defined(HAVE_VSNPRINTF) && !defined(HAVE_EBCDIC)
/* Write at most n characters to the buffer in str, return the
 * number of chars written or -1 if the buffer would have been
 * overflowed.
 *
 * This is portable to any POSIX-compliant system. We use pipe()
 * to create a valid file descriptor, and then fdopen() it to get
 * a valid FILE pointer. The user's buffer and size are assigned
 * to the FILE pointer using setvbuf. Then we close the read side
 * of the pipe to invalidate the descriptor.
 *
 * If the write arguments all fit into size n, the write will
 * return successfully. If the write is too large, the stdio
 * buffer will need to be flushed to the underlying file descriptor.
 * The flush will fail because it is attempting to write to a
 * broken pipe, and the write will be terminated.
 * -- hyc, 2002-07-19
 */
/* This emulation uses vfprintf; on OS/390 we're also emulating
 * that function so it's more efficient just to have a separate
 * version of vsnprintf there.
 */
#include <ac/signal.h>
int ber_pvt_vsnprintf( char *str, size_t n, const char *fmt, va_list ap )
{
	int fds[2], res;
	FILE *f;
	RETSIGTYPE (*sig)();

	if (pipe( fds )) return -1;

	f = fdopen( fds[1], "w" );
	if ( !f ) {
		close( fds[1] );
		close( fds[0] );
		return -1;
	}
	setvbuf( f, str, _IOFBF, n );
	sig = signal( SIGPIPE, SIG_IGN );
	close( fds[0] );

	res = vfprintf( f, fmt, ap );

	fclose( f );
	signal( SIGPIPE, sig );
	if ( res > 0 && res < n ) {
		res = vsprintf( str, fmt, ap );
	}
	return res;
}
#endif

#ifndef HAVE_SNPRINTF
int ber_pvt_snprintf( char *str, size_t n, const char *fmt, ... )
{
	va_list ap;
	int res;

	va_start( ap, fmt );
	res = vsnprintf( str, n, fmt, ap );
	va_end( ap );
	return res;
}
#endif /* !HAVE_SNPRINTF */

#ifdef HAVE_EBCDIC
/* stdio replacements with ASCII/EBCDIC translation for OS/390.
 * The OS/390 port depends on the CONVLIT compiler option being
 * used to force character and string literals to be compiled in
 * ISO8859-1, and the __LIBASCII cpp symbol to be defined to use the
 * OS/390 ASCII-compatibility library. This library only supplies
 * an ASCII version of sprintf, so other needed functions are
 * provided here.
 *
 * All of the internal character manipulation is done in ASCII,
 * but file I/O is EBCDIC, so we catch any stdio reading/writing
 * of files here and do the translations.
 */

#undef fputs
#undef fgets

char *ber_pvt_fgets( char *s, int n, FILE *fp )
{
	s = (char *)fgets( s, n, fp );
	if ( s ) __etoa( s );
	return s;
}

int ber_pvt_fputs( const char *str, FILE *fp )
{
	char buf[8192];

	strncpy( buf, str, sizeof(buf) );
	__atoe( buf );
	return fputs( buf, fp );
}

/* The __LIBASCII doesn't include a working vsprintf, so we make do
 * using just sprintf. This is a very simplistic parser that looks for
 * format strings and uses sprintf to process them one at a time.
 * Literal text is just copied straight to the destination.
 * The result is appended to the destination string. The parser
 * recognizes field-width specifiers and the 'l' qualifier; it
 * may need to be extended to recognize other qualifiers but so
 * far this seems to be enough.
 */
int ber_pvt_vsnprintf( char *str, size_t n, const char *fmt, va_list ap )
{
	char *ptr, *pct, *s2, *f2, *end;
	char fm2[64];
	int len, rem;

	ptr = (char *)fmt;
	s2 = str;
	fm2[0] = '%';
	if (n) {
		end = str + n;
	} else {
		end = NULL;
	}

	for (pct = strchr(ptr, '%'); pct; pct = strchr(ptr, '%')) {
		len = pct-ptr;
		if (end) {
			rem = end-s2;
			if (rem < 1) return -1;
			if (rem < len) len = rem;
		}
		s2 = lutil_strncopy( s2, ptr, len );
		/* Did we cheat the length above? If so, bail out */
		if (len < pct-ptr) return -1;
		for (pct++, f2 = fm2+1; isdigit(*pct);) *f2++ = *pct++;
		if (*pct == 'l') *f2++ = *pct++;
		if (*pct == '%') {
			*s2++ = '%';
		} else {
			*f2++ = *pct;
			*f2 = '\0';
			if (*pct == 's') {
				char *ss = va_arg(ap, char *);
				/* Attempt to limit sprintf output. This
				 * may be thrown off if field widths were
				 * specified for this string.
				 *
				 * If it looks like the string is too
				 * long for the remaining buffer, bypass
				 * sprintf and just copy what fits, then
				 * quit.
				 */
				if (end && strlen(ss) > (rem=end-s2)) {
					strncpy(s2, ss, rem);
					return -1;
				} else {
					s2 += sprintf(s2, fm2, ss);
				}
			} else {
				s2 += sprintf(s2, fm2, va_arg(ap, int));
			}
		}
		ptr = pct + 1;
	}
	if (end) {
		rem = end-s2;
		if (rem > 0) {
			len = strlen(ptr);
			s2 = lutil_strncopy( s2, ptr, rem );
			rem -= len;
		}
		if (rem < 0) return -1;
	} else {
		s2 = lutil_strcopy( s2, ptr );
	}
	return s2 - str;
}

int ber_pvt_vsprintf( char *str, const char *fmt, va_list ap )
{
	return vsnprintf( str, 0, fmt, ap );
}

/* The fixed buffer size here is a problem, we don't know how
 * to flush the buffer and keep printing if the msg is too big. 
 * Hopefully we never try to write something bigger than this
 * in a log msg...
 */
int ber_pvt_vfprintf( FILE *fp, const char *fmt, va_list ap )
{
	char buf[8192];
	int res;

	vsnprintf( buf, sizeof(buf), fmt, ap );
	__atoe( buf );
	res = fputs( buf, fp );
	if (res == EOF) res = -1;
	return res;
}

int ber_pvt_printf( const char *fmt, ... )
{
	va_list ap;
	int res;

	va_start( ap, fmt );
	res = ber_pvt_vfprintf( stdout, fmt, ap );
	va_end( ap );
	return res;
}

int ber_pvt_fprintf( FILE *fp, const char *fmt, ... )
{
	va_list ap;
	int res;

	va_start( ap, fmt );
	res = ber_pvt_vfprintf( fp, fmt, ap );
	va_end( ap );
	return res;
}
#endif
