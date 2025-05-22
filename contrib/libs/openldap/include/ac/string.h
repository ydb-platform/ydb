/* Generic string.h */
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
 * A copy of this license is available in file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 */

#ifndef _AC_STRING_H
#define _AC_STRING_H

#ifdef STDC_HEADERS
#	include <string.h>

#else
#	ifdef HAVE_STRING_H
#		include <string.h>
#	endif
#	if defined(HAVE_STRINGS_H) && (!defined(HAVE_STRING_H) || defined(BOTH_STRINGS_H))
#		include <strings.h>
#	endif

#	ifdef HAVE_MEMORY_H
#		include <memory.h>
#	endif

#	ifndef HAVE_STRRCHR
#		undef strchr
#		define strchr index
#		undef strrchr
#		define strrchr rindex
#	endif

#	ifndef HAVE_MEMCPY
#		undef memcpy
#		define memcpy(d, s, n)		((void) bcopy ((s), (d), (n)))
#		undef memmove
#		define memmove(d, s, n)		((void) bcopy ((s), (d), (n)))
#	endif
#endif

/* use ldap_pvt_strtok instead of strtok or strtok_r! */
LDAP_F(char *) ldap_pvt_strtok LDAP_P(( char *str,
	const char *delim, char **pos ));

#ifndef HAVE_STRDUP
	/* strdup() is missing, declare our own version */
#	undef strdup
#	define strdup(s) ber_strdup(s)
#elif !defined(_WIN32)
	/* some systems fail to declare strdup */
	/* Windows does not require this declaration */
	LDAP_LIBC_F(char *) (strdup) LDAP_P((const char *s));
#endif

/*
 * some systems fail to declare strcasecmp() and strncasecmp()
 * we need them declared so we can obtain pointers to them
 */

/* we don't want these declared for Windows or Mingw */
#ifndef _WIN32
LDAP_LIBC_F(int) (strcasecmp) LDAP_P((const char *s1, const char *s2));
LDAP_LIBC_F(int) (strncasecmp) LDAP_P((const char *s1, const char *s2, size_t n));
#endif

#ifndef SAFEMEMCPY
#	if defined( HAVE_MEMMOVE )
#		define SAFEMEMCPY( d, s, n ) 	memmove((d), (s), (n))
#	elif defined( HAVE_BCOPY )
#		define SAFEMEMCPY( d, s, n ) 	bcopy((s), (d), (n))
#	else
		/* nothing left but memcpy() */
#		define SAFEMEMCPY( d, s, n )	memcpy((d), (s), (n))
#	endif
#endif

#define AC_MEMCPY( d, s, n ) (SAFEMEMCPY((d),(s),(n)))
#define AC_FMEMCPY( d, s, n ) do { \
		if((n) == 1) *((char*)(d)) = *((char*)(s)); \
		else AC_MEMCPY( (d), (s), (n) ); \
	} while(0)

#ifdef NEED_MEMCMP_REPLACEMENT
	int (lutil_memcmp)(const void *b1, const void *b2, size_t len);
#define memcmp lutil_memcmp
#endif

void *(lutil_memrchr)(const void *b, int c, size_t n);
/* GNU extension (glibc >= 2.1.91), only declared when defined(_GNU_SOURCE) */
#if defined(HAVE_MEMRCHR) && defined(_GNU_SOURCE)
#define lutil_memrchr(b, c, n) memrchr(b, c, n)
#endif /* ! HAVE_MEMRCHR */

#define STRLENOF(s)	(sizeof(s)-1)

#if defined( HAVE_NONPOSIX_STRERROR_R )
#	define AC_STRERROR_R(e,b,l)		(strerror_r((e), (b), (l)))
#elif defined( HAVE_STRERROR_R )
#	define AC_STRERROR_R(e,b,l)		(strerror_r((e), (b), (l)) == 0 ? (b) : "Unknown error")
#elif defined( HAVE_SYS_ERRLIST )
#	define AC_STRERROR_R(e,b,l)		((e) > -1 && (e) < sys_nerr \
							? sys_errlist[(e)] : "Unknown error" )
#elif defined( HAVE_STRERROR )
#	define AC_STRERROR_R(e,b,l)		(strerror(e))	/* NOTE: may be NULL */
#else
#	define AC_STRERROR_R(e,b,l)		("Unknown error")
#endif

#endif /* _AC_STRING_H */
