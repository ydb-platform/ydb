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

/*
 * ldap_pvt_uc.h - Header for Unicode functions.
 * These are meant to be used by the OpenLDAP distribution only.
 * These should be named ldap_pvt_....()
 */

#ifndef _LDAP_PVT_UC_H
#define _LDAP_PVT_UC_H 1

#include <lber.h>				/* get ber_slen_t */

#include <ac/bytes.h>
#include "../libraries/liblunicode/ucdata/ucdata.h"

LDAP_BEGIN_DECL

/*
 * UTF-8 (in utf-8.c)
 */

/* UCDATA uses UCS-2 passed in a 4 byte unsigned int */
typedef ac_uint4 ldap_unicode_t;

/* Convert a string with csize octets per character to UTF-8 */
LDAP_F( int ) ldap_ucs_to_utf8s LDAP_P((
	struct berval *ucs, int csize, struct berval *utf8s ));


/* returns the number of bytes in the UTF-8 string */
LDAP_F (ber_len_t) ldap_utf8_bytes( const char * );
/* returns the number of UTF-8 characters in the string */
LDAP_F (ber_len_t) ldap_utf8_chars( const char * );
/* returns the length (in bytes) of the UTF-8 character */
LDAP_F (int) ldap_utf8_offset( const char * );
/* returns the length (in bytes) indicated by the UTF-8 character */
LDAP_F (int) ldap_utf8_charlen( const char * );

/* returns the length (in bytes) indicated by the UTF-8 character
 * also checks that shortest possible encoding was used
 */
LDAP_F (int) ldap_utf8_charlen2( const char * );

/* copies a UTF-8 character and returning number of bytes copied */
LDAP_F (int) ldap_utf8_copy( char *, const char *);

/* returns pointer of next UTF-8 character in string */
LDAP_F (char*) ldap_utf8_next( const char * );
/* returns pointer of previous UTF-8 character in string */
LDAP_F (char*) ldap_utf8_prev( const char * );

/* primitive ctype routines -- not aware of non-ascii characters */
LDAP_F (int) ldap_utf8_isascii( const char * );
LDAP_F (int) ldap_utf8_isalpha( const char * );
LDAP_F (int) ldap_utf8_isalnum( const char * );
LDAP_F (int) ldap_utf8_isdigit( const char * );
LDAP_F (int) ldap_utf8_isxdigit( const char * );
LDAP_F (int) ldap_utf8_isspace( const char * );

/* span characters not in set, return bytes spanned */
LDAP_F (ber_len_t) ldap_utf8_strcspn( const char* str, const char *set);
/* span characters in set, return bytes spanned */
LDAP_F (ber_len_t) ldap_utf8_strspn( const char* str, const char *set);
/* return first occurrence of character in string */
LDAP_F (char *) ldap_utf8_strchr( const char* str, const char *chr);
/* return first character of set in string */
LDAP_F (char *) ldap_utf8_strpbrk( const char* str, const char *set);
/* reentrant tokenizer */
LDAP_F (char*) ldap_utf8_strtok( char* sp, const char* sep, char **last);

/* Optimizations */
LDAP_V (const char) ldap_utf8_lentab[128];
LDAP_V (const char) ldap_utf8_mintab[32];

#define LDAP_UTF8_ISASCII(p) ( !(*(const unsigned char *)(p) & 0x80 ) )
#define LDAP_UTF8_CHARLEN(p) ( LDAP_UTF8_ISASCII(p) \
	? 1 : ldap_utf8_lentab[*(const unsigned char *)(p) ^ 0x80] )

/* This is like CHARLEN but additionally validates to make sure
 * the char used the shortest possible encoding.
 * 'l' is used to temporarily hold the result of CHARLEN.
 */
#define LDAP_UTF8_CHARLEN2(p, l) ( ( ( l = LDAP_UTF8_CHARLEN( p )) < 3 || \
	( ldap_utf8_mintab[*(const unsigned char *)(p) & 0x1f] & (p)[1] ) ) ? \
	l : 0 )

#define LDAP_UTF8_OFFSET(p) ( LDAP_UTF8_ISASCII(p) \
	? 1 : ldap_utf8_offset((p)) )

#define LDAP_UTF8_COPY(d,s) ( LDAP_UTF8_ISASCII(s) \
	? (*(d) = *(s), 1) : ldap_utf8_copy((d),(s)) )

#define LDAP_UTF8_NEXT(p) (	LDAP_UTF8_ISASCII(p) \
	? (char *)(p)+1 : ldap_utf8_next((p)) )

#define LDAP_UTF8_INCR(p) ((p) = LDAP_UTF8_NEXT(p))

/* For symmetry */
#define LDAP_UTF8_PREV(p) (ldap_utf8_prev((p)))
#define LDAP_UTF8_DECR(p) ((p)=LDAP_UTF8_PREV((p)))


/* these probably should be renamed */
LDAP_LUNICODE_F(int) ucstrncmp(
	const ldap_unicode_t *,
	const ldap_unicode_t *,
	ber_len_t );

LDAP_LUNICODE_F(int) ucstrncasecmp(
	const ldap_unicode_t *,
	const ldap_unicode_t *,
	ber_len_t );

LDAP_LUNICODE_F(ldap_unicode_t *) ucstrnchr(
	const ldap_unicode_t *,
	ber_len_t,
	ldap_unicode_t );

LDAP_LUNICODE_F(ldap_unicode_t *) ucstrncasechr(
	const ldap_unicode_t *,
	ber_len_t,
	ldap_unicode_t );

LDAP_LUNICODE_F(void) ucstr2upper(
	ldap_unicode_t *,
	ber_len_t );

#define LDAP_UTF8_NOCASEFOLD	0x0U
#define LDAP_UTF8_CASEFOLD	0x1U
#define LDAP_UTF8_ARG1NFC	0x2U
#define LDAP_UTF8_ARG2NFC	0x4U
#define LDAP_UTF8_APPROX	0x8U

LDAP_LUNICODE_F(struct berval *) UTF8bvnormalize(
	struct berval *,
	struct berval *,
	unsigned,
	void *memctx );

LDAP_LUNICODE_F(int) UTF8bvnormcmp(
	struct berval *,
	struct berval *,
	unsigned,
	void *memctx );

LDAP_END_DECL

#endif

