/* utf-8.c -- Basic UTF-8 routines */
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
/* Basic UTF-8 routines
 *
 * These routines are "dumb".  Though they understand UTF-8,
 * they don't grok Unicode.  That is, they can push bits,
 * but don't have a clue what the bits represent.  That's
 * good enough for use with the LDAP Client SDK.
 *
 * These routines are not optimized.
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap_utf8.h"

#include "ldap-int.h"
#include "ldap_defaults.h"

/*
 * return the number of bytes required to hold the
 * NULL-terminated UTF-8 string NOT INCLUDING the
 * termination.
 */
ber_len_t ldap_utf8_bytes( const char * p )
{
	ber_len_t bytes;

	for( bytes=0; p[bytes]; bytes++ ) {
		/* EMPTY */ ;
	}

	return bytes;
}

ber_len_t ldap_utf8_chars( const char * p )
{
	/* could be optimized and could check for invalid sequences */
	ber_len_t chars=0;

	for( ; *p ; LDAP_UTF8_INCR(p) ) {
		chars++;
	}

	return chars;
}

/* return offset to next character */
int ldap_utf8_offset( const char * p )
{
	return LDAP_UTF8_NEXT(p) - p;
}

/*
 * Returns length indicated by first byte.
 */
const char ldap_utf8_lentab[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 0, 0 };

int ldap_utf8_charlen( const char * p )
{
	if (!(*p & 0x80))
		return 1;

	return ldap_utf8_lentab[*(const unsigned char *)p ^ 0x80];
}

/*
 * Make sure the UTF-8 char used the shortest possible encoding
 * returns charlen if valid, 0 if not. 
 *
 * Here are the valid UTF-8 encodings, taken from RFC 2279 page 4.
 * The table is slightly modified from that of the RFC.
 *
 * UCS-4 range (hex)      UTF-8 sequence (binary)
 * 0000 0000-0000 007F   0.......
 * 0000 0080-0000 07FF   110++++. 10......
 * 0000 0800-0000 FFFF   1110++++ 10+..... 10......
 * 0001 0000-001F FFFF   11110+++ 10++.... 10...... 10......
 * 0020 0000-03FF FFFF   111110++ 10+++... 10...... 10...... 10......
 * 0400 0000-7FFF FFFF   1111110+ 10++++.. 10...... 10...... 10...... 10......
 *
 * The '.' bits are "don't cares". When validating a UTF-8 sequence,
 * at least one of the '+' bits must be set, otherwise the character
 * should have been encoded in fewer octets. Note that in the two-octet
 * case, only the first octet needs to be validated, and this is done
 * in the ldap_utf8_lentab[] above.
 */

/* mask of required bits in second octet */
#undef c
#define c const char
c ldap_utf8_mintab[] = {
	(c)0x20, (c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80,
	(c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80,
	(c)0x30, (c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80, (c)0x80,
	(c)0x38, (c)0x80, (c)0x80, (c)0x80, (c)0x3c, (c)0x80, (c)0x00, (c)0x00 };
#undef c

int ldap_utf8_charlen2( const char * p )
{
	int i = LDAP_UTF8_CHARLEN( p );

	if ( i > 2 ) {
		if ( !( ldap_utf8_mintab[*p & 0x1f] & p[1] ) )
			i = 0;
	}
	return i;
}

/* conv UTF-8 to UCS-4, useful for comparisons */
ldap_ucs4_t ldap_x_utf8_to_ucs4( const char * p )
{
    const unsigned char *c = (const unsigned char *) p;
    ldap_ucs4_t ch;
	int len, i;
	static unsigned char mask[] = {
		0, 0x7f, 0x1f, 0x0f, 0x07, 0x03, 0x01 };

	len = LDAP_UTF8_CHARLEN2(p, len);

	if( len == 0 ) return LDAP_UCS4_INVALID;

	ch = c[0] & mask[len];

	for(i=1; i < len; i++) {
		if ((c[i] & 0xc0) != 0x80) {
			return LDAP_UCS4_INVALID;
		}

		ch <<= 6;
		ch |= c[i] & 0x3f;
	}

	return ch;
}

/* conv UCS-4 to UTF-8, not used */
int ldap_x_ucs4_to_utf8( ldap_ucs4_t c, char *buf )
{
	int len=0;
	unsigned char* p = (unsigned char *) buf;

	/* not a valid Unicode character */
	if ( c < 0 ) return 0;

	/* Just return length, don't convert */
	if(buf == NULL) {
		if( c < 0x80 ) return 1;
		else if( c < 0x800 ) return 2;
		else if( c < 0x10000 ) return 3;
		else if( c < 0x200000 ) return 4;
		else if( c < 0x4000000 ) return 5;
		else return 6;
	}

	if( c < 0x80 ) {
		p[len++] = c;

	} else if( c < 0x800 ) {
		p[len++] = 0xc0 | ( c >> 6 );
		p[len++] = 0x80 | ( c & 0x3f );

	} else if( c < 0x10000 ) {
		p[len++] = 0xe0 | ( c >> 12 );
		p[len++] = 0x80 | ( (c >> 6) & 0x3f );
		p[len++] = 0x80 | ( c & 0x3f );

	} else if( c < 0x200000 ) {
		p[len++] = 0xf0 | ( c >> 18 );
		p[len++] = 0x80 | ( (c >> 12) & 0x3f );
		p[len++] = 0x80 | ( (c >> 6) & 0x3f );
		p[len++] = 0x80 | ( c & 0x3f );

	} else if( c < 0x4000000 ) {
		p[len++] = 0xf8 | ( c >> 24 );
		p[len++] = 0x80 | ( (c >> 18) & 0x3f );
		p[len++] = 0x80 | ( (c >> 12) & 0x3f );
		p[len++] = 0x80 | ( (c >> 6) & 0x3f );
		p[len++] = 0x80 | ( c & 0x3f );

	} else /* if( c < 0x80000000 ) */ {
		p[len++] = 0xfc | ( c >> 30 );
		p[len++] = 0x80 | ( (c >> 24) & 0x3f );
		p[len++] = 0x80 | ( (c >> 18) & 0x3f );
		p[len++] = 0x80 | ( (c >> 12) & 0x3f );
		p[len++] = 0x80 | ( (c >> 6) & 0x3f );
		p[len++] = 0x80 | ( c & 0x3f );
	}

	return len;
}

#define LDAP_UCS_UTF8LEN(c)	\
	c < 0 ? 0 : (c < 0x80 ? 1 : (c < 0x800 ? 2 : (c < 0x10000 ? 3 : \
	(c < 0x200000 ? 4 : (c < 0x4000000 ? 5 : 6)))))

/* Convert a string to UTF-8 format. The input string is expected to
 * have characters of 1, 2, or 4 octets (in network byte order)
 * corresponding to the ASN.1 T61STRING, BMPSTRING, and UNIVERSALSTRING
 * types respectively. (Here T61STRING just means that there is one
 * octet per character and characters may use the high bit of the octet.
 * The characters are assumed to use ISO mappings, no provision is made
 * for converting from T.61 coding rules to Unicode.)
 */

int
ldap_ucs_to_utf8s( struct berval *ucs, int csize, struct berval *utf8s )
{
	unsigned char *in, *end;
	char *ptr;
	ldap_ucs4_t u;
	int i, l = 0;

	utf8s->bv_val = NULL;
	utf8s->bv_len = 0;

	in = (unsigned char *)ucs->bv_val;

	/* Make sure we stop at an even multiple of csize */
	end = in + ( ucs->bv_len & ~(csize-1) );
	
	for (; in < end; ) {
		u = *in++;
		if (csize > 1) {
			u <<= 8;
			u |= *in++;
		}
		if (csize > 2) {
			u <<= 8;
			u |= *in++;
			u <<= 8;
			u |= *in++;
		}
		i = LDAP_UCS_UTF8LEN(u);
		if (i == 0)
			return LDAP_INVALID_SYNTAX;
		l += i;
	}

	utf8s->bv_val = LDAP_MALLOC( l+1 );
	if (utf8s->bv_val == NULL)
		return LDAP_NO_MEMORY;
	utf8s->bv_len = l;

	ptr = utf8s->bv_val;
	for (in = (unsigned char *)ucs->bv_val; in < end; ) {
		u = *in++;
		if (csize > 1) {
			u <<= 8;
			u |= *in++;
		}
		if (csize > 2) {
			u <<= 8;
			u |= *in++;
			u <<= 8;
			u |= *in++;
		}
		ptr += ldap_x_ucs4_to_utf8(u, ptr);
	}
	*ptr = '\0';
	return LDAP_SUCCESS;
}

/*
 * Advance to the next UTF-8 character
 *
 * Ignores length of multibyte character, instead rely on
 * continuation markers to find start of next character.
 * This allows for "resyncing" of when invalid characters
 * are provided provided the start of the next character
 * is appears within the 6 bytes examined.
 */
char* ldap_utf8_next( const char * p )
{
	int i;
	const unsigned char *u = (const unsigned char *) p;

	if( LDAP_UTF8_ISASCII(u) ) {
		return (char *) &p[1];
	}

	for( i=1; i<6; i++ ) {
		if ( ( u[i] & 0xc0 ) != 0x80 ) {
			return (char *) &p[i];
		}
	}

	return (char *) &p[i];
}

/*
 * Advance to the previous UTF-8 character
 *
 * Ignores length of multibyte character, instead rely on
 * continuation markers to find start of next character.
 * This allows for "resyncing" of when invalid characters
 * are provided provided the start of the next character
 * is appears within the 6 bytes examined.
 */
char* ldap_utf8_prev( const char * p )
{
	int i;
	const unsigned char *u = (const unsigned char *) p;

	for( i=-1; i>-6 ; i-- ) {
		if ( ( u[i] & 0xc0 ) != 0x80 ) {
			return (char *) &p[i];
		}
	}

	return (char *) &p[i];
}

/*
 * Copy one UTF-8 character from src to dst returning
 * number of bytes copied.
 *
 * Ignores length of multibyte character, instead rely on
 * continuation markers to find start of next character.
 * This allows for "resyncing" of when invalid characters
 * are provided provided the start of the next character
 * is appears within the 6 bytes examined.
 */
int ldap_utf8_copy( char* dst, const char *src )
{
	int i;
	const unsigned char *u = (const unsigned char *) src;

	dst[0] = src[0];

	if( LDAP_UTF8_ISASCII(u) ) {
		return 1;
	}

	for( i=1; i<6; i++ ) {
		if ( ( u[i] & 0xc0 ) != 0x80 ) {
			return i; 
		}
		dst[i] = src[i];
	}

	return i;
}

#ifndef UTF8_ALPHA_CTYPE
/*
 * UTF-8 ctype routines
 * Only deals with characters < 0x80 (ie: US-ASCII)
 */

int ldap_utf8_isascii( const char * p )
{
	unsigned c = * (const unsigned char *) p;
	return LDAP_ASCII(c);
}

int ldap_utf8_isdigit( const char * p )
{
	unsigned c = * (const unsigned char *) p;

	if(!LDAP_ASCII(c)) return 0;

	return LDAP_DIGIT( c );
}

int ldap_utf8_isxdigit( const char * p )
{
	unsigned c = * (const unsigned char *) p;

	if(!LDAP_ASCII(c)) return 0;

	return LDAP_HEX(c);
}

int ldap_utf8_isspace( const char * p )
{
	unsigned c = * (const unsigned char *) p;

	if(!LDAP_ASCII(c)) return 0;

	switch(c) {
	case ' ':
	case '\t':
	case '\n':
	case '\r':
	case '\v':
	case '\f':
		return 1;
	}

	return 0;
}

/*
 * These are not needed by the C SDK and are
 * not "good enough" for general use.
 */
int ldap_utf8_isalpha( const char * p )
{
	unsigned c = * (const unsigned char *) p;

	if(!LDAP_ASCII(c)) return 0;

	return LDAP_ALPHA(c);
}

int ldap_utf8_isalnum( const char * p )
{
	unsigned c = * (const unsigned char *) p;

	if(!LDAP_ASCII(c)) return 0;

	return LDAP_ALNUM(c);
}

int ldap_utf8_islower( const char * p )
{
	unsigned c = * (const unsigned char *) p;

	if(!LDAP_ASCII(c)) return 0;

	return LDAP_LOWER(c);
}

int ldap_utf8_isupper( const char * p )
{
	unsigned c = * (const unsigned char *) p;

	if(!LDAP_ASCII(c)) return 0;

	return LDAP_UPPER(c);
}
#endif


/*
 * UTF-8 string routines
 */

/* like strchr() */
char * (ldap_utf8_strchr)( const char *str, const char *chr )
{
	for( ; *str != '\0'; LDAP_UTF8_INCR(str) ) {
		if( ldap_x_utf8_to_ucs4( str ) == ldap_x_utf8_to_ucs4( chr ) ) {
			return (char *) str;
		} 
	}

	return NULL;
}

/* like strcspn() but returns number of bytes, not characters */
ber_len_t (ldap_utf8_strcspn)( const char *str, const char *set )
{
	const char *cstr;
	const char *cset;

	for( cstr = str; *cstr != '\0'; LDAP_UTF8_INCR(cstr) ) {
		for( cset = set; *cset != '\0'; LDAP_UTF8_INCR(cset) ) {
			if( ldap_x_utf8_to_ucs4( cstr ) == ldap_x_utf8_to_ucs4( cset ) ) {
				return cstr - str;
			} 
		}
	}

	return cstr - str;
}

/* like strspn() but returns number of bytes, not characters */
ber_len_t (ldap_utf8_strspn)( const char *str, const char *set )
{
	const char *cstr;
	const char *cset;

	for( cstr = str; *cstr != '\0'; LDAP_UTF8_INCR(cstr) ) {
		for( cset = set; ; LDAP_UTF8_INCR(cset) ) {
			if( *cset == '\0' ) {
				return cstr - str;
			}

			if( ldap_x_utf8_to_ucs4( cstr ) == ldap_x_utf8_to_ucs4( cset ) ) {
				break;
			} 
		}
	}

	return cstr - str;
}

/* like strpbrk(), replaces strchr() as well */
char *(ldap_utf8_strpbrk)( const char *str, const char *set )
{
	for( ; *str != '\0'; LDAP_UTF8_INCR(str) ) {
		const char *cset;

		for( cset = set; *cset != '\0'; LDAP_UTF8_INCR(cset) ) {
			if( ldap_x_utf8_to_ucs4( str ) == ldap_x_utf8_to_ucs4( cset ) ) {
				return (char *) str;
			} 
		}
	}

	return NULL;
}

/* like strtok_r(), not strtok() */
char *(ldap_utf8_strtok)(char *str, const char *sep, char **last)
{
	char *begin;
	char *end;

	if( last == NULL ) return NULL;

	begin = str ? str : *last;

	begin += ldap_utf8_strspn( begin, sep );

	if( *begin == '\0' ) {
		*last = NULL;
		return NULL;
	}

	end = &begin[ ldap_utf8_strcspn( begin, sep ) ];

	if( *end != '\0' ) {
		char *next = LDAP_UTF8_NEXT( end );
		*end = '\0';
		end = next;
	}

	*last = end;
	return begin;
}
