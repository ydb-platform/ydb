/* encode.c - ber output encoding routines */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1998-2022 The OpenLDAP Foundation.
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
/* Portions Copyright (c) 1990 Regents of the University of Michigan.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms are permitted
 * provided that this notice is preserved and that due credit is given
 * to the University of Michigan at Ann Arbor. The name of the University
 * may not be used to endorse or promote products derived from this
 * software without specific prior written permission. This software
 * is provided ``as is'' without express or implied warranty.
 */
/* ACKNOWLEDGEMENTS:
 * This work was originally developed by the University of Michigan
 * (as part of U-MICH LDAP).
 */

#include "portable.h"

#include <ctype.h>
#include <limits.h>
#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/stdarg.h>
#include <ac/socket.h>
#include <ac/string.h>

#include "lber-int.h"


#define OCTET_SIZE(type) ((ber_len_t) (sizeof(type)*CHAR_BIT + 7) / 8)
#define TAGBUF_SIZE OCTET_SIZE(ber_tag_t)
#define LENBUF_SIZE (1 + OCTET_SIZE(ber_len_t))
#define HEADER_SIZE (TAGBUF_SIZE + LENBUF_SIZE)

/*
 * BER element size constrains:
 *
 * - We traditionally support a length of max 0xffffffff.  However
 *   some functions return an int length so that is their max.
 *   MAXINT_BERSIZE is the max for those functions.
 *
 * - MAXINT_BERSIZE must fit in MAXINT_BERSIZE_OCTETS octets.
 *
 * - sizeof(ber_elem_size_t) is normally MAXINT_BERSIZE_OCTETS:
 *   Big enough for MAXINT_BERSIZE, but not more.  (Larger wastes
 *   space in the working encoding and DER encoding of a sequence
 *   or set.  Smaller further limits sizes near a sequence/set.)
 *
 * ber_len_t is mostly unrelated to this.  Which may be for the best,
 * since it is also used for lengths of data that are never encoded.
 */
#define MAXINT_BERSIZE \
	(INT_MAX>0xffffffffUL ? (ber_len_t) 0xffffffffUL : INT_MAX-HEADER_SIZE)
#define MAXINT_BERSIZE_OCTETS 4
typedef ber_uint_t ber_elem_size_t; /* normally 32 bits */


/* Prepend tag to ptr, which points to the end of a tag buffer */
static unsigned char *
ber_prepend_tag( unsigned char *ptr, ber_tag_t tag )
{
	do {
		*--ptr = (unsigned char) tag & 0xffU;
	} while ( (tag >>= 8) != 0 );

	return ptr;
}

/* Prepend ber length to ptr, which points to the end of a length buffer */
static unsigned char *
ber_prepend_len( unsigned char *ptr, ber_len_t len )
{
	/*
	 * short len if it's less than 128 - one byte giving the len,
	 * with bit 8 0.
	 * long len otherwise - one byte with bit 8 set, giving the
	 * length of the length, followed by the length itself.
	 */

	*--ptr = (unsigned char) len & 0xffU;

	if ( len >= 0x80 ) {
		unsigned char *endptr = ptr--;

		while ( (len >>= 8) != 0 ) {
			*ptr-- = (unsigned char) len & 0xffU;
		}
		*ptr = (unsigned char) (endptr - ptr) + 0x80U;
	}

	return ptr;
}

/* out->bv_len should be the buffer size on input */
int
ber_encode_oid( BerValue *in, BerValue *out )
{
	unsigned char *der;
	unsigned long val1, val;
	int i, j, len;
	char *ptr, *end, *inend;

	assert( in != NULL );
	assert( out != NULL );

	if ( !out->bv_val || out->bv_len < in->bv_len/2 )
		return -1;

	der = (unsigned char *) out->bv_val;
	ptr = in->bv_val;
	inend = ptr + in->bv_len;

	/* OIDs start with <0-1>.<0-39> or 2.<any>, DER-encoded 40*val1+val2 */
	if ( !isdigit( (unsigned char) *ptr )) return -1;
	val1 = strtoul( ptr, &end, 10 );
	if ( end == ptr || val1 > 2 ) return -1;
	if ( *end++ != '.' || !isdigit( (unsigned char) *end )) return -1;
	val = strtoul( end, &ptr, 10 );
	if ( ptr == end ) return -1;
	if ( val > (val1 < 2 ? 39 : LBER_OID_COMPONENT_MAX - 80) ) return -1;
	val += val1 * 40;

	for (;;) {
		if ( ptr > inend ) return -1;

		/* Write the OID component little-endian, then reverse it */
		len = 0;
		do {
			der[len++] = (val & 0xff) | 0x80;
		} while ( (val >>= 7) != 0 );
		der[0] &= 0x7f;
		for ( i = 0, j = len; i < --j; i++ ) {
			unsigned char tmp = der[i];
			der[i] = der[j];
			der[j] = tmp;
		}
		der += len;

		if ( ptr == inend )
			break;

		if ( *ptr++ != '.' ) return -1;
		if ( !isdigit( (unsigned char) *ptr )) return -1;
		val = strtoul( ptr, &end, 10 );
		if ( end == ptr || val > LBER_OID_COMPONENT_MAX ) return -1;
		ptr = end;
	}

	out->bv_len = (char *)der - out->bv_val;
	return 0;
}

static int
ber_put_int_or_enum(
	BerElement *ber,
	ber_int_t num,
	ber_tag_t tag )
{
	ber_uint_t unum;
	unsigned char sign, data[TAGBUF_SIZE+1 + OCTET_SIZE(ber_int_t)], *ptr;

	sign = 0;
	unum = num;	/* Bit fiddling should be done with unsigned values */
	if ( num < 0 ) {
		sign = 0xffU;
		unum = ~unum;
	}
	for ( ptr = &data[sizeof(data) - 1] ;; unum >>= 8 ) {
		*ptr-- = (sign ^ (unsigned char) unum) & 0xffU;
		if ( unum < 0x80 )	/* top bit at *ptr is sign bit */
			break;
	}

	*ptr = (unsigned char) (&data[sizeof(data) - 1] - ptr); /* length */
	ptr = ber_prepend_tag( ptr, tag );

	return ber_write( ber, (char *) ptr, &data[sizeof(data)] - ptr, 0 );
}

int
ber_put_enum(
	BerElement *ber,
	ber_int_t num,
	ber_tag_t tag )
{
	if ( tag == LBER_DEFAULT ) {
		tag = LBER_ENUMERATED;
	}

	return ber_put_int_or_enum( ber, num, tag );
}

int
ber_put_int(
	BerElement *ber,
	ber_int_t num,
	ber_tag_t tag )
{
	if ( tag == LBER_DEFAULT ) {
		tag = LBER_INTEGER;
	}

	return ber_put_int_or_enum( ber, num, tag );
}

int
ber_put_ostring(
	BerElement *ber,
	LDAP_CONST char *str,
	ber_len_t len,
	ber_tag_t tag )
{
	int rc;
	unsigned char header[HEADER_SIZE], *ptr;

	if ( tag == LBER_DEFAULT ) {
		tag = LBER_OCTETSTRING;
	}

	if ( len > MAXINT_BERSIZE ) {
		return -1;
	}

	ptr = ber_prepend_len( &header[sizeof(header)], len );
	ptr = ber_prepend_tag( ptr, tag );

	rc = ber_write( ber, (char *) ptr, &header[sizeof(header)] - ptr, 0 );
	if ( rc >= 0 && ber_write( ber, str, len, 0 ) >= 0 ) {
		/* length(tag + length + contents) */
		return rc + (int) len;
	}

	return -1;
}

int
ber_put_berval(
	BerElement *ber,
	struct berval *bv,
	ber_tag_t tag )
{
	if( bv == NULL || bv->bv_len == 0 ) {
		return ber_put_ostring( ber, "", (ber_len_t) 0, tag );
	}

	return ber_put_ostring( ber, bv->bv_val, bv->bv_len, tag );
}

int
ber_put_string(
	BerElement *ber,
	LDAP_CONST char *str,
	ber_tag_t tag )
{
	assert( str != NULL );

	return ber_put_ostring( ber, str, strlen( str ), tag );
}

int
ber_put_bitstring(
	BerElement *ber,
	LDAP_CONST char *str,
	ber_len_t blen /* in bits */,
	ber_tag_t tag )
{
	int rc;
	ber_len_t		len;
	unsigned char	unusedbits, header[HEADER_SIZE + 1], *ptr;

	if ( tag == LBER_DEFAULT ) {
		tag = LBER_BITSTRING;
	}

	unusedbits = (unsigned char) -blen & 7;
	len = blen / 8 + (unusedbits != 0); /* (blen+7)/8 without overflow */
	if ( len >= MAXINT_BERSIZE ) {
		return -1;
	}

	header[sizeof(header) - 1] = unusedbits;
	ptr = ber_prepend_len( &header[sizeof(header) - 1], len + 1 );
	ptr = ber_prepend_tag( ptr, tag );

	rc = ber_write( ber, (char *) ptr, &header[sizeof(header)] - ptr, 0 );
	if ( rc >= 0 && ber_write( ber, str, len, 0 ) >= 0 ) {
		/* length(tag + length + unused bit count + bitstring) */
		return rc + (int) len;
	}

	return -1;
}

int
ber_put_null( BerElement *ber, ber_tag_t tag )
{
	unsigned char data[TAGBUF_SIZE + 1], *ptr;

	if ( tag == LBER_DEFAULT ) {
		tag = LBER_NULL;
	}

	data[sizeof(data) - 1] = 0;			/* length */
	ptr = ber_prepend_tag( &data[sizeof(data) - 1], tag );

	return ber_write( ber, (char *) ptr, &data[sizeof(data)] - ptr, 0 );
}

int
ber_put_boolean(
	BerElement *ber,
	ber_int_t boolval,
	ber_tag_t tag )
{
	unsigned char data[TAGBUF_SIZE + 2], *ptr;

	if ( tag == LBER_DEFAULT )
		tag = LBER_BOOLEAN;

	data[sizeof(data) - 1] = boolval ? 0xff : 0;
	data[sizeof(data) - 2] = 1;			/* length */
	ptr = ber_prepend_tag( &data[sizeof(data) - 2], tag );

	return ber_write( ber, (char *) ptr, &data[sizeof(data)] - ptr, 0 );
}


/* Max number of length octets in a sequence or set, normally 5 */
#define SOS_LENLEN (1 + (sizeof(ber_elem_size_t) > MAXINT_BERSIZE_OCTETS ? \
		(ber_len_t) sizeof(ber_elem_size_t) : MAXINT_BERSIZE_OCTETS))

/* Header of incomplete sequence or set */
typedef struct seqorset_header {
	char xtagbuf[TAGBUF_SIZE + 1];	/* room for tag + len(tag or len) */
	union {
		ber_elem_size_t offset;		/* enclosing sequence/set */
		char padding[SOS_LENLEN-1];	/* for final length encoding */
	} next_sos;
#	define SOS_TAG_END(header) ((unsigned char *) &(header).next_sos - 1)
} Seqorset_header;

/* Start a sequence or set */
static int
ber_start_seqorset(
	BerElement *ber,
	ber_tag_t tag )
{
	/*
	 * Write the tag and SOS_LENLEN octets reserved for length, to ber.
	 * For now, length octets = (tag length, previous ber_sos_inner).
	 *
	 * Update ber_sos_inner and the write-cursor ber_sos_ptr.  ber_ptr
	 * will not move until the outermost sequence or set is complete.
	 */

	Seqorset_header	header;
	unsigned char	*headptr;
	ber_len_t		taglen, headlen;
	char			*dest, **p;

	assert( ber != NULL );
	assert( LBER_VALID( ber ) );

	if ( ber->ber_sos_ptr == NULL ) {	/* outermost sequence/set? */
		header.next_sos.offset = 0;
		p = &ber->ber_ptr;
	} else {
		if ( (ber_len_t) -1 > (ber_elem_size_t) -1 ) {
			if ( ber->ber_sos_inner > (ber_elem_size_t) -1 )
				return -1;
		}
		header.next_sos.offset = ber->ber_sos_inner;
		p = &ber->ber_sos_ptr;
	}
	headptr = ber_prepend_tag( SOS_TAG_END(header), tag );
	*SOS_TAG_END(header) = taglen = SOS_TAG_END(header) - headptr;
	headlen = taglen + SOS_LENLEN;

	/* As ber_write(,headptr,headlen,) except update ber_sos_ptr, not *p */
	if ( headlen > (ber_len_t) (ber->ber_end - *p) ) {
		if ( ber_realloc( ber, headlen ) != 0 )
			return -1;
	}
	dest = *p;
	AC_MEMCPY( dest, headptr, headlen );
	ber->ber_sos_ptr = dest + headlen;

	ber->ber_sos_inner = dest + taglen - ber->ber_buf;

	/*
	 * Do not return taglen + SOS_LENLEN here - then ber_put_seqorset()
	 * should return lenlen - SOS_LENLEN + len, which can be < 0.
	 */
	return 0;
}

int
ber_start_seq( BerElement *ber, ber_tag_t tag )
{
	if ( tag == LBER_DEFAULT ) {
		tag = LBER_SEQUENCE;
	}

	return ber_start_seqorset( ber, tag );
}

int
ber_start_set( BerElement *ber, ber_tag_t tag )
{
	if ( tag == LBER_DEFAULT ) {
		tag = LBER_SET;
	}

	return ber_start_seqorset( ber, tag );
}

/* End a sequence or set */
static int
ber_put_seqorset( BerElement *ber )
{
	Seqorset_header	header;
	unsigned char	*lenptr;	/* length octets in the sequence/set */
	ber_len_t		len;		/* length(contents) */
	ber_len_t		xlen;		/* len + length(length) */

	assert( ber != NULL );
	assert( LBER_VALID( ber ) );

	if ( ber->ber_sos_ptr == NULL ) return -1;

	lenptr = (unsigned char *) ber->ber_buf + ber->ber_sos_inner;
	xlen = ber->ber_sos_ptr - (char *) lenptr;
	if ( xlen > MAXINT_BERSIZE + SOS_LENLEN ) {
		return -1;
	}

	/* Extract sequence/set information from length octets */
	memcpy( SOS_TAG_END(header), lenptr, SOS_LENLEN );

	/* Store length, and close gap of leftover reserved length octets */
	len = xlen - SOS_LENLEN;
	if ( !(ber->ber_options & LBER_USE_DER) ) {
		int i;
		lenptr[0] = SOS_LENLEN - 1 + 0x80; /* length(length)-1 */
		for( i = SOS_LENLEN; --i > 0; len >>= 8 ) {
			lenptr[i] = len & 0xffU;
		}
	} else {
		unsigned char *p = ber_prepend_len( lenptr + SOS_LENLEN, len );
		ber_len_t unused = p - lenptr;
		if ( unused != 0 ) {
			/* length(length) < the reserved SOS_LENLEN bytes */
			xlen -= unused;
			AC_MEMCPY( lenptr, p, xlen );
			ber->ber_sos_ptr = (char *) lenptr + xlen;
		}
	}

	ber->ber_sos_inner = header.next_sos.offset;
	if ( header.next_sos.offset == 0 ) { /* outermost sequence/set? */
		/* The ber_ptr is at the set/seq start - move it to the end */
		ber->ber_ptr = ber->ber_sos_ptr;
		ber->ber_sos_ptr = NULL;
	}

	return xlen + *SOS_TAG_END(header); /* lenlen + len + taglen */
}

int
ber_put_seq( BerElement *ber )
{
	return ber_put_seqorset( ber );
}

int
ber_put_set( BerElement *ber )
{
	return ber_put_seqorset( ber );
}

/* N tag */
static ber_tag_t lber_int_null = 0;

/* VARARGS */
int
ber_printf( BerElement *ber, LDAP_CONST char *fmt, ... )
{
	va_list		ap;
	char		*s, **ss;
	struct berval	*bv, **bvp;
	int		rc;
	ber_int_t	i;
	ber_len_t	len;

	assert( ber != NULL );
	assert( fmt != NULL );
	assert( LBER_VALID( ber ) );

	va_start( ap, fmt );

	for ( rc = 0; *fmt && rc != -1; fmt++ ) {
		switch ( *fmt ) {
		case '!': { /* hook */
				BEREncodeCallback *f;
				void *p;

				ber->ber_usertag = 0;

				f = va_arg( ap, BEREncodeCallback * );
				p = va_arg( ap, void * );
				rc = (*f)( ber, p );

				if ( ber->ber_usertag ) {
					goto next;
				}
			} break;

		case 'b':	/* boolean */
			i = va_arg( ap, ber_int_t );
			rc = ber_put_boolean( ber, i, ber->ber_tag );
			break;

		case 'i':	/* int */
			i = va_arg( ap, ber_int_t );
			rc = ber_put_int( ber, i, ber->ber_tag );
			break;

		case 'e':	/* enumeration */
			i = va_arg( ap, ber_int_t );
			rc = ber_put_enum( ber, i, ber->ber_tag );
			break;

		case 'n':	/* null */
			rc = ber_put_null( ber, ber->ber_tag );
			break;

		case 'N':	/* Debug NULL */
			rc = 0;
			if( lber_int_null != 0 ) {
				/* Insert NULL to ensure peer ignores unknown tags */
				rc = ber_put_null( ber, lber_int_null );
			}
			break;

		case 'o':	/* octet string (non-null terminated) */
			s = va_arg( ap, char * );
			len = va_arg( ap, ber_len_t );
			rc = ber_put_ostring( ber, s, len, ber->ber_tag );
			break;

		case 'O':	/* berval octet string */
			bv = va_arg( ap, struct berval * );
			if( bv == NULL ) break;
			rc = ber_put_berval( ber, bv, ber->ber_tag );
			break;

		case 's':	/* string */
			s = va_arg( ap, char * );
			rc = ber_put_string( ber, s, ber->ber_tag );
			break;

		case 'B':	/* bit string */
		case 'X':	/* bit string (deprecated) */
			s = va_arg( ap, char * );
			len = va_arg( ap, ber_len_t );	/* in bits */
			rc = ber_put_bitstring( ber, s, len, ber->ber_tag );
			break;

		case 't':	/* tag for the next element */
			ber->ber_tag = va_arg( ap, ber_tag_t );
			goto next;

		case 'v':	/* vector of strings */
			if ( (ss = va_arg( ap, char ** )) == NULL )
				break;
			for ( i = 0; ss[i] != NULL; i++ ) {
				if ( (rc = ber_put_string( ber, ss[i],
				    ber->ber_tag )) == -1 )
					break;
			}
			break;

		case 'V':	/* sequences of strings + lengths */
			if ( (bvp = va_arg( ap, struct berval ** )) == NULL )
				break;
			for ( i = 0; bvp[i] != NULL; i++ ) {
				if ( (rc = ber_put_berval( ber, bvp[i],
				    ber->ber_tag )) == -1 )
					break;
			}
			break;

		case 'W':	/* BerVarray */
			if ( (bv = va_arg( ap, BerVarray )) == NULL )
				break;
			for ( i = 0; bv[i].bv_val != NULL; i++ ) {
				if ( (rc = ber_put_berval( ber, &bv[i],
				    ber->ber_tag )) == -1 )
					break;
			}
			break;

		case '{':	/* begin sequence */
			rc = ber_start_seq( ber, ber->ber_tag );
			break;

		case '}':	/* end sequence */
			rc = ber_put_seqorset( ber );
			break;

		case '[':	/* begin set */
			rc = ber_start_set( ber, ber->ber_tag );
			break;

		case ']':	/* end set */
			rc = ber_put_seqorset( ber );
			break;

		default:
			if( ber->ber_debug ) {
				ber_log_printf( LDAP_DEBUG_ANY, ber->ber_debug,
					"ber_printf: unknown fmt %c\n", *fmt );
			}
			rc = -1;
			break;
		}

		ber->ber_tag = LBER_DEFAULT;
	next:;
	}

	va_end( ap );

	return rc;
}
