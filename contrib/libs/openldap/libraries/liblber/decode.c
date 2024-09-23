/* decode.c - ber input decoding routines */
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

#include <stdio.h>

#include <ac/stdlib.h>
#include <ac/stdarg.h>
#include <ac/string.h>
#include <ac/socket.h>

#include "lber-int.h"


/* out->bv_len should be the buffer size on input */
int
ber_decode_oid( BerValue *in, BerValue *out )
{
	const unsigned char *der;
	unsigned long val;
	unsigned val1;
	ber_len_t i;
	char *ptr;

	assert( in != NULL );
	assert( out != NULL );

	/* need 4 chars/inbyte + \0 for input={7f 7f 7f...} */
	if ( !out->bv_val || (out->bv_len+3)/4 <= in->bv_len )
		return -1;

	ptr = NULL;
	der = (unsigned char *) in->bv_val;
	val = 0;
	for ( i=0; i < in->bv_len; i++ ) {
		val |= der[i] & 0x7f;
		if ( !( der[i] & 0x80 )) {
			if ( ptr == NULL ) {
				/* Initial "x.y": val=x*40+y, x<=2, y<40 if x<2 */
				ptr = out->bv_val;
				val1 = (val < 80 ? val/40 : 2);
				val -= val1*40;
				ptr += sprintf( ptr, "%u", val1 );
			}
			ptr += sprintf( ptr, ".%lu", val );
			val = 0;
		} else if ( val - 1UL < LBER_OID_COMPONENT_MAX >> 7 ) {
			val <<= 7;
		} else {
			/* val would overflow, or is 0 from invalid initial 0x80 octet */
			return -1;
		}
	}
	if ( ptr == NULL || val != 0 )
		return -1;

	out->bv_len = ptr - out->bv_val;
	return 0;
}

/* Return tag, with *bv = rest of element (starting at length octets) */
static ber_tag_t
ber_tag_and_rest( const BerElement *ber, struct berval *bv )
{
	ber_tag_t	tag;
	ptrdiff_t	rest;
	unsigned char	*ptr;

	assert( ber != NULL );
	assert( LBER_VALID( ber ) );

	ptr = (unsigned char *) ber->ber_ptr;
	rest = (unsigned char *) ber->ber_end - ptr;
	if ( rest <= 0 ) {
		goto fail;
	}

	tag = ber->ber_tag;
	if ( (char *) ptr == ber->ber_buf ) {
		tag = *ptr;
	}
	ptr++;
	rest--;
	if ( (tag & LBER_BIG_TAG_MASK) != LBER_BIG_TAG_MASK ) {
		goto done;
	}

	do {
		if ( rest <= 0 ) {
			break;
		}
		tag <<= 8;
		tag |= *ptr++ & 0xffU;
		rest--;

		if ( ! (tag & LBER_MORE_TAG_MASK) ) {
			goto done;
		}
	} while ( tag <= (ber_tag_t)-1 / 256 );

 fail:
	/* Error or unsupported tag size */
	tag = LBER_DEFAULT;

 done:
	bv->bv_len = rest;
	bv->bv_val = (char *) ptr;
	return tag;
}

/* Return the tag - LBER_DEFAULT returned means trouble */
ber_tag_t
ber_get_tag( BerElement *ber )
{
	struct berval bv;
	ber_tag_t tag = ber_tag_and_rest( ber, &bv );

	ber->ber_ptr = bv.bv_val;
	return tag;
}

/* Return next element's tag and point *bv at its contents in-place */
ber_tag_t
ber_peek_element( const BerElement *ber, struct berval *bv )
{
	ber_tag_t	tag;
	ber_len_t	len, rest;
	unsigned	i;
	unsigned char *ptr;

	assert( bv != NULL );

	/*
	 * Any ber element looks like this: tag length contents.
	 * Assuming everything's ok, we return the tag, and point
	 * bv at the contents.
	 *
	 * Assumptions:
	 *	1) definite lengths
	 *	2) primitive encodings used whenever possible
	 */

	len = 0;

	/*
	 * First, we read the tag.
	 */
	tag = ber_tag_and_rest( ber, bv );

	rest = bv->bv_len;
	ptr = (unsigned char *) bv->bv_val;
	if ( tag == LBER_DEFAULT || rest == 0 ) {
		goto fail;
	}

	/*
	 * Next, read the length.  The first octet determines the length
	 * of the length.	If bit 8 is 0, the length is the short form,
	 * otherwise if the octet != 0x80 it's the long form, otherwise
	 * the ber element has the unsupported indefinite-length format.
	 * Lengths that do not fit in a ber_len_t are not accepted.
	 */

	len = *ptr++;
	rest--;

	if ( len & 0x80U ) {
		len &= 0x7fU;
		if ( len - 1U > sizeof(ber_len_t) - 1U || rest < len ) {
			/* Indefinite-length/too long length/not enough data */
			goto fail;
		}

		rest -= len;
		i = len;
		for( len = *ptr++ & 0xffU; --i; len |= *ptr++ & 0xffU ) {
			len <<= 8;
		}
	}

	/* BER element should have enough data left */
	if( len > rest ) {
	fail:
		tag = LBER_DEFAULT;
	}

	bv->bv_len = len;
	bv->bv_val = (char *) ptr;
	return tag;
}

/* Move past next element, point *bv at it in-place, and return its tag.
 * The caller may \0-terminate *bv, as next octet is saved in ber->ber_tag.
 * Similar to ber_get_stringbv(ber, bv, LBER_BV_NOTERM) except on error.
 */
ber_tag_t
ber_skip_element( BerElement *ber, struct berval *bv )
{
	ber_tag_t tag = ber_peek_element( ber, bv );

	if ( tag != LBER_DEFAULT ) {
		ber->ber_ptr = bv->bv_val + bv->bv_len;
		ber->ber_tag = *(unsigned char *) ber->ber_ptr;
	}

	return tag;
}

/* Move past next element, point *bv at the complete element in-place, and
 * return its tag. The caller may \0-terminate *bv, as next octet is saved in
 * ber->ber_tag. Similar to ber_skip_element(ber, bv) except the tag+length
 * header is also included in *bv.
 */
ber_tag_t
ber_skip_raw( BerElement *ber, struct berval *bv )
{
	char		*val = ber->ber_ptr;
	ber_tag_t	tag = ber_skip_element( ber, bv );

	if ( tag != LBER_DEFAULT ) {
		bv->bv_len += bv->bv_val - val;
		bv->bv_val = val;
	}

	return tag;
}

ber_tag_t
ber_peek_tag(
	BerElement *ber,
	ber_len_t *len )
{
	struct berval bv;
	ber_tag_t tag = ber_peek_element( ber, &bv );

	*len = bv.bv_len;
	return tag;
}

ber_tag_t
ber_skip_tag( BerElement *ber, ber_len_t *lenp )
{
	struct berval bv;
	ber_tag_t tag = ber_peek_element( ber, &bv );

	ber->ber_ptr = bv.bv_val;
	ber->ber_tag = *(unsigned char *) ber->ber_ptr;

	*lenp = bv.bv_len;
	return tag;
}

ber_tag_t
ber_get_int(
	BerElement *ber,
	ber_int_t *num )
{
	struct berval bv;
	ber_tag_t	tag = ber_skip_element( ber, &bv );

	if ( tag == LBER_DEFAULT ) {
		return tag;
	}

	return ber_decode_int( &bv, num ) ? LBER_DEFAULT : tag;
}

int
ber_decode_int( const struct berval *bv, ber_int_t *num )
{
	ber_len_t	len = bv->bv_len;
	if ( len > sizeof(ber_int_t) )
		return -1;

	assert( num != NULL );

	/* parse two's complement integer */
	if( len ) {
		unsigned char *buf = (unsigned char *) bv->bv_val;
		ber_len_t i;
		ber_int_t netnum = buf[0] & 0xff;

		/* sign extend */
		netnum = (netnum ^ 0x80) - 0x80;

		/* shift in the bytes */
		for( i = 1; i < len; i++ ) {
			netnum = (netnum << 8 ) | buf[i];
		}

		*num = netnum;

	} else {
		*num = 0;
	}

	return 0;
}

ber_tag_t
ber_get_enum(
	BerElement *ber,
	ber_int_t *num )
{
	return ber_get_int( ber, num );
}

ber_tag_t
ber_get_stringb(
	BerElement *ber,
	char *buf,
	ber_len_t *len )
{
	struct berval bv;
	ber_tag_t	tag;

	if ( (tag = ber_skip_element( ber, &bv )) == LBER_DEFAULT ) {
		return LBER_DEFAULT;
	}

	/* must fit within allocated space with termination */
	if ( bv.bv_len >= *len ) {
		return LBER_DEFAULT;
	}

	memcpy( buf, bv.bv_val, bv.bv_len );
	buf[bv.bv_len] = '\0';

	*len = bv.bv_len;
	return tag;
}

/* Definitions for get_string vector
 *
 * ChArray, BvArray, and BvVec are self-explanatory.
 * BvOff is a struct berval embedded in an array of larger structures
 * of siz bytes at off bytes from the beginning of the struct.
 */
enum bgbvc { ChArray, BvArray, BvVec, BvOff };

/* Use this single cookie for state, to keep actual
 * stack use to the absolute minimum.
 */
typedef struct bgbvr {
	const enum bgbvc choice;
	const int option;	/* (ALLOC unless BvOff) | (STRING if ChArray) */
	ber_len_t siz;		/* input array element size, output count */
	ber_len_t off;		/* BvOff offset to the struct berval */
	void *result;
} bgbvr;

static ber_tag_t
ber_get_stringbvl( BerElement *ber, bgbvr *b )
{
	int i = 0, n;
	ber_tag_t tag;
	ber_len_t tot_size = 0, siz = b->siz;
	char *last, *orig;
	struct berval bv, *bvp = NULL;
	union stringbvl_u {
		char **ca;				/* ChArray */
		BerVarray ba;			/* BvArray */
		struct berval **bv;		/* BvVec */
		char *bo;				/* BvOff */
	} res;

	tag = ber_skip_tag( ber, &bv.bv_len );

	if ( tag != LBER_DEFAULT ) {
		tag = 0;
		orig = ber->ber_ptr;
		last = orig + bv.bv_len;

		for ( ; ber->ber_ptr < last; i++, tot_size += siz ) {
			if ( ber_skip_element( ber, &bv ) == LBER_DEFAULT )
				break;
		}
		if ( ber->ber_ptr != last ) {
			i = 0;
			tag = LBER_DEFAULT;
		}

		ber->ber_ptr = orig;
		ber->ber_tag = *(unsigned char *) orig;
	}

	b->siz = i;
	if ( i == 0 ) {
		return tag;
	}

	/* Allocate and NULL-terminate the result vector */
	b->result = ber_memalloc_x( tot_size + siz, ber->ber_memctx );
	if ( b->result == NULL ) {
		return LBER_DEFAULT;
	}
	switch (b->choice) {
	case ChArray:
		res.ca = b->result;
		res.ca[i] = NULL;
		break;
	case BvArray:
		res.ba = b->result;
		res.ba[i].bv_val = NULL;
		break;
	case BvVec:
		res.bv = b->result;
		res.bv[i] = NULL;
		break;
	case BvOff:
		res.bo = (char *) b->result + b->off;
		((struct berval *) (res.bo + tot_size))->bv_val = NULL;
		tot_size = 0;
		break;
	}

	n = 0;
	do {
		tag = ber_get_stringbv( ber, &bv, b->option );
		if ( tag == LBER_DEFAULT ) {
			goto failed;
		}

		/* store my result */
		switch (b->choice) {
		case ChArray:
			res.ca[n] = bv.bv_val;
			break;
		case BvArray:
			res.ba[n] = bv;
			break;
		case BvVec:
			bvp = ber_memalloc_x( sizeof( struct berval ),
				ber->ber_memctx );
			if ( !bvp ) {
				ber_memfree_x( bv.bv_val, ber->ber_memctx );
				goto failed;
			}
			res.bv[n] = bvp;
			*bvp = bv;
			break;
		case BvOff:
			*(struct berval *)(res.bo + tot_size) = bv;
			tot_size += siz;
			break;
		}
	} while (++n < i);
	return tag;

failed:
	if (b->choice != BvOff) { /* BvOff does not have LBER_BV_ALLOC set */
		while (--n >= 0) {
			switch(b->choice) {
			case ChArray:
				ber_memfree_x(res.ca[n], ber->ber_memctx);
				break;
			case BvArray:
				ber_memfree_x(res.ba[n].bv_val, ber->ber_memctx);
				break;
			case BvVec:
				ber_memfree_x(res.bv[n]->bv_val, ber->ber_memctx);
				ber_memfree_x(res.bv[n], ber->ber_memctx);
				break;
			default:
				break;
			}
		}
	}
	ber_memfree_x(b->result, ber->ber_memctx);
	b->result = NULL;
	return LBER_DEFAULT;
}

ber_tag_t
ber_get_stringbv( BerElement *ber, struct berval *bv, int option )
{
	ber_tag_t	tag;
	char		*data;

	tag = ber_skip_element( ber, bv );
	if ( tag == LBER_DEFAULT ||
		(( option & LBER_BV_STRING ) &&
		 bv->bv_len && memchr( bv->bv_val, 0, bv->bv_len - 1 )))
	{
		bv->bv_val = NULL;
		return LBER_DEFAULT;
	}

	data = bv->bv_val;
	if ( option & LBER_BV_ALLOC ) {
		bv->bv_val = (char *) ber_memalloc_x( bv->bv_len + 1,
			ber->ber_memctx );
		if ( bv->bv_val == NULL ) {
			return LBER_DEFAULT;
		}

		if ( bv->bv_len != 0 ) {
			memcpy( bv->bv_val, data, bv->bv_len );
		}
		data = bv->bv_val;
	}
	if ( !( option & LBER_BV_NOTERM ))
		data[bv->bv_len] = '\0';

	return tag;
}

ber_tag_t
ber_get_stringbv_null( BerElement *ber, struct berval *bv, int option )
{
	ber_tag_t	tag;
	char		*data;

	tag = ber_skip_element( ber, bv );
	if ( tag == LBER_DEFAULT || bv->bv_len == 0 ) {
		bv->bv_val = NULL;
		return tag;
	}

	if (( option & LBER_BV_STRING ) &&
		memchr( bv->bv_val, 0, bv->bv_len - 1 ))
	{
		bv->bv_val = NULL;
		return LBER_DEFAULT;
	}

	data = bv->bv_val;
	if ( option & LBER_BV_ALLOC ) {
		bv->bv_val = (char *) ber_memalloc_x( bv->bv_len + 1,
			ber->ber_memctx );
		if ( bv->bv_val == NULL ) {
			return LBER_DEFAULT;
		}

		memcpy( bv->bv_val, data, bv->bv_len );
		data = bv->bv_val;
	}
	if ( !( option & LBER_BV_NOTERM ))
		data[bv->bv_len] = '\0';

	return tag;
}

ber_tag_t
ber_get_stringa( BerElement *ber, char **buf )
{
	BerValue	bv;
	ber_tag_t	tag;

	assert( buf != NULL );

	tag = ber_get_stringbv( ber, &bv, LBER_BV_ALLOC | LBER_BV_STRING );
	*buf = bv.bv_val;

	return tag;
}

ber_tag_t
ber_get_stringa_null( BerElement *ber, char **buf )
{
	BerValue	bv;
	ber_tag_t	tag;

	assert( buf != NULL );

	tag = ber_get_stringbv_null( ber, &bv, LBER_BV_ALLOC | LBER_BV_STRING );
	*buf = bv.bv_val;

	return tag;
}

ber_tag_t
ber_get_stringal( BerElement *ber, struct berval **bv )
{
	ber_tag_t	tag;

	assert( ber != NULL );
	assert( bv != NULL );

	*bv = (struct berval *) ber_memalloc_x( sizeof(struct berval),
		ber->ber_memctx );
	if ( *bv == NULL ) {
		return LBER_DEFAULT;
	}

	tag = ber_get_stringbv( ber, *bv, LBER_BV_ALLOC );
	if ( tag == LBER_DEFAULT ) {
		ber_memfree_x( *bv, ber->ber_memctx );
		*bv = NULL;
	}
	return tag;
}

ber_tag_t
ber_get_bitstringa(
	BerElement *ber,
	char **buf,
	ber_len_t *blen )
{
	ber_tag_t	tag;
	struct berval	data;
	unsigned char	unusedbits;

	assert( buf != NULL );
	assert( blen != NULL );

	if ( (tag = ber_skip_element( ber, &data )) == LBER_DEFAULT ) {
		goto fail;
	}

	if ( --data.bv_len > (ber_len_t)-1 / 8 ) {
		goto fail;
	}
	unusedbits = *(unsigned char *) data.bv_val++;
	if ( unusedbits > 7 ) {
		goto fail;
	}

	if ( memchr( data.bv_val, 0, data.bv_len )) {
		goto fail;
	}

	*buf = (char *) ber_memalloc_x( data.bv_len, ber->ber_memctx );
	if ( *buf == NULL ) {
		return LBER_DEFAULT;
	}
	memcpy( *buf, data.bv_val, data.bv_len );

	*blen = data.bv_len * 8 - unusedbits;
	return tag;

 fail:
	*buf = NULL;
	return LBER_DEFAULT;
}

ber_tag_t
ber_get_null( BerElement *ber )
{
	ber_len_t	len;
	ber_tag_t	tag = ber_skip_tag( ber, &len );

	return( len == 0 ? tag : LBER_DEFAULT );
}

ber_tag_t
ber_get_boolean(
	BerElement *ber,
	ber_int_t *boolval )
{
	return ber_get_int( ber, boolval );
}

ber_tag_t
ber_first_element(
	BerElement *ber,
	ber_len_t *len,
	char **last )
{
	assert( last != NULL );

	/* skip the sequence header, use the len to mark where to stop */
	if ( ber_skip_tag( ber, len ) == LBER_DEFAULT ) {
		*last = NULL;
		return LBER_DEFAULT;
	}

	*last = ber->ber_ptr + *len;

	if ( *len == 0 ) {
		return LBER_DEFAULT;
	}

	return ber_peek_tag( ber, len );
}

ber_tag_t
ber_next_element(
	BerElement *ber,
	ber_len_t *len,
	LDAP_CONST char *last )
{
	assert( ber != NULL );
	assert( last != NULL );
	assert( LBER_VALID( ber ) );

	if ( ber->ber_ptr >= last ) {
		return LBER_DEFAULT;
	}

	return ber_peek_tag( ber, len );
}

/* VARARGS */
ber_tag_t
ber_scanf ( BerElement *ber,
	LDAP_CONST char *fmt,
	... )
{
	va_list		ap;
	LDAP_CONST char		*fmt_reset;
	char		*s, **ss, ***sss;
	struct berval	data, *bval, **bvp, ***bvpp;
	ber_int_t	*i;
	ber_len_t	*l;
	ber_tag_t	*t;
	ber_tag_t	rc;
	ber_len_t	len;

	va_start( ap, fmt );

	assert( ber != NULL );
	assert( fmt != NULL );
	assert( LBER_VALID( ber ) );

	fmt_reset = fmt;

	if ( ber->ber_debug & (LDAP_DEBUG_TRACE|LDAP_DEBUG_BER)) {
		ber_log_printf( LDAP_DEBUG_TRACE, ber->ber_debug,
			"ber_scanf fmt (%s) ber:\n", fmt );
		ber_log_dump( LDAP_DEBUG_BER, ber->ber_debug, ber, 1 );
	}

	for ( rc = 0; *fmt && rc != LBER_DEFAULT; fmt++ ) {
		/* When this is modified, remember to update
		 * the error-cleanup code below accordingly. */
		switch ( *fmt ) {
		case '!': { /* Hook */
				BERDecodeCallback *f;
				void *p;

				f = va_arg( ap, BERDecodeCallback * );
				p = va_arg( ap, void * );

				rc = (*f)( ber, p, 0 );
			} break;

		case 'a':	/* octet string - allocate storage as needed */
			ss = va_arg( ap, char ** );
			rc = ber_get_stringa( ber, ss );
			break;

		case 'A':	/* octet string - allocate storage as needed,
				 * but return NULL if len == 0 */
			ss = va_arg( ap, char ** );
			rc = ber_get_stringa_null( ber, ss );
			break;

		case 'b':	/* boolean */
			i = va_arg( ap, ber_int_t * );
			rc = ber_get_boolean( ber, i );
			break;

		case 'B':	/* bit string - allocate storage as needed */
			ss = va_arg( ap, char ** );
			l = va_arg( ap, ber_len_t * ); /* for length, in bits */
			rc = ber_get_bitstringa( ber, ss, l );
			break;

		case 'e':	/* enumerated */
		case 'i':	/* integer */
			i = va_arg( ap, ber_int_t * );
			rc = ber_get_int( ber, i );
			break;

		case 'l':	/* length of next item */
			l = va_arg( ap, ber_len_t * );
			rc = ber_peek_tag( ber, l );
			break;

		case 'm':	/* octet string in berval, in-place */
			bval = va_arg( ap, struct berval * );
			rc = ber_get_stringbv( ber, bval, 0 );
			break;

		case 'M':	/* bvoffarray - must include address of
				 * a record len, and record offset.
				 * number of records will be returned thru
				 * len ptr on finish. parsed in-place.
				 */
		{
			bgbvr cookie = { BvOff, 0 };
			bvp = va_arg( ap, struct berval ** );
			l = va_arg( ap, ber_len_t * );
			cookie.siz = *l;
			cookie.off = va_arg( ap, ber_len_t );
			rc = ber_get_stringbvl( ber, &cookie );
			*bvp = cookie.result;
			*l = cookie.siz;
			break;
		}

		case 'n':	/* null */
			rc = ber_get_null( ber );
			break;

		case 'o':	/* octet string in a supplied berval */
			bval = va_arg( ap, struct berval * );
			rc = ber_get_stringbv( ber, bval, LBER_BV_ALLOC );
			break;

		case 'O':	/* octet string - allocate & include length */
			bvp = va_arg( ap, struct berval ** );
			rc = ber_get_stringal( ber, bvp );
			break;

		case 's':	/* octet string - in a buffer */
			s = va_arg( ap, char * );
			l = va_arg( ap, ber_len_t * );
			rc = ber_get_stringb( ber, s, l );
			break;

		case 't':	/* tag of next item */
			t = va_arg( ap, ber_tag_t * );
			*t = rc = ber_peek_tag( ber, &len );
			break;

		case 'T':	/* skip tag of next item */
			t = va_arg( ap, ber_tag_t * );
			*t = rc = ber_skip_tag( ber, &len );
			break;

		case 'v':	/* sequence of strings */
		{
			bgbvr cookie = {
				ChArray, LBER_BV_ALLOC | LBER_BV_STRING, sizeof( char * )
			};
			rc = ber_get_stringbvl( ber, &cookie );
			*(va_arg( ap, char *** )) = cookie.result;
			break;
		}

		case 'V':	/* sequence of strings + lengths */
		{
			bgbvr cookie = {
				BvVec, LBER_BV_ALLOC, sizeof( struct berval * )
			};
			rc = ber_get_stringbvl( ber, &cookie );
			*(va_arg( ap, struct berval *** )) = cookie.result;
			break;
		}

		case 'W':	/* bvarray */
		{
			bgbvr cookie = {
				BvArray, LBER_BV_ALLOC, sizeof( struct berval )
			};
			rc = ber_get_stringbvl( ber, &cookie );
			*(va_arg( ap, struct berval ** )) = cookie.result;
			break;
		}

		case 'x':	/* skip the next element - whatever it is */
			rc = ber_skip_element( ber, &data );
			break;

		case '{':	/* begin sequence */
		case '[':	/* begin set */
			switch ( fmt[1] ) {
			case 'v': case 'V': case 'W': case 'M':
				break;
			default:
				rc = ber_skip_tag( ber, &len );
				break;
			}
			break;

		case '}':	/* end sequence */
		case ']':	/* end set */
			break;

		default:
			if( ber->ber_debug ) {
				ber_log_printf( LDAP_DEBUG_ANY, ber->ber_debug,
					"ber_scanf: unknown fmt %c\n", *fmt );
			}
			rc = LBER_DEFAULT;
			break;
		}
	}

	va_end( ap );

	if ( rc == LBER_DEFAULT ) {
		/*
		 * Error.  Reclaim malloced memory that was given to the caller.
		 * Set allocated pointers to NULL, "data length" outvalues to 0.
		 */
		va_start( ap, fmt );

		for ( ; fmt_reset < fmt; fmt_reset++ ) {
		switch ( *fmt_reset ) {
		case '!': { /* Hook */
				BERDecodeCallback *f;
				void *p;

				f = va_arg( ap, BERDecodeCallback * );
				p = va_arg( ap, void * );

				(void) (*f)( ber, p, 1 );
			} break;

		case 'a':	/* octet string - allocate storage as needed */
		case 'A':
			ss = va_arg( ap, char ** );
			ber_memfree_x( *ss, ber->ber_memctx );
			*ss = NULL;
			break;

		case 'b':	/* boolean */
		case 'e':	/* enumerated */
		case 'i':	/* integer */
			(void) va_arg( ap, ber_int_t * );
			break;

		case 'l':	/* length of next item */
			*(va_arg( ap, ber_len_t * )) = 0;
			break;

		case 'm':	/* berval in-place */
			bval = va_arg( ap, struct berval * );
			BER_BVZERO( bval );
			break;

		case 'M':	/* BVoff array in-place */
			bvp = va_arg( ap, struct berval ** );
			ber_memfree_x( *bvp, ber->ber_memctx );
			*bvp = NULL;
			*(va_arg( ap, ber_len_t * )) = 0;
			(void) va_arg( ap, ber_len_t );
			break;

		case 'o':	/* octet string in a supplied berval */
			bval = va_arg( ap, struct berval * );
			ber_memfree_x( bval->bv_val, ber->ber_memctx );
			BER_BVZERO( bval );
			break;

		case 'O':	/* octet string - allocate & include length */
			bvp = va_arg( ap, struct berval ** );
			ber_bvfree_x( *bvp, ber->ber_memctx );
			*bvp = NULL;
			break;

		case 's':	/* octet string - in a buffer */
			(void) va_arg( ap, char * );
			*(va_arg( ap, ber_len_t * )) = 0;
			break;

		case 't':	/* tag of next item */
		case 'T':	/* skip tag of next item */
			(void) va_arg( ap, ber_tag_t * );
			break;

		case 'B':	/* bit string - allocate storage as needed */
			ss = va_arg( ap, char ** );
			ber_memfree_x( *ss, ber->ber_memctx );
			*ss = NULL;
			*(va_arg( ap, ber_len_t * )) = 0; /* for length, in bits */
			break;

		case 'v':	/* sequence of strings */
			sss = va_arg( ap, char *** );
			ber_memvfree_x( (void **) *sss, ber->ber_memctx );
			*sss = NULL;
			break;

		case 'V':	/* sequence of strings + lengths */
			bvpp = va_arg( ap, struct berval *** );
			ber_bvecfree_x( *bvpp, ber->ber_memctx );
			*bvpp = NULL;
			break;

		case 'W':	/* BerVarray */
			bvp = va_arg( ap, struct berval ** );
			ber_bvarray_free_x( *bvp, ber->ber_memctx );
			*bvp = NULL;
			break;

		case 'n':	/* null */
		case 'x':	/* skip the next element - whatever it is */
		case '{':	/* begin sequence */
		case '[':	/* begin set */
		case '}':	/* end sequence */
		case ']':	/* end set */
			break;

		default:
			/* format should be good */
			assert( 0 );
		}
		}

		va_end( ap );
	}

	return rc;
}
