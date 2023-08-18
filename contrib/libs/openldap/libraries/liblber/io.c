/* io.c - ber general i/o routines */
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

#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/ctype.h>
#include <ac/errno.h>
#include <ac/socket.h>
#include <ac/string.h>
#include <ac/unistd.h>

#ifdef HAVE_IO_H
#include <io.h>
#endif

#include "lber-int.h"
#include "ldap_log.h"

ber_slen_t
ber_skip_data(
	BerElement *ber,
	ber_len_t len )
{
	ber_len_t	actuallen, nleft;

	assert( ber != NULL );
	assert( LBER_VALID( ber ) );

	nleft = ber_pvt_ber_remaining( ber );
	actuallen = nleft < len ? nleft : len;
	ber->ber_ptr += actuallen;
	ber->ber_tag = *(unsigned char *)ber->ber_ptr;

	return( (ber_slen_t) actuallen );
}

/*
 * Read from the ber buffer.  The caller must maintain ber->ber_tag.
 * Do not use to read whole tags.  See ber_get_tag() and ber_skip_data().
 */
ber_slen_t
ber_read(
	BerElement *ber,
	char *buf,
	ber_len_t len )
{
	ber_len_t	actuallen, nleft;

	assert( ber != NULL );
	assert( buf != NULL );
	assert( LBER_VALID( ber ) );

	nleft = ber_pvt_ber_remaining( ber );
	actuallen = nleft < len ? nleft : len;

	AC_MEMCPY( buf, ber->ber_ptr, actuallen );

	ber->ber_ptr += actuallen;

	return( (ber_slen_t) actuallen );
}

/*
 * Write to the ber buffer.
 * Note that ber_start_seqorset/ber_put_seqorset() bypass ber_write().
 */
ber_slen_t
ber_write(
	BerElement *ber,
	LDAP_CONST char *buf,
	ber_len_t len,
	int zero )	/* nonzero is unsupported from OpenLDAP 2.4.18 */
{
	char **p;

	assert( ber != NULL );
	assert( buf != NULL );
	assert( LBER_VALID( ber ) );

	if ( zero != 0 ) {
		ber_log_printf( LDAP_DEBUG_ANY, ber->ber_debug, "%s",
			"ber_write: nonzero 4th argument not supported\n" );
		return( -1 );
	}

	p = ber->ber_sos_ptr == NULL ? &ber->ber_ptr : &ber->ber_sos_ptr;
	if ( len > (ber_len_t) (ber->ber_end - *p) ) {
		if ( ber_realloc( ber, len ) != 0 ) return( -1 );
	}
	AC_MEMCPY( *p, buf, len );
	*p += len;

	return( (ber_slen_t) len );
}

/* Resize the ber buffer */
int
ber_realloc( BerElement *ber, ber_len_t len )
{
	ber_len_t	total, offset, sos_offset, rw_offset;
	char		*buf;

	assert( ber != NULL );
	assert( LBER_VALID( ber ) );

	/* leave room for ber_flatten() to \0-terminate ber_buf */
	if ( ++len == 0 ) {
		return( -1 );
	}

	total = ber_pvt_ber_total( ber );

#define LBER_EXBUFSIZ	4060 /* a few words less than 2^N for binary buddy */
#if defined( LBER_EXBUFSIZ ) && LBER_EXBUFSIZ > 0
# ifndef notdef
	/* don't realloc by small amounts */
	total += len < LBER_EXBUFSIZ ? LBER_EXBUFSIZ : len;
# else
	{	/* not sure what value this adds.  reduce fragmentation? */
		ber_len_t have = (total + (LBER_EXBUFSIZE - 1)) / LBER_EXBUFSIZ;
		ber_len_t need = (len + (LBER_EXBUFSIZ - 1)) / LBER_EXBUFSIZ;
		total = ( have + need ) * LBER_EXBUFSIZ;
	}
# endif
#else
	total += len;	/* realloc just what's needed */
#endif

	if ( total < len || total > (ber_len_t)-1 / 2 /* max ber_slen_t */ ) {
		return( -1 );
	}

	buf = ber->ber_buf;
	offset = ber->ber_ptr - buf;
	sos_offset = ber->ber_sos_ptr ? ber->ber_sos_ptr - buf : 0;
	/* if ber_sos_ptr != NULL, it is > ber_buf so that sos_offset > 0 */
	rw_offset = ber->ber_rwptr ? ber->ber_rwptr - buf : 0;

	buf = (char *) ber_memrealloc_x( buf, total, ber->ber_memctx );
	if ( buf == NULL ) {
		return( -1 );
	}

	ber->ber_buf = buf;
	ber->ber_end = buf + total;
	ber->ber_ptr = buf + offset;
	if ( sos_offset )
		ber->ber_sos_ptr = buf + sos_offset;
	if ( ber->ber_rwptr )
		ber->ber_rwptr = buf + rw_offset;

	return( 0 );
}

void
ber_free_buf( BerElement *ber )
{
	assert( LBER_VALID( ber ) );

	if ( ber->ber_buf) ber_memfree_x( ber->ber_buf, ber->ber_memctx );

	ber->ber_buf = NULL;
	ber->ber_sos_ptr = NULL;
	ber->ber_valid = LBER_UNINITIALIZED;
}

void
ber_free( BerElement *ber, int freebuf )
{
	if( ber == NULL ) {
		LDAP_MEMORY_DEBUG_ASSERT( ber != NULL );
		return;
	}

	if( freebuf ) ber_free_buf( ber );

	ber_memfree_x( (char *) ber, ber->ber_memctx );
}

int
ber_flush( Sockbuf *sb, BerElement *ber, int freeit )
{
	return ber_flush2( sb, ber,
		freeit ? LBER_FLUSH_FREE_ON_SUCCESS
			: LBER_FLUSH_FREE_NEVER );
}

int
ber_flush2( Sockbuf *sb, BerElement *ber, int freeit )
{
	ber_len_t	towrite;
	ber_slen_t	rc;

	assert( sb != NULL );
	assert( ber != NULL );
	assert( SOCKBUF_VALID( sb ) );
	assert( LBER_VALID( ber ) );

	if ( ber->ber_rwptr == NULL ) {
		ber->ber_rwptr = ber->ber_buf;
	}
	towrite = ber->ber_ptr - ber->ber_rwptr;

	if ( sb->sb_debug ) {
		ber_log_printf( LDAP_DEBUG_TRACE, sb->sb_debug,
			"ber_flush2: %ld bytes to sd %ld%s\n",
			towrite, (long) sb->sb_fd,
			ber->ber_rwptr != ber->ber_buf ?  " (re-flush)" : "" );
		ber_log_bprint( LDAP_DEBUG_BER, sb->sb_debug,
			ber->ber_rwptr, towrite );
	}

	while ( towrite > 0 ) {
#ifdef LBER_TRICKLE
		sleep(1);
		rc = ber_int_sb_write( sb, ber->ber_rwptr, 1 );
#else
		rc = ber_int_sb_write( sb, ber->ber_rwptr, towrite );
#endif
		if ( rc <= 0 ) {
			if ( freeit & LBER_FLUSH_FREE_ON_ERROR ) ber_free( ber, 1 );
			return -1;
		}
		towrite -= rc;
		ber->ber_rwptr += rc;
	} 

	if ( freeit & LBER_FLUSH_FREE_ON_SUCCESS ) ber_free( ber, 1 );

	return 0;
}

BerElement *
ber_alloc_t( int options )
{
	BerElement	*ber;

	ber = (BerElement *) LBER_CALLOC( 1, sizeof(BerElement) );

	if ( ber == NULL ) {
		return NULL;
	}

	ber->ber_valid = LBER_VALID_BERELEMENT;
	ber->ber_tag = LBER_DEFAULT;
	ber->ber_options = options;
	ber->ber_debug = ber_int_debug;

	assert( LBER_VALID( ber ) );
	return ber;
}

BerElement *
ber_alloc( void )	/* deprecated */
{
	return ber_alloc_t( 0 );
}

BerElement *
der_alloc( void )	/* deprecated */
{
	return ber_alloc_t( LBER_USE_DER );
}

BerElement *
ber_dup( BerElement *ber )
{
	BerElement	*new;

	assert( ber != NULL );
	assert( LBER_VALID( ber ) );

	if ( (new = ber_alloc_t( ber->ber_options )) == NULL ) {
		return NULL;
	}

	*new = *ber;

	assert( LBER_VALID( new ) );
	return( new );
}


void
ber_init2( BerElement *ber, struct berval *bv, int options )
{
	assert( ber != NULL );

	(void) memset( (char *)ber, '\0', sizeof( BerElement ));
	ber->ber_valid = LBER_VALID_BERELEMENT;
	ber->ber_tag = LBER_DEFAULT;
	ber->ber_options = (char) options;
	ber->ber_debug = ber_int_debug;

	if ( bv != NULL ) {
		ber->ber_buf = bv->bv_val;
		ber->ber_ptr = ber->ber_buf;
		ber->ber_end = ber->ber_buf + bv->bv_len;
	}

	assert( LBER_VALID( ber ) );
}

/* OLD U-Mich ber_init() */
void
ber_init_w_nullc( BerElement *ber, int options )
{
	ber_init2( ber, NULL, options );
}

/* New C-API ber_init() */
/* This function constructs a BerElement containing a copy
** of the data in the bv argument.
*/
BerElement *
ber_init( struct berval *bv )
{
	BerElement *ber;

	assert( bv != NULL );

	if ( bv == NULL ) {
		return NULL;
	}

	ber = ber_alloc_t( 0 );

	if( ber == NULL ) {
		/* allocation failed */
		return NULL;
	}

	/* copy the data */
	if ( ((ber_len_t) ber_write ( ber, bv->bv_val, bv->bv_len, 0 ))
		!= bv->bv_len )
	{
		/* write failed, so free and return NULL */
		ber_free( ber, 1 );
		return NULL;
	}

	ber_reset( ber, 1 );	/* reset the pointer to the start of the buffer */
	return ber;
}

/* New C-API ber_flatten routine */
/* This routine allocates a struct berval whose contents are a BER
** encoding taken from the ber argument.  The bvPtr pointer points to
** the returned berval.
**
** ber_flatten2 is the same, but uses a struct berval passed by
** the caller. If alloc is 0 the returned bv uses the ber buf directly.
*/
int ber_flatten2(
	BerElement *ber,
	struct berval *bv,
	int alloc )
{
	assert( bv != NULL );

	if ( bv == NULL ) {
		return -1;
	}

	if ( ber == NULL ) {
		/* ber is null, create an empty berval */
		bv->bv_val = NULL;
		bv->bv_len = 0;

	} else if ( ber->ber_sos_ptr != NULL ) {
		/* unmatched "{" and "}" */
		return -1;

	} else {
		/* copy the berval */
		ber_len_t len = ber_pvt_ber_write( ber );

		if ( alloc ) {
			bv->bv_val = (char *) ber_memalloc_x( len + 1, ber->ber_memctx );
			if ( bv->bv_val == NULL ) {
				return -1;
			}
			AC_MEMCPY( bv->bv_val, ber->ber_buf, len );
			bv->bv_val[len] = '\0';
		} else if ( ber->ber_buf != NULL ) {
			bv->bv_val = ber->ber_buf;
			bv->bv_val[len] = '\0';
		} else {
			bv->bv_val = "";
		}
		bv->bv_len = len;
	}
	return 0;
}

int ber_flatten(
	BerElement *ber,
	struct berval **bvPtr)
{
	struct berval *bv;
	int rc;
 
	assert( bvPtr != NULL );

	if(bvPtr == NULL) {
		return -1;
	}

	bv = ber_memalloc_x( sizeof(struct berval), ber->ber_memctx );
	if ( bv == NULL ) {
		return -1;
	}
	rc = ber_flatten2(ber, bv, 1);
	if (rc == -1) {
		ber_memfree_x(bv, ber->ber_memctx);
	} else {
		*bvPtr = bv;
	}
	return rc;
}

void
ber_reset( BerElement *ber, int was_writing )
{
	assert( ber != NULL );
	assert( LBER_VALID( ber ) );

	if ( was_writing ) {
		ber->ber_end = ber->ber_ptr;
		ber->ber_ptr = ber->ber_buf;

	} else {
		ber->ber_ptr = ber->ber_end;
	}

	ber->ber_rwptr = NULL;
}

/*
 * A rewrite of ber_get_next that can safely be called multiple times 
 * for the same packet. It will simply continue where it stopped until
 * a full packet is read.
 */

#define LENSIZE	4

ber_tag_t
ber_get_next(
	Sockbuf *sb,
	ber_len_t *len,
	BerElement *ber )
{
	assert( sb != NULL );
	assert( len != NULL );
	assert( ber != NULL );
	assert( SOCKBUF_VALID( sb ) );
	assert( LBER_VALID( ber ) );

	if ( ber->ber_debug & LDAP_DEBUG_TRACE ) {
		ber_log_printf( LDAP_DEBUG_TRACE, ber->ber_debug,
			"ber_get_next\n" );
	}

	/*
	 * Any ber element looks like this: tag length contents.
	 * Assuming everything's ok, we return the tag byte (we
	 * can assume a single byte), return the length in len,
	 * and the rest of the undecoded element in buf.
	 *
	 * Assumptions:
	 *	1) small tags (less than 128)
	 *	2) definite lengths
	 *	3) primitive encodings used whenever possible
	 *
	 * The code also handles multi-byte tags. The first few bytes
	 * of the message are read to check for multi-byte tags and
	 * lengths. These bytes are temporarily stored in the ber_tag,
	 * ber_len, and ber_usertag fields of the berelement until
	 * tag/len parsing is complete. After this parsing, any leftover
	 * bytes and the rest of the message are copied into the ber_buf.
	 *
	 * We expect tag and len to be at most 32 bits wide.
	 */

	if (ber->ber_rwptr == NULL) {
		assert( ber->ber_buf == NULL );
		ber->ber_rwptr = (char *) &ber->ber_len-1;
		ber->ber_ptr = ber->ber_rwptr;
		ber->ber_tag = 0;
	}

	while (ber->ber_rwptr > (char *)&ber->ber_tag && ber->ber_rwptr <
		(char *)&ber->ber_len + LENSIZE*2) {
		ber_slen_t sblen;
		char buf[sizeof(ber->ber_len)-1];
		ber_len_t tlen = 0;

		/* The tag & len can be at most 9 bytes; we try to read up to 8 here */
		sock_errset(0);
		sblen=((char *)&ber->ber_len + LENSIZE*2 - 1)-ber->ber_rwptr;
		/* Trying to read the last len byte of a 9 byte tag+len */
		if (sblen<1)
			sblen = 1;
		sblen=ber_int_sb_read( sb, ber->ber_rwptr, sblen );
		if (sblen<=0) return LBER_DEFAULT;
		ber->ber_rwptr += sblen;

		/* We got at least one byte, try to parse the tag. */
		if (ber->ber_ptr == (char *)&ber->ber_len-1) {
			ber_tag_t tag;
			unsigned char *p = (unsigned char *)ber->ber_ptr;
			tag = *p++;
			if ((tag & LBER_BIG_TAG_MASK) == LBER_BIG_TAG_MASK) {
				ber_len_t i;
				for (i=1; (char *)p<ber->ber_rwptr; i++) {
					tag <<= 8;
					tag |= *p++;
					if (!(tag & LBER_MORE_TAG_MASK))
						break;
					/* Is the tag too big? */
					if (i == sizeof(ber_tag_t)-1) {
						sock_errset(ERANGE);
						return LBER_DEFAULT;
					}
				}
				/* Did we run out of bytes? */
				if ((char *)p == ber->ber_rwptr) {
					sock_errset(EWOULDBLOCK);
					return LBER_DEFAULT;
				}
			}
			ber->ber_tag = tag;
			ber->ber_ptr = (char *)p;
		}

		if ( ber->ber_ptr == ber->ber_rwptr ) {
			sock_errset(EWOULDBLOCK);
			return LBER_DEFAULT;
		}

		/* Now look for the length */
		if (*ber->ber_ptr & 0x80) {	/* multi-byte */
			int i;
			unsigned char *p = (unsigned char *)ber->ber_ptr;
			int llen = *p++ & 0x7f;
			if (llen > LENSIZE) {
				sock_errset(ERANGE);
				return LBER_DEFAULT;
			}
			/* Not enough bytes? */
			if (ber->ber_rwptr - (char *)p < llen) {
				sock_errset(EWOULDBLOCK);
				return LBER_DEFAULT;
			}
			for (i=0; i<llen; i++) {
				tlen <<=8;
				tlen |= *p++;
			}
			ber->ber_ptr = (char *)p;
		} else {
			tlen = *(unsigned char *)ber->ber_ptr++;
		}

		/* Are there leftover data bytes inside ber->ber_len? */
		if (ber->ber_ptr < (char *)&ber->ber_usertag) {
			if (ber->ber_rwptr < (char *)&ber->ber_usertag) {
				sblen = ber->ber_rwptr - ber->ber_ptr;
			} else {
				sblen = (char *)&ber->ber_usertag - ber->ber_ptr;
			}
			AC_MEMCPY(buf, ber->ber_ptr, sblen);
			ber->ber_ptr += sblen;
		} else {
			sblen = 0;
		}
		ber->ber_len = tlen;

		/* now fill the buffer. */

		/* make sure length is reasonable */
		if ( ber->ber_len == 0 ) {
			sock_errset(ERANGE);
			return LBER_DEFAULT;
		}

		if ( sb->sb_max_incoming && ber->ber_len > sb->sb_max_incoming ) {
			ber_log_printf( LDAP_DEBUG_CONNS, ber->ber_debug,
				"ber_get_next: sockbuf_max_incoming exceeded "
				"(%ld > %ld)\n", ber->ber_len, sb->sb_max_incoming );
			sock_errset(ERANGE);
			return LBER_DEFAULT;
		}

		if (ber->ber_buf==NULL) {
			ber_len_t l = ber->ber_rwptr - ber->ber_ptr;
			/* ber->ber_ptr is always <= ber->ber->ber_rwptr.
			 * make sure ber->ber_len agrees with what we've
			 * already read.
			 */
			if ( ber->ber_len < sblen + l ) {
				sock_errset(ERANGE);
				return LBER_DEFAULT;
			}
			ber->ber_buf = (char *) ber_memalloc_x( ber->ber_len + 1, ber->ber_memctx );
			if (ber->ber_buf==NULL) {
				return LBER_DEFAULT;
			}
			ber->ber_end = ber->ber_buf + ber->ber_len;
			if (sblen) {
				AC_MEMCPY(ber->ber_buf, buf, sblen);
			}
			if (l > 0) {
				AC_MEMCPY(ber->ber_buf + sblen, ber->ber_ptr, l);
				sblen += l;
			}
			*ber->ber_end = '\0';
			ber->ber_ptr = ber->ber_buf;
			ber->ber_usertag = 0;
			if ((ber_len_t)sblen == ber->ber_len) {
				goto done;
			}
			ber->ber_rwptr = ber->ber_buf + sblen;
		}
	}

	if ((ber->ber_rwptr>=ber->ber_buf) && (ber->ber_rwptr<ber->ber_end)) {
		ber_slen_t res;
		ber_slen_t to_go;
		
		to_go = ber->ber_end - ber->ber_rwptr;
		/* unsigned/signed overflow */
		if (to_go<0) return LBER_DEFAULT;
		
		sock_errset(0);
		res = ber_int_sb_read( sb, ber->ber_rwptr, to_go );
		if (res<=0) return LBER_DEFAULT;
		ber->ber_rwptr+=res;
		
		if (res<to_go) {
			sock_errset(EWOULDBLOCK);
			return LBER_DEFAULT;
		}
done:
		ber->ber_rwptr = NULL;
		*len = ber->ber_len;
		if ( ber->ber_debug ) {
			ber_log_printf( LDAP_DEBUG_TRACE, ber->ber_debug,
				"ber_get_next: tag 0x%lx len %ld contents:\n",
				ber->ber_tag, ber->ber_len );
			ber_log_dump( LDAP_DEBUG_BER, ber->ber_debug, ber, 1 );
		}
		return (ber->ber_tag);
	}

	/* invalid input */
	return LBER_DEFAULT;
}

char *
ber_start( BerElement* ber )
{
	return ber->ber_buf;
}

int
ber_len( BerElement* ber )
{
	return ( ber->ber_end - ber->ber_buf );
}

int
ber_ptrlen( BerElement* ber )
{
	return ( ber->ber_ptr - ber->ber_buf );
}

void
ber_rewind ( BerElement * ber )
{
	ber->ber_rwptr = NULL;
	ber->ber_sos_ptr = NULL;
	ber->ber_end = ber->ber_ptr;
	ber->ber_ptr = ber->ber_buf;
#if 0	/* TODO: Should we add this? */
	ber->ber_tag = LBER_DEFAULT;
	ber->ber_usertag = 0;
#endif
}

int
ber_remaining( BerElement * ber )
{
	return ber_pvt_ber_remaining( ber );
}
