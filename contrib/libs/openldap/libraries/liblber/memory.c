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

#include "portable.h"

#include <ac/stdlib.h>
#include <ac/string.h>

#include "lber-int.h"

#ifdef LDAP_MEMORY_TRACE
#include <stdio.h>
#endif

#ifdef LDAP_MEMORY_DEBUG
/*
 * LDAP_MEMORY_DEBUG should only be enabled for the purposes of
 * debugging memory management within OpenLDAP libraries and slapd.
 *
 * It should only be enabled by an experienced developer as it causes
 * the inclusion of numerous assert()'s, many of which may be triggered
 * by a perfectly valid program.  If LDAP_MEMORY_DEBUG & 2 is true,
 * that includes asserts known to break both slapd and current clients.
 *
 * The code behind this macro is subject to change as needed to
 * support this testing.
 */

struct ber_mem_hdr {
	ber_int_t	bm_top;	/* Pattern to detect buf overrun from prev buffer */
	ber_int_t	bm_length; /* Length of user allocated area */
#ifdef LDAP_MEMORY_TRACE
	ber_int_t	bm_sequence; /* Allocation sequence number */
#endif
	union bmu_align_u {	/* Force alignment, pattern to detect back clobber */
		ber_len_t	bmu_len_t;
		ber_tag_t	bmu_tag_t;
		ber_int_t	bmu_int_t;

		size_t	bmu_size_t;
		void *	bmu_voidp;
		double	bmu_double;
		long	bmu_long;
		long	(*bmu_funcp)( double );
		unsigned char	bmu_char[4];
	} ber_align;
#define bm_junk	ber_align.bmu_len_t
#define bm_data	ber_align.bmu_char[1]
#define bm_char	ber_align.bmu_char
};

/* Pattern at top of allocated space */
#define LBER_MEM_JUNK ((ber_int_t) 0xdeaddada)

static const struct ber_mem_hdr ber_int_mem_hdr = { LBER_MEM_JUNK };

/* Note sequence and ber_int_meminuse are counters, but are not
 * thread safe.  If you want to use these values for multithreaded applications,
 * you must put mutexes around them, otherwise they will have incorrect values.
 * When debugging, if you sort the debug output, the sequence number will 
 * put allocations/frees together.  It is then a simple matter to write a script
 * to find any allocations that don't have a buffer free function.
 */
long ber_int_meminuse = 0;
#ifdef LDAP_MEMORY_TRACE
static ber_int_t sequence = 0;
#endif

/* Pattern placed just before user data */
static unsigned char toppattern[4] = { 0xde, 0xad, 0xba, 0xde };
/* Pattern placed just after user data */
static unsigned char endpattern[4] = { 0xd1, 0xed, 0xde, 0xca };

#define mbu_len sizeof(ber_int_mem_hdr.ber_align)

/* Test if pattern placed just before user data is good */
#define testdatatop(val) ( \
	*(val->bm_char+mbu_len-4)==toppattern[0] && \
	*(val->bm_char+mbu_len-3)==toppattern[1] && \
	*(val->bm_char+mbu_len-2)==toppattern[2] && \
	*(val->bm_char+mbu_len-1)==toppattern[3] )

/* Place pattern just before user data */
#define setdatatop(val)	*(val->bm_char+mbu_len-4)=toppattern[0]; \
	*(val->bm_char+mbu_len-3)=toppattern[1]; \
	*(val->bm_char+mbu_len-2)=toppattern[2]; \
	*(val->bm_char+mbu_len-1)=toppattern[3];

/* Test if pattern placed just after user data is good */
#define testend(val) ( 	*((unsigned char *)val+0)==endpattern[0] && \
	*((unsigned char *)val+1)==endpattern[1] && \
	*((unsigned char *)val+2)==endpattern[2] && \
	*((unsigned char *)val+3)==endpattern[3] )

/* Place pattern just after user data */
#define setend(val)  	*((unsigned char *)val+0)=endpattern[0]; \
	*((unsigned char *)val+1)=endpattern[1]; \
	*((unsigned char *)val+2)=endpattern[2]; \
	*((unsigned char *)val+3)=endpattern[3];

#define BER_MEM_BADADDR	((void *) &ber_int_mem_hdr.bm_data)
#define BER_MEM_VALID(p)	do { \
		assert( (p) != BER_MEM_BADADDR );	\
		assert( (p) != (void *) &ber_int_mem_hdr );	\
	} while(0)

#else
#define BER_MEM_VALID(p)	/* no-op */
#endif

BerMemoryFunctions *ber_int_memory_fns = NULL;

void
ber_memfree_x( void *p, void *ctx )
{
	if( p == NULL ) {
		return;
	}

	BER_MEM_VALID( p );

	if( ber_int_memory_fns == NULL || ctx == NULL ) {
#ifdef LDAP_MEMORY_DEBUG
		struct ber_mem_hdr *mh = (struct ber_mem_hdr *)
			((char *)p - sizeof(struct ber_mem_hdr));
		assert( mh->bm_top == LBER_MEM_JUNK);
		assert( testdatatop( mh));
		assert( testend( (char *)&mh[1] + mh->bm_length) );
		ber_int_meminuse -= mh->bm_length;

#ifdef LDAP_MEMORY_TRACE
		fprintf(stderr, "0x%08lx 0x%08lx -f- %ld ber_memfree %ld\n",
			(long)mh->bm_sequence, (long)mh, (long)mh->bm_length,
			ber_int_meminuse);
#endif
		/* Fill the free space with poison */
		memset( mh, 0xff, mh->bm_length + sizeof(struct ber_mem_hdr) + sizeof(ber_int_t));
		free( mh );
#else
		free( p );
#endif
		return;
	}

	assert( ber_int_memory_fns->bmf_free != 0 );

	(*ber_int_memory_fns->bmf_free)( p, ctx );
}

void
ber_memfree( void *p )
{
	ber_memfree_x(p, NULL);
}

void
ber_memvfree_x( void **vec, void *ctx )
{
	int	i;

	if( vec == NULL ) {
		return;
	}

	BER_MEM_VALID( vec );

	for ( i = 0; vec[i] != NULL; i++ ) {
		ber_memfree_x( vec[i], ctx );
	}

	ber_memfree_x( vec, ctx );
}

void
ber_memvfree( void **vec )
{
	ber_memvfree_x( vec, NULL );
}

void *
ber_memalloc_x( ber_len_t s, void *ctx )
{
	void *new;

	if( s == 0 ) {
		LDAP_MEMORY_DEBUG_ASSERT( s != 0 );
		return NULL;
	}

	if( ber_int_memory_fns == NULL || ctx == NULL ) {
#ifdef LDAP_MEMORY_DEBUG
		new = malloc(s + sizeof(struct ber_mem_hdr) + sizeof( ber_int_t));
		if( new )
		{
		struct ber_mem_hdr *mh = new;
		mh->bm_top = LBER_MEM_JUNK;
		mh->bm_length = s;
		setdatatop( mh);
		setend( (char *)&mh[1] + mh->bm_length );

		ber_int_meminuse += mh->bm_length;	/* Count mem inuse */

#ifdef LDAP_MEMORY_TRACE
		mh->bm_sequence = sequence++;
		fprintf(stderr, "0x%08lx 0x%08lx -a- %ld ber_memalloc %ld\n",
			(long)mh->bm_sequence, (long)mh, (long)mh->bm_length,
			ber_int_meminuse);
#endif
		/* poison new memory */
		memset( (char *)&mh[1], 0xff, s);

		BER_MEM_VALID( &mh[1] );
		new = &mh[1];
		}
#else
		new = malloc( s );
#endif
	} else {
		new = (*ber_int_memory_fns->bmf_malloc)( s, ctx );
	}

	if( new == NULL ) {
		ber_errno = LBER_ERROR_MEMORY;
	}

	return new;
}

void *
ber_memalloc( ber_len_t s )
{
	return ber_memalloc_x( s, NULL );
}

void *
ber_memcalloc_x( ber_len_t n, ber_len_t s, void *ctx )
{
	void *new;

	if( n == 0 || s == 0 ) {
		LDAP_MEMORY_DEBUG_ASSERT( n != 0 && s != 0);
		return NULL;
	}

	if( ber_int_memory_fns == NULL || ctx == NULL ) {
#ifdef LDAP_MEMORY_DEBUG
		new = n < (-sizeof(struct ber_mem_hdr) - sizeof(ber_int_t)) / s
			? calloc(1, n*s + sizeof(struct ber_mem_hdr) + sizeof(ber_int_t))
			: NULL;
		if( new )
		{
		struct ber_mem_hdr *mh = new;

		mh->bm_top = LBER_MEM_JUNK;
		mh->bm_length = n*s;
		setdatatop( mh);
		setend( (char *)&mh[1] + mh->bm_length );

		ber_int_meminuse += mh->bm_length;

#ifdef LDAP_MEMORY_TRACE
		mh->bm_sequence = sequence++;
		fprintf(stderr, "0x%08lx 0x%08lx -a- %ld ber_memcalloc %ld\n",
			(long)mh->bm_sequence, (long)mh, (long)mh->bm_length,
			ber_int_meminuse);
#endif
		BER_MEM_VALID( &mh[1] );
		new = &mh[1];
		}
#else
		new = calloc( n, s );
#endif

	} else {
		new = (*ber_int_memory_fns->bmf_calloc)( n, s, ctx );
	}

	if( new == NULL ) {
		ber_errno = LBER_ERROR_MEMORY;
	}

	return new;
}

void *
ber_memcalloc( ber_len_t n, ber_len_t s )
{
	return ber_memcalloc_x( n, s, NULL );
}

void *
ber_memrealloc_x( void* p, ber_len_t s, void *ctx )
{
	void *new = NULL;

	/* realloc(NULL,s) -> malloc(s) */
	if( p == NULL ) {
		return ber_memalloc_x( s, ctx );
	}
	
	/* realloc(p,0) -> free(p) */
	if( s == 0 ) {
		ber_memfree_x( p, ctx );
		return NULL;
	}

	BER_MEM_VALID( p );

	if( ber_int_memory_fns == NULL || ctx == NULL ) {
#ifdef LDAP_MEMORY_DEBUG
		ber_int_t oldlen;
		struct ber_mem_hdr *mh = (struct ber_mem_hdr *)
			((char *)p - sizeof(struct ber_mem_hdr));
		assert( mh->bm_top == LBER_MEM_JUNK);
		assert( testdatatop( mh));
		assert( testend( (char *)&mh[1] + mh->bm_length) );
		oldlen = mh->bm_length;

		p = realloc( mh, s + sizeof(struct ber_mem_hdr) + sizeof(ber_int_t) );
		if( p == NULL ) {
			ber_errno = LBER_ERROR_MEMORY;
			return NULL;
		}

			mh = p;
		mh->bm_length = s;
		setend( (char *)&mh[1] + mh->bm_length );
		if( s > oldlen ) {
			/* poison any new memory */
			memset( (char *)&mh[1] + oldlen, 0xff, s - oldlen);
		}

		assert( mh->bm_top == LBER_MEM_JUNK);
		assert( testdatatop( mh));

		ber_int_meminuse += s - oldlen;
#ifdef LDAP_MEMORY_TRACE
		fprintf(stderr, "0x%08lx 0x%08lx -a- %ld ber_memrealloc %ld\n",
			(long)mh->bm_sequence, (long)mh, (long)mh->bm_length,
			ber_int_meminuse);
#endif
			BER_MEM_VALID( &mh[1] );
		return &mh[1];
#else
		new = realloc( p, s );
#endif
	} else {
		new = (*ber_int_memory_fns->bmf_realloc)( p, s, ctx );
	}

	if( new == NULL ) {
		ber_errno = LBER_ERROR_MEMORY;
	}

	return new;
}

void *
ber_memrealloc( void* p, ber_len_t s )
{
	return ber_memrealloc_x( p, s, NULL );
}

void
ber_bvfree_x( struct berval *bv, void *ctx )
{
	if( bv == NULL ) {
		return;
	}

	BER_MEM_VALID( bv );

	if ( bv->bv_val != NULL ) {
		ber_memfree_x( bv->bv_val, ctx );
	}

	ber_memfree_x( (char *) bv, ctx );
}

void
ber_bvfree( struct berval *bv )
{
	ber_bvfree_x( bv, NULL );
}

void
ber_bvecfree_x( struct berval **bv, void *ctx )
{
	int	i;

	if( bv == NULL ) {
		return;
	}

	BER_MEM_VALID( bv );

	/* count elements */
	for ( i = 0; bv[i] != NULL; i++ ) ;

	/* free in reverse order */
	for ( i--; i >= 0; i-- ) {
		ber_bvfree_x( bv[i], ctx );
	}

	ber_memfree_x( (char *) bv, ctx );
}

void
ber_bvecfree( struct berval **bv )
{
	ber_bvecfree_x( bv, NULL );
}

int
ber_bvecadd_x( struct berval ***bvec, struct berval *bv, void *ctx )
{
	ber_len_t i;
	struct berval **new;

	if( *bvec == NULL ) {
		if( bv == NULL ) {
			/* nothing to add */
			return 0;
		}

		*bvec = ber_memalloc_x( 2 * sizeof(struct berval *), ctx );

		if( *bvec == NULL ) {
			return -1;
		}

		(*bvec)[0] = bv;
		(*bvec)[1] = NULL;

		return 1;
	}

	BER_MEM_VALID( bvec );

	/* count entries */
	for ( i = 0; (*bvec)[i] != NULL; i++ ) {
		/* EMPTY */;
	}

	if( bv == NULL ) {
		return i;
	}

	new = ber_memrealloc_x( *bvec, (i+2) * sizeof(struct berval *), ctx);

	if( new == NULL ) {
		return -1;
	}

	*bvec = new;

	(*bvec)[i++] = bv;
	(*bvec)[i] = NULL;

	return i;
}

int
ber_bvecadd( struct berval ***bvec, struct berval *bv )
{
	return ber_bvecadd_x( bvec, bv, NULL );
}

struct berval *
ber_dupbv_x(
	struct berval *dst, struct berval *src, void *ctx )
{
	struct berval *new, tmp;

	if( src == NULL ) {
		ber_errno = LBER_ERROR_PARAM;
		return NULL;
	}

	if ( dst ) {
		new = &tmp;
	} else {
		if(( new = ber_memalloc_x( sizeof(struct berval), ctx )) == NULL ) {
			return NULL;
		}
	}

	if ( src->bv_val == NULL ) {
		new->bv_val = NULL;
		new->bv_len = 0;
	} else {

		if(( new->bv_val = ber_memalloc_x( src->bv_len + 1, ctx )) == NULL ) {
			if ( !dst )
				ber_memfree_x( new, ctx );
			return NULL;
		}

		AC_MEMCPY( new->bv_val, src->bv_val, src->bv_len );
		new->bv_val[src->bv_len] = '\0';
		new->bv_len = src->bv_len;
	}

	if ( dst ) {
		*dst = *new;
		new = dst;
	}

	return new;
}

struct berval *
ber_dupbv(
	struct berval *dst, struct berval *src )
{
	return ber_dupbv_x( dst, src, NULL );
}

struct berval *
ber_bvdup(
	struct berval *src )
{
	return ber_dupbv_x( NULL, src, NULL );
}

struct berval *
ber_str2bv_x(
	LDAP_CONST char *s, ber_len_t len, int dup, struct berval *bv,
	void *ctx)
{
	struct berval *new;

	if( s == NULL ) {
		ber_errno = LBER_ERROR_PARAM;
		return NULL;
	}

	if( bv ) {
		new = bv;
	} else {
		if(( new = ber_memalloc_x( sizeof(struct berval), ctx )) == NULL ) {
			return NULL;
		}
	}

	new->bv_len = len ? len : strlen( s );
	if ( dup ) {
		if ( (new->bv_val = ber_memalloc_x( new->bv_len+1, ctx )) == NULL ) {
			if ( !bv )
				ber_memfree_x( new, ctx );
			return NULL;
		}

		AC_MEMCPY( new->bv_val, s, new->bv_len );
		new->bv_val[new->bv_len] = '\0';
	} else {
		new->bv_val = (char *) s;
	}

	return( new );
}

struct berval *
ber_str2bv(
	LDAP_CONST char *s, ber_len_t len, int dup, struct berval *bv)
{
	return ber_str2bv_x( s, len, dup, bv, NULL );
}

struct berval *
ber_mem2bv_x(
	LDAP_CONST char *s, ber_len_t len, int dup, struct berval *bv,
	void *ctx)
{
	struct berval *new;

	if( s == NULL ) {
		ber_errno = LBER_ERROR_PARAM;
		return NULL;
	}

	if( bv ) {
		new = bv;
	} else {
		if(( new = ber_memalloc_x( sizeof(struct berval), ctx )) == NULL ) {
			return NULL;
		}
	}

	new->bv_len = len;
	if ( dup ) {
		if ( (new->bv_val = ber_memalloc_x( new->bv_len+1, ctx )) == NULL ) {
			if ( !bv ) {
				ber_memfree_x( new, ctx );
			}
			return NULL;
		}

		AC_MEMCPY( new->bv_val, s, new->bv_len );
		new->bv_val[new->bv_len] = '\0';
	} else {
		new->bv_val = (char *) s;
	}

	return( new );
}

struct berval *
ber_mem2bv(
	LDAP_CONST char *s, ber_len_t len, int dup, struct berval *bv)
{
	return ber_mem2bv_x( s, len, dup, bv, NULL );
}

char *
ber_strdup_x( LDAP_CONST char *s, void *ctx )
{
	char    *p;
	size_t	len;
	
#ifdef LDAP_MEMORY_DEBUG
	assert(s != NULL);			/* bv damn better point to something */
#endif

	if( s == NULL ) {
		ber_errno = LBER_ERROR_PARAM;
		return NULL;
	}

	len = strlen( s ) + 1;
	if ( (p = ber_memalloc_x( len, ctx )) != NULL ) {
		AC_MEMCPY( p, s, len );
	}

	return p;
}

char *
ber_strdup( LDAP_CONST char *s )
{
	return ber_strdup_x( s, NULL );
}

ber_len_t
ber_strnlen( LDAP_CONST char *s, ber_len_t len )
{
	ber_len_t l;

	for ( l = 0; l < len && s[l] != '\0'; l++ ) ;

	return l;
}

char *
ber_strndup_x( LDAP_CONST char *s, ber_len_t l, void *ctx )
{
	char    *p;
	size_t	len;
	
#ifdef LDAP_MEMORY_DEBUG
	assert(s != NULL);			/* bv damn better point to something */
#endif

	if( s == NULL ) {
		ber_errno = LBER_ERROR_PARAM;
		return NULL;
	}

	len = ber_strnlen( s, l );
	if ( (p = ber_memalloc_x( len + 1, ctx )) != NULL ) {
		AC_MEMCPY( p, s, len );
		p[len] = '\0';
	}

	return p;
}

char *
ber_strndup( LDAP_CONST char *s, ber_len_t l )
{
	return ber_strndup_x( s, l, NULL );
}

/*
 * dst is resized as required by src and the value of src is copied into dst
 * dst->bv_val must be NULL (and dst->bv_len must be 0), or it must be
 * alloc'ed with the context ctx
 */
struct berval *
ber_bvreplace_x( struct berval *dst, LDAP_CONST struct berval *src, void *ctx )
{
	assert( dst != NULL );
	assert( !BER_BVISNULL( src ) );

	if ( BER_BVISNULL( dst ) || dst->bv_len < src->bv_len ) {
		dst->bv_val = ber_memrealloc_x( dst->bv_val, src->bv_len + 1, ctx );
	}

	AC_MEMCPY( dst->bv_val, src->bv_val, src->bv_len + 1 );
	dst->bv_len = src->bv_len;

	return dst;
}

struct berval *
ber_bvreplace( struct berval *dst, LDAP_CONST struct berval *src )
{
	return ber_bvreplace_x( dst, src, NULL );
}

void
ber_bvarray_free_x( BerVarray a, void *ctx )
{
	int i;

	if (a) {
		BER_MEM_VALID( a );

		/* count elements */
		for (i=0; a[i].bv_val; i++) ;
		
		/* free in reverse order */
		for (i--; i>=0; i--) {
			ber_memfree_x(a[i].bv_val, ctx);
		}

		ber_memfree_x(a, ctx);
	}
}

void
ber_bvarray_free( BerVarray a )
{
	ber_bvarray_free_x(a, NULL);
}

int
ber_bvarray_dup_x( BerVarray *dst, BerVarray src, void *ctx )
{
	int i, j;
	BerVarray new;

	if ( !src ) {
		*dst = NULL;
		return 0;
	}

	for (i=0; !BER_BVISNULL( &src[i] ); i++) ;
	new = ber_memalloc_x(( i+1 ) * sizeof(BerValue), ctx );
	if ( !new )
		return -1;
	for (j=0; j<i; j++) {
		ber_dupbv_x( &new[j], &src[j], ctx );
		if ( BER_BVISNULL( &new[j] )) {
			ber_bvarray_free_x( new, ctx );
			return -1;
		}
	}
	BER_BVZERO( &new[j] );
	*dst = new;
	return 0;
}

int
ber_bvarray_add_x( BerVarray *a, BerValue *bv, void *ctx )
{
	int	n;

	if ( *a == NULL ) {
		if (bv == NULL) {
			return 0;
		}
		n = 0;

		*a = (BerValue *) ber_memalloc_x( 2 * sizeof(BerValue), ctx );
		if ( *a == NULL ) {
			return -1;
		}

	} else {
		BerVarray atmp;
		BER_MEM_VALID( a );

		for ( n = 0; *a != NULL && (*a)[n].bv_val != NULL; n++ ) {
			;	/* just count them */
		}

		if (bv == NULL) {
			return n;
		}

		atmp = (BerValue *) ber_memrealloc_x( (char *) *a,
		    (n + 2) * sizeof(BerValue), ctx );

		if( atmp == NULL ) {
			return -1;
		}

		*a = atmp;
	}

	(*a)[n++] = *bv;
	(*a)[n].bv_val = NULL;
	(*a)[n].bv_len = 0;

	return n;
}

int
ber_bvarray_add( BerVarray *a, BerValue *bv )
{
	return ber_bvarray_add_x( a, bv, NULL );
}
