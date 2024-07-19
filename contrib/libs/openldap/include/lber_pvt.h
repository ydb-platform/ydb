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
 * lber_pvt.h - Header for ber_pvt_ functions. 
 * These are meant to be internal to OpenLDAP Software.
 */

#ifndef _LBER_PVT_H
#define _LBER_PVT_H 1

#include <lber.h>

LDAP_BEGIN_DECL

/* for allocating aligned buffers (on the stack) */
#define LBER_ALIGNED_BUFFER(uname,size) \
	union uname { \
		char buffer[size]; \
		/* force alignment */ \
		int ialign; \
		long lalign; \
		float falign; \
		double dalign; \
		char* palign; \
	}

#define LBER_ELEMENT_SIZEOF (256) /* must be >= sizeof(BerElement) */
typedef LBER_ALIGNED_BUFFER(lber_berelement_u,LBER_ELEMENT_SIZEOF)
	BerElementBuffer;

typedef struct sockbuf_buf {
	ber_len_t		buf_size;
	ber_len_t		buf_ptr;
	ber_len_t		buf_end;
	char			*buf_base;
} Sockbuf_Buf;

/*
 * bprint.c
 */
LBER_V( BER_LOG_PRINT_FN ) ber_pvt_log_print;

LBER_F( int )
ber_pvt_log_printf LDAP_P((
	int errlvl,
	int loglvl,
	const char *fmt,
	... )) LDAP_GCCATTR((format(printf, 3, 4)));

/*
 * sockbuf.c
 */
LBER_F( ber_slen_t )
ber_pvt_sb_do_write LDAP_P(( Sockbuf_IO_Desc *sbiod, Sockbuf_Buf *buf_out ));

LBER_F( void )
ber_pvt_sb_buf_init LDAP_P(( Sockbuf_Buf *buf ));

LBER_F( void )
ber_pvt_sb_buf_destroy LDAP_P(( Sockbuf_Buf *buf ));

LBER_F( int )
ber_pvt_sb_grow_buffer LDAP_P(( Sockbuf_Buf *buf, ber_len_t minsize ));

LBER_F( ber_len_t )
ber_pvt_sb_copy_out LDAP_P(( Sockbuf_Buf *sbb, char *buf, ber_len_t len ));

LBER_F( int )
ber_pvt_socket_set_nonblock LDAP_P(( ber_socket_t sd, int nb ));

/*
 * memory.c
 */
LBER_F( void * )
ber_memalloc_x LDAP_P((
	ber_len_t s, void *ctx));

LBER_F( void * )
ber_memrealloc_x LDAP_P((
	void* p,
	ber_len_t s, void *ctx ));

LBER_F( void * )
ber_memcalloc_x LDAP_P((
	ber_len_t n,
	ber_len_t s, void *ctx ));

LBER_F( void )
ber_memfree_x LDAP_P((
	void* p, void *ctx ));

LBER_F( void )
ber_memvfree_x LDAP_P((
	void** vector, void *ctx ));

LBER_F( void )
ber_bvfree_x LDAP_P((
	struct berval *bv, void *ctx ));

LBER_F( void )
ber_bvecfree_x LDAP_P((
	struct berval **bv, void *ctx ));

LBER_F( int )
ber_bvecadd_x LDAP_P((
	struct berval ***bvec,
	struct berval *bv, void *ctx ));

LBER_F( struct berval * )
ber_dupbv_x LDAP_P((
	struct berval *dst, struct berval *src, void *ctx ));

LBER_F( struct berval * )
ber_str2bv_x LDAP_P((
	LDAP_CONST char *, ber_len_t len, int dup, struct berval *bv, void *ctx));

LBER_F( struct berval * )
ber_mem2bv_x LDAP_P((
	LDAP_CONST char *, ber_len_t len, int dup, struct berval *bv, void *ctx));

LBER_F( char * )
ber_strdup_x LDAP_P((
	LDAP_CONST char *, void *ctx ));

LBER_F( struct berval * )
ber_bvreplace_x LDAP_P((
	struct berval *dst, LDAP_CONST struct berval *src, void *ctx ));

LBER_F( void )
ber_bvarray_free_x LDAP_P(( BerVarray p, void *ctx ));

LBER_F( int )
ber_bvarray_add_x LDAP_P(( BerVarray *p, BerValue *bv, void *ctx ));

LBER_F( int )
ber_bvarray_dup_x LDAP_P(( BerVarray *dst, BerVarray src, void *ctx ));

#if 0
#define ber_bvstrcmp(v1,v2) \
	((v1)->bv_len < (v2)->bv_len \
		? -1 : ((v1)->bv_len > (v2)->bv_len \
			? 1 : strncmp((v1)->bv_val, (v2)->bv_val, (v1)->bv_len) ))
#else
	/* avoid strncmp() */
#define ber_bvstrcmp(v1,v2)	ber_bvcmp((v1),(v2))
#endif

#define ber_bvstrcasecmp(v1,v2) \
	((v1)->bv_len < (v2)->bv_len \
		? -1 : ((v1)->bv_len > (v2)->bv_len \
			? 1 : strncasecmp((v1)->bv_val, (v2)->bv_val, (v1)->bv_len) ))

#define ber_bvccmp(v1,c) \
	( (v1)->bv_len == 1 && (v1)->bv_val[0] == (c) )

#define ber_strccmp(s,c) \
	( (s)[0] == (c) && (s)[1] == '\0' )

#define ber_bvchr(bv,c) \
	((char *) memchr( (bv)->bv_val, (c), (bv)->bv_len ))

#define ber_bvrchr(bv,c) \
	((char *) lutil_memrchr( (bv)->bv_val, (c), (bv)->bv_len ))

#define ber_bvchr_post(dst,bv,c) \
	do { \
		(dst)->bv_val = memchr( (bv)->bv_val, (c), (bv)->bv_len ); \
		(dst)->bv_len = (dst)->bv_val ? (bv)->bv_len - ((dst)->bv_val - (bv)->bv_val) : 0; \
	} while (0)

#define ber_bvchr_pre(dst,bv,c) \
	do { \
		(dst)->bv_val = memchr( (bv)->bv_val, (c), (bv)->bv_len ); \
		(dst)->bv_len = (dst)->bv_val ? ((dst)->bv_val - (bv)->bv_val) : (bv)->bv_len; \
		(dst)->bv_val = (bv)->bv_val; \
	} while (0)

#define ber_bvrchr_post(dst,bv,c) \
	do { \
		(dst)->bv_val = lutil_memrchr( (bv)->bv_val, (c), (bv)->bv_len ); \
		(dst)->bv_len = (dst)->bv_val ? (bv)->bv_len - ((dst)->bv_val - (bv)->bv_val) : 0; \
	} while (0)

#define ber_bvrchr_pre(dst,bv,c) \
	do { \
		(dst)->bv_val = lutil_memrchr( (bv)->bv_val, (c), (bv)->bv_len ); \
		(dst)->bv_len = (dst)->bv_val ? ((dst)->bv_val - (bv)->bv_val) : (bv)->bv_len; \
		(dst)->bv_val = (bv)->bv_val; \
	} while (0)

#define BER_STRLENOF(s)	(sizeof(s)-1)
#define BER_BVC(s)		{ BER_STRLENOF(s), (char *)(s) }
#define BER_BVNULL		{ 0L, NULL }
#define BER_BVZERO(bv) \
	do { \
		(bv)->bv_len = 0; \
		(bv)->bv_val = NULL; \
	} while (0)
#define BER_BVSTR(bv,s)	\
	do { \
		(bv)->bv_len = BER_STRLENOF(s); \
		(bv)->bv_val = (s); \
	} while (0)
#define BER_BVISNULL(bv)	((bv)->bv_val == NULL)
#define BER_BVISEMPTY(bv)	((bv)->bv_len == 0)

LDAP_END_DECL

#endif

