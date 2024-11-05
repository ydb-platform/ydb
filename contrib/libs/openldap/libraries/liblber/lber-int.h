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

#ifndef _LBER_INT_H
#define _LBER_INT_H

#include "lber.h"
#define LDAP_INT_DEBUG
#include "ldap_log.h"
#include "lber_pvt.h"
#include "ldap_queue.h"

LDAP_BEGIN_DECL

typedef void (*BER_LOG_FN)(FILE *file,
	const char *subsys, int level, const char *fmt, ... );

LBER_V (BER_ERRNO_FN) ber_int_errno_fn;

#ifdef LDAP_MEMORY_TRACE
# ifndef LDAP_MEMORY_DEBUG
#  define LDAP_MEMORY_DEBUG 1
# endif
#endif

#ifdef LDAP_MEMORY_DEBUG
LBER_V (long)	ber_int_meminuse;
#endif
#if defined(LDAP_MEMORY_DEBUG) && ((LDAP_MEMORY_DEBUG +0) & 2)
# define LDAP_MEMORY_DEBUG_ASSERT assert
#else
# define LDAP_MEMORY_DEBUG_ASSERT(expr) ((void) 0)
#endif

struct lber_options {
	short lbo_valid;
	unsigned short		lbo_options;
	int			lbo_debug;
};

LBER_F( int ) ber_pvt_log_output(
	const char *subsystem,
	int level,
	const char *fmt, ... );

#define LBER_UNINITIALIZED		0x0
#define LBER_INITIALIZED		0x1
#define LBER_VALID_BERELEMENT	0x2
#define LBER_VALID_SOCKBUF		0x3

LBER_V (struct lber_options) ber_int_options;
#define ber_int_debug ber_int_options.lbo_debug

/* Data encoded in ASN.1 BER format */
struct berelement {
	struct		lber_options ber_opts;
#define ber_valid		ber_opts.lbo_valid
#define ber_options		ber_opts.lbo_options
#define ber_debug		ber_opts.lbo_debug

	/*
	 * The members below, when not NULL/LBER_DEFAULT/etc, are:
	 *   ber_buf       Data buffer.  Other pointers normally point into it.
	 *   ber_rwptr     Read/write cursor for Sockbuf I/O.
	 *   ber_memctx    Context passed to ber_memalloc() & co.
	 * When decoding data (reading it from the BerElement):
	 *   ber_end       End of BER data.
	 *   ber_ptr       Read cursor, except for 1st octet of tags.
	 *   ber_tag       1st octet of next tag, saved from *ber_ptr when
	 *                 ber_ptr may be pointing at a tag and is >ber_buf.
	 *                 The octet *ber_ptr itself may get overwritten with
	 *                 a \0, to terminate the preceding element.
	 * When encoding data (writing it to the BerElement):
	 *   ber_end       End of allocated buffer - 1 (allowing a final \0).
	 *   ber_ptr       Last complete BER element (normally write cursor).
	 *   ber_sos_ptr   NULL or write cursor for incomplete sequence or set.
	 *   ber_sos_inner offset(seq/set length octets) if ber_sos_ptr!=NULL.
	 *   ber_tag       Default tag for next ber_printf() element.
	 *   ber_usertag   Boolean set by ber_printf "!" if it sets ber_tag.
	 *   ber_len       Reused for ber_sos_inner.
	 * When output to a Sockbuf:
	 *   ber_ptr       End of encoded data to write.
	 * When input from a Sockbuf:
	 *   See ber_get_next().
	 */

	/* Do not change the order of these 3 fields! see ber_get_next */
	ber_tag_t	ber_tag;
	ber_len_t	ber_len;
	ber_tag_t	ber_usertag;

	char		*ber_buf;
	char		*ber_ptr;
	char		*ber_end;

	char		*ber_sos_ptr;
#	define		ber_sos_inner	ber_len /* reused for binary compat */

	char		*ber_rwptr;
	void		*ber_memctx;
};
#define LBER_VALID(ber)	((ber)->ber_valid==LBER_VALID_BERELEMENT)

#define ber_pvt_ber_remaining(ber)	((ber)->ber_end - (ber)->ber_ptr)
#define ber_pvt_ber_total(ber)		((ber)->ber_end - (ber)->ber_buf)
#define ber_pvt_ber_write(ber)		((ber)->ber_ptr - (ber)->ber_buf)

struct sockbuf {
	struct lber_options sb_opts;
	Sockbuf_IO_Desc		*sb_iod;		/* I/O functions */
#define	sb_valid		sb_opts.lbo_valid
#define	sb_options		sb_opts.lbo_options
#define	sb_debug		sb_opts.lbo_debug
	ber_socket_t		sb_fd;
	ber_len_t			sb_max_incoming;
   	unsigned int		sb_trans_needs_read:1;
   	unsigned int		sb_trans_needs_write:1;
#ifdef LDAP_PF_LOCAL_SENDMSG
	char				sb_ungetlen;
	char				sb_ungetbuf[8];
#endif
};

#define SOCKBUF_VALID( sb )	( (sb)->sb_valid == LBER_VALID_SOCKBUF )


/*
 * decode.c, encode.c
 */

/* Simplest OID max-DER-component to implement in both decode and encode */
#define LBER_OID_COMPONENT_MAX ((unsigned long)-1 - 128)


/*
 * io.c
 */
LBER_F( int )
ber_realloc LDAP_P((
	BerElement *ber,
	ber_len_t len ));

LBER_F (char *) ber_start LDAP_P(( BerElement * ));
LBER_F (int) ber_len LDAP_P(( BerElement * ));
LBER_F (int) ber_ptrlen LDAP_P(( BerElement * ));
LBER_F (void) ber_rewind LDAP_P(( BerElement * ));

/*
 * bprint.c
 */
#define ber_log_printf ber_pvt_log_printf

LBER_F( int )
ber_log_bprint LDAP_P((
	int errlvl,
	int loglvl,
	const char *data,
	ber_len_t len ));

LBER_F( int )
ber_log_dump LDAP_P((
	int errlvl,
	int loglvl,
	BerElement *ber,
	int inout ));

LBER_V (BER_LOG_FN) ber_int_log_proc;
LBER_V (FILE *) ber_pvt_err_file;

/* memory.c */
	/* simple macros to realloc for now */
LBER_V (BerMemoryFunctions *)	ber_int_memory_fns;
LBER_F (char *)	ber_strndup( LDAP_CONST char *, ber_len_t );
LBER_F (char *)	ber_strndup_x( LDAP_CONST char *, ber_len_t, void *ctx );

#define LBER_MALLOC(s)		ber_memalloc((s))
#define LBER_CALLOC(n,s)	ber_memcalloc((n),(s))
#define LBER_REALLOC(p,s)	ber_memrealloc((p),(s))
#define LBER_FREE(p)		ber_memfree((p))	
#define LBER_VFREE(v)		ber_memvfree((void**)(v))
#define LBER_STRDUP(s)		ber_strdup((s))
#define LBER_STRNDUP(s,l)	ber_strndup((s),(l))

/* sockbuf.c */

LBER_F(	int )
ber_int_sb_init LDAP_P(( Sockbuf *sb ));

LBER_F( int )
ber_int_sb_close LDAP_P(( Sockbuf *sb ));

LBER_F(	int )
ber_int_sb_destroy LDAP_P(( Sockbuf *sb ));

LBER_F( ber_slen_t )
ber_int_sb_read LDAP_P(( Sockbuf *sb, void *buf, ber_len_t len ));

LBER_F( ber_slen_t )
ber_int_sb_write LDAP_P(( Sockbuf *sb, void *buf, ber_len_t len ));

LDAP_END_DECL

#endif /* _LBER_INT_H */
