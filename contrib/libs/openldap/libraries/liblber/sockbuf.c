/* sockbuf.c - i/o routines with support for adding i/o layers. */
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

#include <ac/stdlib.h>

#include <ac/ctype.h>
#include <ac/errno.h>
#include <ac/socket.h>
#include <ac/string.h>
#include <ac/unistd.h>

#ifdef HAVE_IO_H
#include <io.h>
#endif /* HAVE_IO_H */

#if defined( HAVE_FCNTL_H )
#include <fcntl.h>
#endif

#if defined( HAVE_SYS_FILIO_H )
#include <sys/filio.h>
#elif defined( HAVE_SYS_IOCTL_H )
#include <sys/ioctl.h>
#endif

#include "lber-int.h"

#ifndef LBER_MIN_BUFF_SIZE
#define LBER_MIN_BUFF_SIZE		4096
#endif
#ifndef LBER_MAX_BUFF_SIZE
#define LBER_MAX_BUFF_SIZE		(65536*256)
#endif
#ifndef LBER_DEFAULT_READAHEAD
#define LBER_DEFAULT_READAHEAD	16384
#endif

Sockbuf *
ber_sockbuf_alloc( void )
{
	Sockbuf			*sb;

	sb = LBER_CALLOC( 1, sizeof( Sockbuf ) );

	if( sb == NULL ) return NULL;

	ber_int_sb_init( sb );
	return sb;
}

void
ber_sockbuf_free( Sockbuf *sb )
{
	assert( sb != NULL );
	assert( SOCKBUF_VALID( sb ) );

	ber_int_sb_close( sb );
	ber_int_sb_destroy( sb );
	LBER_FREE( sb );
}

/* Return values: -1: error, 0: no operation performed or the answer is false,
 * 1: successful operation or the answer is true
 */
int
ber_sockbuf_ctrl( Sockbuf *sb, int opt, void *arg )
{
	Sockbuf_IO_Desc		*p;
	int			ret = 0;

	assert( sb != NULL );
	assert( SOCKBUF_VALID( sb ) );

	switch ( opt ) {
		case LBER_SB_OPT_HAS_IO:
			p = sb->sb_iod;
			while ( p && p->sbiod_io != (Sockbuf_IO *)arg ) {
				p = p->sbiod_next;
			}
   
			if ( p ) {
				ret = 1;
			}
			break;

		case LBER_SB_OPT_GET_FD:
			if ( arg != NULL ) {
				*((ber_socket_t *)arg) = sb->sb_fd;
			}
			ret = ( sb->sb_fd == AC_SOCKET_INVALID ? -1 : 1);
			break;

		case LBER_SB_OPT_SET_FD:
			sb->sb_fd = *((ber_socket_t *)arg);
			ret = 1;
			break;

		case LBER_SB_OPT_SET_NONBLOCK:
			ret = ber_pvt_socket_set_nonblock( sb->sb_fd, arg != NULL)
				? -1 : 1;
			break;

		case LBER_SB_OPT_DRAIN: {
				/* Drain the data source to enable possible errors (e.g.
				 * TLS) to be propagated to the upper layers
				 */
				char buf[LBER_MIN_BUFF_SIZE];

				do {
					ret = ber_int_sb_read( sb, buf, sizeof( buf ) );
				} while ( ret == sizeof( buf ) );

				ret = 1;
			} break;

		case LBER_SB_OPT_NEEDS_READ:
			ret = ( sb->sb_trans_needs_read ? 1 : 0 );
			break;

		case LBER_SB_OPT_NEEDS_WRITE:
			ret = ( sb->sb_trans_needs_write ? 1 : 0 );
			break;

		case LBER_SB_OPT_GET_MAX_INCOMING:
			if ( arg != NULL ) {
				*((ber_len_t *)arg) = sb->sb_max_incoming;
			}
			ret = 1;
			break;

		case LBER_SB_OPT_SET_MAX_INCOMING:
			sb->sb_max_incoming = *((ber_len_t *)arg);
			ret = 1;
			break;

		case LBER_SB_OPT_UNGET_BUF:
#ifdef LDAP_PF_LOCAL_SENDMSG
			sb->sb_ungetlen = ((struct berval *)arg)->bv_len;
			if ( sb->sb_ungetlen <= sizeof( sb->sb_ungetbuf )) {
				AC_MEMCPY( sb->sb_ungetbuf, ((struct berval *)arg)->bv_val,
					sb->sb_ungetlen );
				ret = 1;
			} else {
				sb->sb_ungetlen = 0;
				ret = -1;
			}
#endif
			break;

		default:
			ret = sb->sb_iod->sbiod_io->sbi_ctrl( sb->sb_iod, opt, arg );
			break;
   }

	return ret;
}

int
ber_sockbuf_add_io( Sockbuf *sb, Sockbuf_IO *sbio, int layer, void *arg )
{
	Sockbuf_IO_Desc		*d, *p, **q;
   
	assert( sb != NULL );
	assert( SOCKBUF_VALID( sb ) );
   
	if ( sbio == NULL ) {
		return -1;
	}
   
	q = &sb->sb_iod;
	p = *q;
	while ( p && p->sbiod_level > layer ) {
		q = &p->sbiod_next;
		p = *q;
	}
   
	d = LBER_MALLOC( sizeof( *d ) );
	if ( d == NULL ) {
		return -1;
	}
   
	d->sbiod_level = layer;
	d->sbiod_sb = sb;
	d->sbiod_io = sbio;
	memset( &d->sbiod_pvt, '\0', sizeof( d->sbiod_pvt ) );
	d->sbiod_next = p;
	*q = d;

	if ( sbio->sbi_setup != NULL && ( sbio->sbi_setup( d, arg ) < 0 ) ) {
		return -1;
	}

	return 0;
}
   
int
ber_sockbuf_remove_io( Sockbuf *sb, Sockbuf_IO *sbio, int layer )
{
	Sockbuf_IO_Desc		*p, **q;

	assert( sb != NULL );
	assert( SOCKBUF_VALID( sb ) );
   
	if ( sb->sb_iod == NULL ) {
		return -1;
	}
   
	q = &sb->sb_iod;
	while ( *q != NULL ) {
		p = *q;
		if ( layer == p->sbiod_level && p->sbiod_io == sbio ) {
			if ( p->sbiod_io->sbi_remove != NULL &&
				p->sbiod_io->sbi_remove( p ) < 0 )
			{
				return -1;
			}
			*q = p->sbiod_next;
			LBER_FREE( p );
			break;
		}
		q = &p->sbiod_next;
	}

	return 0;
}

void
ber_pvt_sb_buf_init( Sockbuf_Buf *buf )
{
	buf->buf_base = NULL;
	buf->buf_ptr = 0;
	buf->buf_end = 0;
	buf->buf_size = 0;
}

void
ber_pvt_sb_buf_destroy( Sockbuf_Buf *buf )
{
	assert( buf != NULL);

	if (buf->buf_base) {
		LBER_FREE( buf->buf_base );
	}
	ber_pvt_sb_buf_init( buf );
}

int
ber_pvt_sb_grow_buffer( Sockbuf_Buf *buf, ber_len_t minsize )
{
	ber_len_t		pw;
	char			*p;
   
	assert( buf != NULL );

	for ( pw = LBER_MIN_BUFF_SIZE; pw < minsize; pw <<= 1 ) {
		if (pw > LBER_MAX_BUFF_SIZE) return -1;
	}

	if ( buf->buf_size < pw ) {
		p = LBER_REALLOC( buf->buf_base, pw );
		if ( p == NULL ) return -1;
		buf->buf_base = p;
		buf->buf_size = pw;
	}
	return 0;
}

ber_len_t
ber_pvt_sb_copy_out( Sockbuf_Buf *sbb, char *buf, ber_len_t len )
{
	ber_len_t		max;

	assert( buf != NULL );
	assert( sbb != NULL );
#if 0
	assert( sbb->buf_size > 0 );
#endif

	max = sbb->buf_end - sbb->buf_ptr;
	max = ( max < len) ? max : len;
	if ( max ) {
		AC_MEMCPY( buf, sbb->buf_base + sbb->buf_ptr, max );
		sbb->buf_ptr += max;
		if ( sbb->buf_ptr >= sbb->buf_end ) {
			sbb->buf_ptr = sbb->buf_end = 0;
		}
   }
	return max;
}

ber_slen_t
ber_pvt_sb_do_write( Sockbuf_IO_Desc *sbiod, Sockbuf_Buf *buf_out )
{
	ber_len_t		to_go;
	ber_slen_t ret;

	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

	to_go = buf_out->buf_end - buf_out->buf_ptr;
	assert( to_go > 0 );
   
	for(;;) {
		ret = LBER_SBIOD_WRITE_NEXT( sbiod, buf_out->buf_base +
			buf_out->buf_ptr, to_go );
#ifdef EINTR
		if ((ret<0) && (errno==EINTR)) continue;
#endif
		break;
	}

	if ( ret <= 0 ) return ret;
   
	buf_out->buf_ptr += ret;
	if (buf_out->buf_ptr == buf_out->buf_end) {
		buf_out->buf_end = buf_out->buf_ptr = 0;
	}

	return ret;
}

int
ber_pvt_socket_set_nonblock( ber_socket_t sd, int nb )
{
#ifdef HAVE_FCNTL
	int flags = fcntl( sd, F_GETFL);
	if( nb ) {
		flags |= O_NONBLOCK;
	} else {
		flags &= ~O_NONBLOCK;
	}
	return fcntl( sd, F_SETFL, flags );
		
#elif defined( FIONBIO )
	ioctl_t status = nb ? 1 : 0;
	return ioctl( sd, FIONBIO, &status );
#endif
}

int
ber_int_sb_init( Sockbuf *sb )
{
	assert( sb != NULL);

	sb->sb_valid=LBER_VALID_SOCKBUF;
	sb->sb_options = 0;
	sb->sb_debug = ber_int_debug;
	sb->sb_fd = AC_SOCKET_INVALID;
	sb->sb_iod = NULL;
	sb->sb_trans_needs_read = 0;
	sb->sb_trans_needs_write = 0;
   
	assert( SOCKBUF_VALID( sb ) );
	return 0;
}
   
int
ber_int_sb_close( Sockbuf *sb )
{
	Sockbuf_IO_Desc		*p;

	assert( sb != NULL);
   
	p = sb->sb_iod;
	while ( p ) {
		if ( p->sbiod_io->sbi_close && p->sbiod_io->sbi_close( p ) < 0 ) {
			return -1;
		}
		p = p->sbiod_next;
	}
   
	sb->sb_fd = AC_SOCKET_INVALID;
   
	return 0;
}

int
ber_int_sb_destroy( Sockbuf *sb )
{
	Sockbuf_IO_Desc		*p;

	assert( sb != NULL);
	assert( SOCKBUF_VALID( sb ) );
   
	while ( sb->sb_iod ) {
		p = sb->sb_iod->sbiod_next;
		ber_sockbuf_remove_io( sb, sb->sb_iod->sbiod_io,
			sb->sb_iod->sbiod_level );
		sb->sb_iod = p;
	}

	return ber_int_sb_init( sb );
}

ber_slen_t
ber_int_sb_read( Sockbuf *sb, void *buf, ber_len_t len )
{
	ber_slen_t		ret;

	assert( buf != NULL );
	assert( sb != NULL);
	assert( sb->sb_iod != NULL );
	assert( SOCKBUF_VALID( sb ) );

	for (;;) {
		ret = sb->sb_iod->sbiod_io->sbi_read( sb->sb_iod, buf, len );

#ifdef EINTR	
		if ( ( ret < 0 ) && ( errno == EINTR ) ) continue;
#endif
		break;
	}

	return ret;
}

ber_slen_t
ber_int_sb_write( Sockbuf *sb, void *buf, ber_len_t len )
{
	ber_slen_t		ret;

	assert( buf != NULL );
	assert( sb != NULL);
	assert( sb->sb_iod != NULL );
	assert( SOCKBUF_VALID( sb ) );

	for (;;) {
		ret = sb->sb_iod->sbiod_io->sbi_write( sb->sb_iod, buf, len );

#ifdef EINTR	
		if ( ( ret < 0 ) && ( errno == EINTR ) ) continue;
#endif
		break;
	}

	return ret;
}

/*
 * Support for TCP
 */

static ber_slen_t
sb_stream_read( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	assert( sbiod != NULL);
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

#if defined(MACOS)
/*
 * MacTCP/OpenTransport
 */
	return tcpread( sbiod->sbiod_sb->sb_fd, 0, (unsigned char *)buf,
		len, NULL );

#elif defined( HAVE_PCNFS ) || \
   defined( HAVE_WINSOCK ) || defined ( __BEOS__ )
/*
 * PCNFS (under DOS)
 */
/*
 * Windows Socket API (under DOS/Windows 3.x)
 */
/*
 * 32-bit Windows Socket API (under Windows NT or Windows 95)
 */
	return recv( sbiod->sbiod_sb->sb_fd, buf, len, 0 );

#elif defined( HAVE_NCSA )
/*
 * NCSA Telnet TCP/IP stack (under DOS)
 */
	return nread( sbiod->sbiod_sb->sb_fd, buf, len );

#else
	return read( sbiod->sbiod_sb->sb_fd, buf, len );
#endif
}

static ber_slen_t
sb_stream_write( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	assert( sbiod != NULL);
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

#if defined(MACOS) 
/*
 * MacTCP/OpenTransport
 */
#define MAX_WRITE	65535
	return tcpwrite( sbiod->sbiod_sb->sb_fd, (unsigned char *)buf,
		(len<MAX_WRITE) ? len : MAX_WRITE );

#elif defined( HAVE_PCNFS) \
	|| defined( HAVE_WINSOCK) || defined ( __BEOS__ )
/*
 * PCNFS (under DOS)
 */
/*
 * Windows Socket API (under DOS/Windows 3.x)
 */
/*
 * 32-bit Windows Socket API (under Windows NT or Windows 95)
 */
	return send( sbiod->sbiod_sb->sb_fd, buf, len, 0 );

#elif defined(HAVE_NCSA)
	return netwrite( sbiod->sbiod_sb->sb_fd, buf, len );

#elif defined(VMS)
/*
 * VMS -- each write must be 64K or smaller
 */
#define MAX_WRITE 65535
	return write( sbiod->sbiod_sb->sb_fd, buf,
		(len<MAX_WRITE) ? len : MAX_WRITE);
#else
	return write( sbiod->sbiod_sb->sb_fd, buf, len );
#endif   
}   
   
static int 
sb_stream_close( Sockbuf_IO_Desc *sbiod )
{
	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );
	if ( sbiod->sbiod_sb->sb_fd != AC_SOCKET_INVALID )
		tcp_close( sbiod->sbiod_sb->sb_fd );
   return 0;
}

/* The argument is a pointer to the socket descriptor */
static int
sb_stream_setup( Sockbuf_IO_Desc *sbiod, void *arg ) {
	assert( sbiod != NULL );

	if ( arg != NULL ) {
		sbiod->sbiod_sb->sb_fd = *((int *)arg);
	}
	return 0;
}

static int
sb_stream_ctrl( Sockbuf_IO_Desc *sbiod, int opt, void *arg ) {
	/* This is an end IO descriptor */
	return 0;
}

Sockbuf_IO ber_sockbuf_io_tcp = {
	sb_stream_setup,	/* sbi_setup */
	NULL,				/* sbi_remove */
	sb_stream_ctrl,		/* sbi_ctrl */
	sb_stream_read,		/* sbi_read */
	sb_stream_write,	/* sbi_write */
	sb_stream_close		/* sbi_close */
};


/*
 * Support for readahead (UDP needs it)
 */

static int
sb_rdahead_setup( Sockbuf_IO_Desc *sbiod, void *arg )
{
	Sockbuf_Buf		*p;

	assert( sbiod != NULL );

	p = LBER_MALLOC( sizeof( *p ) );
	if ( p == NULL ) return -1;

	ber_pvt_sb_buf_init( p );

	if ( arg == NULL ) {
		ber_pvt_sb_grow_buffer( p, LBER_DEFAULT_READAHEAD );
	} else {
		ber_pvt_sb_grow_buffer( p, *((int *)arg) );
	}

	sbiod->sbiod_pvt = p;
	return 0;
}

static int
sb_rdahead_remove( Sockbuf_IO_Desc *sbiod )
{
	Sockbuf_Buf		*p;

	assert( sbiod != NULL );

	p = (Sockbuf_Buf *)sbiod->sbiod_pvt;

	if ( p->buf_ptr != p->buf_end ) return -1;

	ber_pvt_sb_buf_destroy( (Sockbuf_Buf *)(sbiod->sbiod_pvt) );
	LBER_FREE( sbiod->sbiod_pvt );
	sbiod->sbiod_pvt = NULL;

	return 0;
}

static ber_slen_t
sb_rdahead_read( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	Sockbuf_Buf		*p;
	ber_slen_t		bufptr = 0, ret, max;

	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );
	assert( sbiod->sbiod_next != NULL );

	p = (Sockbuf_Buf *)sbiod->sbiod_pvt;

	assert( p->buf_size > 0 );

	/* Are there anything left in the buffer? */
	ret = ber_pvt_sb_copy_out( p, buf, len );
	bufptr += ret;
	len -= ret;

	if ( len == 0 ) return bufptr;

	max = p->buf_size - p->buf_end;
	ret = 0;
	while ( max > 0 ) {
		ret = LBER_SBIOD_READ_NEXT( sbiod, p->buf_base + p->buf_end,
			max );
#ifdef EINTR	
		if ( ( ret < 0 ) && ( errno == EINTR ) ) continue;
#endif
		break;
	}

	if ( ret < 0 ) {
		return ( bufptr ? bufptr : ret );
	}

	p->buf_end += ret;
	bufptr += ber_pvt_sb_copy_out( p, (char *) buf + bufptr, len );
	return bufptr;
}

static ber_slen_t
sb_rdahead_write( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	assert( sbiod != NULL );
	assert( sbiod->sbiod_next != NULL );

	return LBER_SBIOD_WRITE_NEXT( sbiod, buf, len );
}

static int
sb_rdahead_close( Sockbuf_IO_Desc *sbiod )
{
	assert( sbiod != NULL );

	/* Just erase the buffer */
	ber_pvt_sb_buf_destroy((Sockbuf_Buf *)sbiod->sbiod_pvt);
	return 0;
}

static int
sb_rdahead_ctrl( Sockbuf_IO_Desc *sbiod, int opt, void *arg )
{
	Sockbuf_Buf		*p;

	p = (Sockbuf_Buf *)sbiod->sbiod_pvt;

	if ( opt == LBER_SB_OPT_DATA_READY ) {
		if ( p->buf_ptr != p->buf_end ) {
			return 1;
		}

	} else if ( opt == LBER_SB_OPT_SET_READAHEAD ) {
		if ( p->buf_size >= *((ber_len_t *)arg) ) {
			return 0;
		}
		return ( ber_pvt_sb_grow_buffer( p, *((int *)arg) ) ?
			-1 : 1 );
	}

	return LBER_SBIOD_CTRL_NEXT( sbiod, opt, arg );
}

Sockbuf_IO ber_sockbuf_io_readahead = {
	sb_rdahead_setup,	/* sbi_setup */
	sb_rdahead_remove,	/* sbi_remove */
	sb_rdahead_ctrl,	/* sbi_ctrl */
	sb_rdahead_read,	/* sbi_read */
	sb_rdahead_write,	/* sbi_write */
	sb_rdahead_close	/* sbi_close */
};

/*
 * Support for simple file IO
 */

static ber_slen_t
sb_fd_read( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	assert( sbiod != NULL);
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

#ifdef LDAP_PF_LOCAL_SENDMSG
	if ( sbiod->sbiod_sb->sb_ungetlen ) {
		ber_len_t blen = sbiod->sbiod_sb->sb_ungetlen;
		if ( blen > len )
			blen = len;
		AC_MEMCPY( buf, sbiod->sbiod_sb->sb_ungetbuf, blen );
		buf = (char *) buf + blen;
		len -= blen;
		sbiod->sbiod_sb->sb_ungetlen -= blen;
		if ( sbiod->sbiod_sb->sb_ungetlen ) {
			AC_MEMCPY( sbiod->sbiod_sb->sb_ungetbuf,
				sbiod->sbiod_sb->sb_ungetbuf+blen,
				sbiod->sbiod_sb->sb_ungetlen );
		}
		if ( len == 0 )
			return blen;
	}
#endif
	return read( sbiod->sbiod_sb->sb_fd, buf, len );
}

static ber_slen_t
sb_fd_write( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	assert( sbiod != NULL);
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

	return write( sbiod->sbiod_sb->sb_fd, buf, len );
}

static int 
sb_fd_close( Sockbuf_IO_Desc *sbiod )
{   
	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

	if ( sbiod->sbiod_sb->sb_fd != AC_SOCKET_INVALID )
		close( sbiod->sbiod_sb->sb_fd );
	return 0;
}

/* The argument is a pointer to the file descriptor */
static int
sb_fd_setup( Sockbuf_IO_Desc *sbiod, void *arg ) {
	assert( sbiod != NULL );

	if ( arg != NULL )
		sbiod->sbiod_sb->sb_fd = *((int *)arg);
	return 0;
}

static int
sb_fd_ctrl( Sockbuf_IO_Desc *sbiod, int opt, void *arg ) {
	/* This is an end IO descriptor */
	return 0;
}

Sockbuf_IO ber_sockbuf_io_fd = {
	sb_fd_setup,	/* sbi_setup */
	NULL,			/* sbi_remove */
	sb_fd_ctrl,		/* sbi_ctrl */
	sb_fd_read,		/* sbi_read */
	sb_fd_write,		/* sbi_write */
	sb_fd_close		/* sbi_close */
};

/*
 * Debugging layer
 */

static int
sb_debug_setup( Sockbuf_IO_Desc *sbiod, void *arg )
{
	assert( sbiod != NULL );
	
	if ( arg == NULL ) arg = "sockbuf_";

	sbiod->sbiod_pvt = LBER_MALLOC( strlen( arg ) + 1 );
	if ( sbiod->sbiod_pvt == NULL ) return -1;

	strcpy( (char *)sbiod->sbiod_pvt, (char *)arg );
	return 0;
}

static int
sb_debug_remove( Sockbuf_IO_Desc *sbiod )
{
	assert( sbiod != NULL );
	assert( sbiod->sbiod_pvt != NULL );

	LBER_FREE( sbiod->sbiod_pvt );
	sbiod->sbiod_pvt = NULL;
	return 0;
}

static int
sb_debug_ctrl( Sockbuf_IO_Desc *sbiod, int opt, void *arg )
{
	return LBER_SBIOD_CTRL_NEXT( sbiod, opt, arg );
}

static ber_slen_t
sb_debug_read( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	ber_slen_t		ret;
	char ebuf[128];

	ret = LBER_SBIOD_READ_NEXT( sbiod, buf, len );
	if (sbiod->sbiod_sb->sb_debug & LDAP_DEBUG_PACKETS) {
		int err = sock_errno();
		if ( ret < 0 ) {
			ber_log_printf( LDAP_DEBUG_PACKETS, sbiod->sbiod_sb->sb_debug,
				"%sread: want=%ld error=%s\n", (char *)sbiod->sbiod_pvt,
				(long)len, AC_STRERROR_R( err, ebuf, sizeof ebuf ) );
		} else {
			ber_log_printf( LDAP_DEBUG_PACKETS, sbiod->sbiod_sb->sb_debug,
				"%sread: want=%ld, got=%ld\n", (char *)sbiod->sbiod_pvt,
				(long)len, (long)ret );
			ber_log_bprint( LDAP_DEBUG_PACKETS, sbiod->sbiod_sb->sb_debug,
				(const char *)buf, ret );
		}
		sock_errset(err);
	}
	return ret;
}

static ber_slen_t
sb_debug_write( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	ber_slen_t		ret;
	char ebuf[128];

	ret = LBER_SBIOD_WRITE_NEXT( sbiod, buf, len );
	if (sbiod->sbiod_sb->sb_debug & LDAP_DEBUG_PACKETS) {
		int err = sock_errno();
		if ( ret < 0 ) {
			ber_log_printf( LDAP_DEBUG_PACKETS, sbiod->sbiod_sb->sb_debug,
				"%swrite: want=%ld error=%s\n",
				(char *)sbiod->sbiod_pvt, (long)len,
				AC_STRERROR_R( err, ebuf, sizeof ebuf ) );
		} else {
			ber_log_printf( LDAP_DEBUG_PACKETS, sbiod->sbiod_sb->sb_debug,
				"%swrite: want=%ld, written=%ld\n",
				(char *)sbiod->sbiod_pvt, (long)len, (long)ret );
			ber_log_bprint( LDAP_DEBUG_PACKETS, sbiod->sbiod_sb->sb_debug,
				(const char *)buf, ret );
		}
		sock_errset(err);
	}

	return ret;
}

Sockbuf_IO ber_sockbuf_io_debug = {
	sb_debug_setup,		/* sbi_setup */
	sb_debug_remove,	/* sbi_remove */
	sb_debug_ctrl,		/* sbi_ctrl */
	sb_debug_read,		/* sbi_read */
	sb_debug_write,		/* sbi_write */
	NULL				/* sbi_close */
};

#ifdef LDAP_CONNECTIONLESS

/*
 * Support for UDP (CLDAP)
 *
 * All I/O at this level must be atomic. For ease of use, the sb_readahead
 * must be used above this module. All data reads and writes are prefixed
 * with a sockaddr_storage containing the address of the remote entity. Upper levels
 * must read and write this sockaddr_storage before doing the usual ber_printf/scanf
 * operations on LDAP messages.
 */

static int 
sb_dgram_setup( Sockbuf_IO_Desc *sbiod, void *arg )
{
	assert( sbiod != NULL);
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

	if ( arg != NULL ) sbiod->sbiod_sb->sb_fd = *((int *)arg);
	return 0;
}

static ber_slen_t
sb_dgram_read( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	ber_slen_t rc;
	ber_socklen_t addrlen;
	struct sockaddr *src;
   
	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );
	assert( buf != NULL );

	addrlen = sizeof( struct sockaddr_storage );
	src = buf;
	buf = (char *) buf + addrlen;
	len -= addrlen;
	rc = recvfrom( sbiod->sbiod_sb->sb_fd, buf, len, 0, src, &addrlen );

	return rc > 0 ? rc+sizeof(struct sockaddr_storage) : rc;
}

static ber_slen_t 
sb_dgram_write( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len )
{
	ber_slen_t rc;
	struct sockaddr *dst;
	socklen_t dstsize;
   
	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );
	assert( buf != NULL );

	dst = buf;
	buf = (char *) buf + sizeof( struct sockaddr_storage );
	len -= sizeof( struct sockaddr_storage );
	dstsize = dst->sa_family == AF_INET ? sizeof( struct sockaddr_in )
#ifdef LDAP_PF_INET6
		: dst->sa_family == AF_INET6 ? sizeof( struct sockaddr_in6 )
#endif
		: sizeof( struct sockaddr_storage );
	rc = sendto( sbiod->sbiod_sb->sb_fd, buf, len, 0, dst, dstsize );

	if ( rc < 0 ) return -1;
   
	/* fake error if write was not atomic */
	if (rc < len) {
# ifdef EMSGSIZE
		errno = EMSGSIZE;
# endif
		return -1;
	}
	rc = len + sizeof(struct sockaddr_storage);
	return rc;
}

static int 
sb_dgram_close( Sockbuf_IO_Desc *sbiod )
{
	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );
  
	if ( sbiod->sbiod_sb->sb_fd != AC_SOCKET_INVALID )
		tcp_close( sbiod->sbiod_sb->sb_fd );
	return 0;
}

static int
sb_dgram_ctrl( Sockbuf_IO_Desc *sbiod, int opt, void *arg )
{
	/* This is an end IO descriptor */
	return 0;
}

Sockbuf_IO ber_sockbuf_io_udp =
{
	sb_dgram_setup,		/* sbi_setup */
	NULL,			/* sbi_remove */
	sb_dgram_ctrl,		/* sbi_ctrl */
	sb_dgram_read,		/* sbi_read */
	sb_dgram_write,		/* sbi_write */
	sb_dgram_close		/* sbi_close */
};

#endif	/* LDAP_CONNECTIONLESS */
