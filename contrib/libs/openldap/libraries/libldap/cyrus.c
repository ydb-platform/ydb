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

#include "ldap-int.h"

#ifdef HAVE_CYRUS_SASL

#include <stdio.h>

#include <ac/socket.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>
#include <ac/errno.h>
#include <ac/ctype.h>
#include <ac/unistd.h>

#ifdef HAVE_LIMITS_H
#include <limits.h>
#endif

#ifndef INT_MAX
#define	INT_MAX	2147483647	/* 32 bit signed max */
#endif

#if !defined(HOST_NAME_MAX) && defined(_POSIX_HOST_NAME_MAX)
#define HOST_NAME_MAX _POSIX_HOST_NAME_MAX
#endif

#ifdef HAVE_SASL_SASL_H
#include <sasl/sasl.h>
#else
#include <sasl.h>
#endif

#if SASL_VERSION_MAJOR >= 2
#define SASL_CONST const
#else
#define SASL_CONST
#endif

/*
* Various Cyrus SASL related stuff.
*/

static const sasl_callback_t client_callbacks[] = {
#ifdef SASL_CB_GETREALM
	{ SASL_CB_GETREALM, NULL, NULL },
#endif
	{ SASL_CB_USER, NULL, NULL },
	{ SASL_CB_AUTHNAME, NULL, NULL },
	{ SASL_CB_PASS, NULL, NULL },
	{ SASL_CB_ECHOPROMPT, NULL, NULL },
	{ SASL_CB_NOECHOPROMPT, NULL, NULL },
	{ SASL_CB_LIST_END, NULL, NULL }
};

/*
 * ldap_int_initialize is responsible for calling this only once.
 */
int ldap_int_sasl_init( void )
{
#ifdef HAVE_SASL_VERSION
	/* stringify the version number, sasl.h doesn't do it for us */
#define VSTR0(maj, min, pat)	#maj "." #min "." #pat
#define VSTR(maj, min, pat)	VSTR0(maj, min, pat)
#define SASL_VERSION_STRING	VSTR(SASL_VERSION_MAJOR, SASL_VERSION_MINOR, \
				SASL_VERSION_STEP)
	{ int rc;
	sasl_version( NULL, &rc );
	if ( ((rc >> 16) != ((SASL_VERSION_MAJOR << 8)|SASL_VERSION_MINOR)) ||
		(rc & 0xffff) < SASL_VERSION_STEP) {
		char version[sizeof("xxx.xxx.xxxxx")];
		sprintf( version, "%u.%d.%d", (unsigned)rc >> 24, (rc >> 16) & 0xff,
			rc & 0xffff );

		Debug1( LDAP_DEBUG_ANY,
		"ldap_int_sasl_init: SASL library version mismatch:"
		" expected " SASL_VERSION_STRING ","
		" got %s\n", version );
		return -1;
	}
	}
#endif

/* SASL 2 takes care of its own memory completely internally */
#if SASL_VERSION_MAJOR < 2 && !defined(CSRIMALLOC)
	sasl_set_alloc(
		ber_memalloc,
		ber_memcalloc,
		ber_memrealloc,
		ber_memfree );
#endif /* CSRIMALLOC */

#ifdef LDAP_R_COMPILE
	sasl_set_mutex(
		ldap_pvt_sasl_mutex_new,
		ldap_pvt_sasl_mutex_lock,
		ldap_pvt_sasl_mutex_unlock,    
		ldap_pvt_sasl_mutex_dispose );    
#endif

	if ( sasl_client_init( NULL ) == SASL_OK ) {
		return 0;
	}

#if SASL_VERSION_MAJOR < 2
	/* A no-op to make sure we link with Cyrus 1.5 */
	sasl_client_auth( NULL, NULL, NULL, 0, NULL, NULL );
#endif
	return -1;
}

static void
sb_sasl_cyrus_init(
	struct sb_sasl_generic_data *p,
	ber_len_t *min_send,
	ber_len_t *max_send,
	ber_len_t *max_recv)
{
	sasl_conn_t *sasl_context = (sasl_conn_t *)p->ops_private;
	ber_len_t maxbuf;

	sasl_getprop( sasl_context, SASL_MAXOUTBUF,
		      (SASL_CONST void **)(char *) &maxbuf );

	*min_send = SASL_MIN_BUFF_SIZE;
	*max_send = maxbuf;
	*max_recv = SASL_MAX_BUFF_SIZE;
}

static ber_int_t
sb_sasl_cyrus_encode(
	struct sb_sasl_generic_data *p,
	unsigned char *buf,
	ber_len_t len,
	Sockbuf_Buf *dst)
{
	sasl_conn_t *sasl_context = (sasl_conn_t *)p->ops_private;
	ber_int_t ret;
	unsigned tmpsize = dst->buf_size;

	ret = sasl_encode( sasl_context, (char *)buf, len,
			   (SASL_CONST char **)&dst->buf_base,
			   &tmpsize );

	dst->buf_size = tmpsize;
	dst->buf_end = dst->buf_size;

	if ( ret != SASL_OK ) {
		ber_log_printf( LDAP_DEBUG_ANY, p->sbiod->sbiod_sb->sb_debug,
				"sb_sasl_cyrus_encode: failed to encode packet: %s\n",
				sasl_errstring( ret, NULL, NULL ) );
		return -1;
	}

	return 0;
}

static ber_int_t
sb_sasl_cyrus_decode(
	struct sb_sasl_generic_data *p,
	const Sockbuf_Buf *src,
	Sockbuf_Buf *dst)
{
	sasl_conn_t *sasl_context = (sasl_conn_t *)p->ops_private;
	ber_int_t ret;
	unsigned tmpsize = dst->buf_size;

	ret = sasl_decode( sasl_context,
			   src->buf_base, src->buf_end,
			   (SASL_CONST char **)&dst->buf_base,
			   (unsigned *)&tmpsize );


	dst->buf_size = tmpsize;
	dst->buf_end = dst->buf_size;

	if ( ret != SASL_OK ) {
		ber_log_printf( LDAP_DEBUG_ANY, p->sbiod->sbiod_sb->sb_debug,
				"sb_sasl_cyrus_decode: failed to decode packet: %s\n",
				sasl_errstring( ret, NULL, NULL ) );
		return -1;
	}

	return 0;
}

static void
sb_sasl_cyrus_reset_buf(
	struct sb_sasl_generic_data *p,
	Sockbuf_Buf *buf)
{
#if SASL_VERSION_MAJOR >= 2
	ber_pvt_sb_buf_init( buf );
#else
	ber_pvt_sb_buf_destroy( buf );
#endif
}

static void
sb_sasl_cyrus_fini(
	struct sb_sasl_generic_data *p)
{
#if SASL_VERSION_MAJOR >= 2
	/*
	 * SASLv2 encode/decode buffers are managed by
	 * libsasl2. Ensure they are not freed by liblber.
	 */
	p->buf_in.buf_base = NULL;
	p->buf_out.buf_base = NULL;
#endif
}

static const struct sb_sasl_generic_ops sb_sasl_cyrus_ops = {
	sb_sasl_cyrus_init,
	sb_sasl_cyrus_encode,
	sb_sasl_cyrus_decode,
	sb_sasl_cyrus_reset_buf,
	sb_sasl_cyrus_fini
 };

int ldap_pvt_sasl_install( Sockbuf *sb, void *ctx_arg )
{
	struct sb_sasl_generic_install install_arg;

	install_arg.ops		= &sb_sasl_cyrus_ops;
	install_arg.ops_private = ctx_arg;

	return ldap_pvt_sasl_generic_install( sb, &install_arg );
}

void ldap_pvt_sasl_remove( Sockbuf *sb )
{
	ldap_pvt_sasl_generic_remove( sb );
}

static int
sasl_err2ldap( int saslerr )
{
	int rc;

	/* map SASL errors to LDAP API errors returned by:
	 *	sasl_client_new()
	 *		SASL_OK, SASL_NOMECH, SASL_NOMEM
	 *	sasl_client_start()
	 *		SASL_OK, SASL_NOMECH, SASL_NOMEM, SASL_INTERACT
	 *	sasl_client_step()
	 *		SASL_OK, SASL_INTERACT, SASL_BADPROT, SASL_BADSERV
	 */

	switch (saslerr) {
		case SASL_CONTINUE:
			rc = LDAP_MORE_RESULTS_TO_RETURN;
			break;
		case SASL_INTERACT:
			rc = LDAP_LOCAL_ERROR;
			break;
		case SASL_OK:
			rc = LDAP_SUCCESS;
			break;
		case SASL_NOMEM:
			rc = LDAP_NO_MEMORY;
			break;
		case SASL_NOMECH:
			rc = LDAP_AUTH_UNKNOWN;
			break;
		case SASL_BADPROT:
			rc = LDAP_DECODING_ERROR;
			break;
		case SASL_BADSERV:
			rc = LDAP_AUTH_UNKNOWN;
			break;

		/* other codes */
		case SASL_BADAUTH:
			rc = LDAP_AUTH_UNKNOWN;
			break;
		case SASL_NOAUTHZ:
			rc = LDAP_PARAM_ERROR;
			break;
		case SASL_FAIL:
			rc = LDAP_LOCAL_ERROR;
			break;
		case SASL_TOOWEAK:
		case SASL_ENCRYPT:
			rc = LDAP_AUTH_UNKNOWN;
			break;
		default:
			rc = LDAP_LOCAL_ERROR;
			break;
	}

	assert( rc == LDAP_SUCCESS || LDAP_API_ERROR( rc ) );
	return rc;
}

int
ldap_int_sasl_open(
	LDAP *ld, 
	LDAPConn *lc,
	const char * host )
{
	int rc;
	sasl_conn_t *ctx;

	assert( lc->lconn_sasl_authctx == NULL );

	if ( host == NULL ) {
		ld->ld_errno = LDAP_LOCAL_ERROR;
		return ld->ld_errno;
	}

#if SASL_VERSION_MAJOR >= 2
	rc = sasl_client_new( "ldap", host, NULL, NULL,
		client_callbacks, 0, &ctx );
#else
	rc = sasl_client_new( "ldap", host, client_callbacks,
		SASL_SECURITY_LAYER, &ctx );
#endif

	if ( rc != SASL_OK ) {
		ld->ld_errno = sasl_err2ldap( rc );
		return ld->ld_errno;
	}

	Debug1( LDAP_DEBUG_TRACE, "ldap_int_sasl_open: host=%s\n",
		host );

	lc->lconn_sasl_authctx = ctx;

	return LDAP_SUCCESS;
}

int ldap_int_sasl_close( LDAP *ld, LDAPConn *lc )
{
	sasl_conn_t *ctx = lc->lconn_sasl_authctx;

	if( ctx != NULL ) {
		sasl_dispose( &ctx );
		if ( lc->lconn_sasl_sockctx &&
			lc->lconn_sasl_authctx != lc->lconn_sasl_sockctx ) {
			ctx = lc->lconn_sasl_sockctx;
			sasl_dispose( &ctx );
		}
		lc->lconn_sasl_sockctx = NULL;
		lc->lconn_sasl_authctx = NULL;
	}
	if( lc->lconn_sasl_cbind ) {
		ldap_memfree( lc->lconn_sasl_cbind );
		lc->lconn_sasl_cbind = NULL;
	}

	return LDAP_SUCCESS;
}

int ldap_pvt_sasl_cbinding_parse( const char *arg )
{
	int i = -1;

	if ( strcasecmp(arg, "none") == 0 )
		i = LDAP_OPT_X_SASL_CBINDING_NONE;
	else if ( strcasecmp(arg, "tls-unique") == 0 )
		i = LDAP_OPT_X_SASL_CBINDING_TLS_UNIQUE;
	else if ( strcasecmp(arg, "tls-endpoint") == 0 )
		i = LDAP_OPT_X_SASL_CBINDING_TLS_ENDPOINT;

	return i;
}

void *ldap_pvt_sasl_cbinding( void *ssl, int type, int is_server )
{
#if defined(SASL_CHANNEL_BINDING) && defined(HAVE_TLS)
	char unique_prefix[] = "tls-unique:";
	char endpoint_prefix[] = "tls-server-end-point:";
	char cbinding[ 64 ];
	struct berval cbv = { 64, cbinding };
	unsigned char *cb_data; /* used since cb->data is const* */
	sasl_channel_binding_t *cb;
	char *prefix;
	int plen;

	switch (type) {
	case LDAP_OPT_X_SASL_CBINDING_NONE:
		return NULL;
	case LDAP_OPT_X_SASL_CBINDING_TLS_UNIQUE:
		if ( !ldap_pvt_tls_get_unique( ssl, &cbv, is_server ))
			return NULL;
		prefix = unique_prefix;
		plen = sizeof(unique_prefix) -1;
		break;
	case LDAP_OPT_X_SASL_CBINDING_TLS_ENDPOINT:
		if ( !ldap_pvt_tls_get_endpoint( ssl, &cbv, is_server ))
			return NULL;
		prefix = endpoint_prefix;
		plen = sizeof(endpoint_prefix) -1;
		break;
	default:
		return NULL;
	}

	cb = ldap_memalloc( sizeof(*cb) + plen + cbv.bv_len );
	cb->len = plen + cbv.bv_len;
	cb->data = cb_data = (unsigned char *)(cb+1);
	memcpy( cb_data, prefix, plen );
	memcpy( cb_data + plen, cbv.bv_val, cbv.bv_len );
	cb->name = "ldap";
	cb->critical = 0;

	return cb;
#else
	return NULL;
#endif
}

int
ldap_int_sasl_bind(
	LDAP			*ld,
	const char		*dn,
	const char		*mechs,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	unsigned		flags,
	LDAP_SASL_INTERACT_PROC *interact,
	void			*defaults,
	LDAPMessage		*result,
	const char		**rmech,
	int				*msgid )
{
	const char		*mech;
	sasl_ssf_t		*ssf;
	sasl_conn_t		*ctx;
	sasl_interact_t *prompts = NULL;
	struct berval	ccred = BER_BVNULL;
	int saslrc, rc;
	unsigned credlen;
#if !defined(_WIN32)
	char my_hostname[HOST_NAME_MAX + 1];
#endif
	int free_saslhost = 0;

	Debug1( LDAP_DEBUG_TRACE, "ldap_int_sasl_bind: %s\n",
		mechs ? mechs : "<null>" );

	/* do a quick !LDAPv3 check... ldap_sasl_bind will do the rest. */
	if (ld->ld_version < LDAP_VERSION3) {
		ld->ld_errno = LDAP_NOT_SUPPORTED;
		return ld->ld_errno;
	}

	/* Starting a Bind */
	if ( !result ) {
		const char *pmech = NULL;
		sasl_conn_t	*oldctx;
		ber_socket_t		sd;
		void	*ssl;

		rc = 0;
		LDAP_MUTEX_LOCK( &ld->ld_conn_mutex );
		ber_sockbuf_ctrl( ld->ld_sb, LBER_SB_OPT_GET_FD, &sd );

		if ( sd == AC_SOCKET_INVALID || !ld->ld_defconn ) {
			/* not connected yet */

			rc = ldap_open_defconn( ld );

			if ( rc == 0 ) {
				ber_sockbuf_ctrl( ld->ld_defconn->lconn_sb,
					LBER_SB_OPT_GET_FD, &sd );

				if( sd == AC_SOCKET_INVALID ) {
					ld->ld_errno = LDAP_LOCAL_ERROR;
					rc = ld->ld_errno;
				}
			}
		}
		if ( rc == 0 && ld->ld_defconn &&
			ld->ld_defconn->lconn_status == LDAP_CONNST_CONNECTING ) {
			rc = ldap_int_check_async_open( ld, sd );
		}
		LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
		if( rc != 0 ) return ld->ld_errno;

		oldctx = ld->ld_defconn->lconn_sasl_authctx;

		/* If we already have an authentication context, clear it out */
		if( oldctx ) {
			if ( oldctx != ld->ld_defconn->lconn_sasl_sockctx ) {
				sasl_dispose( &oldctx );
			}
			ld->ld_defconn->lconn_sasl_authctx = NULL;
		}

		{
			char *saslhost;
			int nocanon = (int)LDAP_BOOL_GET( &ld->ld_options,
				LDAP_BOOL_SASL_NOCANON );

			/* If we don't need to canonicalize just use the host
			 * from the LDAP URI.
			 * Always use the result of gethostname() for LDAPI.
			 * Skip for Windows which doesn't support LDAPI.
			 */
#if !defined(_WIN32)
			if (ld->ld_defconn->lconn_server->lud_scheme != NULL &&
			    strcmp("ldapi", ld->ld_defconn->lconn_server->lud_scheme) == 0) {
				rc = gethostname(my_hostname, HOST_NAME_MAX + 1);
				if (rc == 0) {
					saslhost = my_hostname;
				} else {
					saslhost = "localhost";
				}
			} else
#endif
			if ( nocanon )
				saslhost = ld->ld_defconn->lconn_server->lud_host;
			else {
				saslhost = ldap_host_connected_to( ld->ld_defconn->lconn_sb,
				"localhost" );
				free_saslhost = 1;
			}
			rc = ldap_int_sasl_open( ld, ld->ld_defconn, saslhost );
			if ( free_saslhost )
				LDAP_FREE( saslhost );
		}

		if ( rc != LDAP_SUCCESS ) return rc;

		ctx = ld->ld_defconn->lconn_sasl_authctx;

#ifdef HAVE_TLS
		/* Check for TLS */
		ssl = ldap_pvt_tls_sb_ctx( ld->ld_defconn->lconn_sb );
		if ( ssl ) {
			struct berval authid = BER_BVNULL;
			ber_len_t fac;

			fac = ldap_pvt_tls_get_strength( ssl );
			/* failure is OK, we just can't use SASL EXTERNAL */
			(void) ldap_pvt_tls_get_my_dn( ssl, &authid, NULL, 0 );

			(void) ldap_int_sasl_external( ld, ld->ld_defconn, authid.bv_val, fac );
			LDAP_FREE( authid.bv_val );
#ifdef SASL_CHANNEL_BINDING	/* 2.1.25+ */
			if ( ld->ld_defconn->lconn_sasl_cbind == NULL ) {
				void *cb;
				cb = ldap_pvt_sasl_cbinding( ssl,
							     ld->ld_options.ldo_sasl_cbinding,
							     0 );
				if ( cb != NULL ) {
					sasl_setprop( ld->ld_defconn->lconn_sasl_authctx,
						SASL_CHANNEL_BINDING, cb );
					ld->ld_defconn->lconn_sasl_cbind = cb;
				}
			}
#endif
		}
#endif

#if !defined(_WIN32)
		/* Check for local */
		if ( ldap_pvt_url_scheme2proto(
			ld->ld_defconn->lconn_server->lud_scheme ) == LDAP_PROTO_IPC )
		{
			char authid[sizeof("gidNumber=4294967295+uidNumber=4294967295,"
				"cn=peercred,cn=external,cn=auth")];
			sprintf( authid, "gidNumber=%u+uidNumber=%u,"
				"cn=peercred,cn=external,cn=auth",
				getegid(), geteuid() );
			(void) ldap_int_sasl_external( ld, ld->ld_defconn, authid,
				LDAP_PVT_SASL_LOCAL_SSF );
		}
#endif

		/* (re)set security properties */
		sasl_setprop( ctx, SASL_SEC_PROPS,
			&ld->ld_options.ldo_sasl_secprops );

		mech = NULL;

		do {
			saslrc = sasl_client_start( ctx,
				mechs,
#if SASL_VERSION_MAJOR < 2
				NULL,
#endif
				&prompts,
				(SASL_CONST char **)&ccred.bv_val,
				&credlen,
				&mech );

			if( pmech == NULL && mech != NULL ) {
				pmech = mech;
				*rmech = mech;

				if( flags != LDAP_SASL_QUIET ) {
					fprintf(stderr,
						"SASL/%s authentication started\n",
						pmech );
				}
			}

			if( saslrc == SASL_INTERACT ) {
				int res;
				if( !interact ) break;
				res = (interact)( ld, flags, defaults, prompts );

				if( res != LDAP_SUCCESS ) break;
			}
		} while ( saslrc == SASL_INTERACT );
		rc = LDAP_SASL_BIND_IN_PROGRESS;

	} else {
		/* continuing an in-progress Bind */
		struct berval *scred = NULL;

		ctx = ld->ld_defconn->lconn_sasl_authctx;

		rc = ldap_parse_sasl_bind_result( ld, result, &scred, 0 );
		if ( rc != LDAP_SUCCESS ) {
			if ( scred )
				ber_bvfree( scred );
			goto done;
		}

		rc = ldap_result2error( ld, result, 0 );
		if ( rc != LDAP_SUCCESS && rc != LDAP_SASL_BIND_IN_PROGRESS ) {
			if( scred ) {
				/* and server provided us with data? */
				Debug2( LDAP_DEBUG_TRACE,
					"ldap_int_sasl_bind: rc=%d len=%ld\n",
					rc, scred ? (long) scred->bv_len : -1L );
				ber_bvfree( scred );
				scred = NULL;
			}
			goto done;
		}

		mech = *rmech;
		if ( rc == LDAP_SUCCESS && mech == NULL ) {
			if ( scred )
				ber_bvfree( scred );
			goto success;
		}

		do {
			if( ! scred ) {
				/* no data! */
				Debug0( LDAP_DEBUG_TRACE,
					"ldap_int_sasl_bind: no data in step!\n" );
			}

			saslrc = sasl_client_step( ctx,
				(scred == NULL) ? NULL : scred->bv_val,
				(scred == NULL) ? 0 : scred->bv_len,
				&prompts,
				(SASL_CONST char **)&ccred.bv_val,
				&credlen );

			Debug1( LDAP_DEBUG_TRACE, "sasl_client_step: %d\n",
				saslrc );

			if( saslrc == SASL_INTERACT ) {
				int res;
				if( !interact ) break;
				res = (interact)( ld, flags, defaults, prompts );
				if( res != LDAP_SUCCESS ) break;
			}
		} while ( saslrc == SASL_INTERACT );

		ber_bvfree( scred );
	}

	if ( (saslrc != SASL_OK) && (saslrc != SASL_CONTINUE) ) {
		rc = ld->ld_errno = sasl_err2ldap( saslrc );
#if SASL_VERSION_MAJOR >= 2
		if ( ld->ld_error ) {
			LDAP_FREE( ld->ld_error );
		}
		ld->ld_error = LDAP_STRDUP( sasl_errdetail( ctx ) );
#endif
		goto done;
	}

	if ( saslrc == SASL_OK )
		*rmech = NULL;

	ccred.bv_len = credlen;

	if ( rc == LDAP_SASL_BIND_IN_PROGRESS ) {
		rc = ldap_sasl_bind( ld, dn, mech, &ccred, sctrls, cctrls, msgid );

		if ( ccred.bv_val != NULL ) {
#if SASL_VERSION_MAJOR < 2
			LDAP_FREE( ccred.bv_val );
#endif
			ccred.bv_val = NULL;
		}
		if ( rc == LDAP_SUCCESS )
			rc = LDAP_SASL_BIND_IN_PROGRESS;
		goto done;
	}

success:
	/* Conversation was completed successfully by now */
	if( flags != LDAP_SASL_QUIET ) {
		char *data;
		saslrc = sasl_getprop( ctx, SASL_USERNAME,
			(SASL_CONST void **)(char *) &data );
		if( saslrc == SASL_OK && data && *data ) {
			fprintf( stderr, "SASL username: %s\n", data );
		}

#if SASL_VERSION_MAJOR < 2
		saslrc = sasl_getprop( ctx, SASL_REALM,
			(SASL_CONST void **) &data );
		if( saslrc == SASL_OK && data && *data ) {
			fprintf( stderr, "SASL realm: %s\n", data );
		}
#endif
	}

	ssf = NULL;
	saslrc = sasl_getprop( ctx, SASL_SSF, (SASL_CONST void **)(char *) &ssf );
	if( saslrc == SASL_OK ) {
		if( flags != LDAP_SASL_QUIET ) {
			fprintf( stderr, "SASL SSF: %lu\n",
				(unsigned long) *ssf );
		}

		if( ssf && *ssf ) {
			if ( ld->ld_defconn->lconn_sasl_sockctx ) {
				sasl_conn_t	*oldctx = ld->ld_defconn->lconn_sasl_sockctx;
				sasl_dispose( &oldctx );
				ldap_pvt_sasl_remove( ld->ld_defconn->lconn_sb );
			}
			ldap_pvt_sasl_install( ld->ld_defconn->lconn_sb, ctx );
			ld->ld_defconn->lconn_sasl_sockctx = ctx;

			if( flags != LDAP_SASL_QUIET ) {
				fprintf( stderr, "SASL data security layer installed.\n" );
			}
		}
	}
	ld->ld_defconn->lconn_sasl_authctx = ctx;

done:
	return rc;
}

int
ldap_int_sasl_external(
	LDAP *ld,
	LDAPConn *conn,
	const char * authid,
	ber_len_t ssf )
{
	int sc;
	sasl_conn_t *ctx;
#if SASL_VERSION_MAJOR < 2
	sasl_external_properties_t extprops;
#else
	sasl_ssf_t sasl_ssf = ssf;
#endif

	ctx = conn->lconn_sasl_authctx;

	if ( ctx == NULL ) {
		return LDAP_LOCAL_ERROR;
	}
   
#if SASL_VERSION_MAJOR >= 2
	sc = sasl_setprop( ctx, SASL_SSF_EXTERNAL, &sasl_ssf );
	if ( sc == SASL_OK )
		sc = sasl_setprop( ctx, SASL_AUTH_EXTERNAL, authid );
#else
	memset( &extprops, '\0', sizeof(extprops) );
	extprops.ssf = ssf;
	extprops.auth_id = (char *) authid;

	sc = sasl_setprop( ctx, SASL_SSF_EXTERNAL,
		(void *) &extprops );
#endif

	if ( sc != SASL_OK ) {
		return LDAP_LOCAL_ERROR;
	}

	return LDAP_SUCCESS;
}


#define GOT_MINSSF	1
#define	GOT_MAXSSF	2
#define	GOT_MAXBUF	4

static struct {
	struct berval key;
	int sflag;
	int ival;
	int idef;
} sprops[] = {
	{ BER_BVC("none"), 0, 0, 0 },
	{ BER_BVC("nodict"), SASL_SEC_NODICTIONARY, 0, 0 },
	{ BER_BVC("noplain"), SASL_SEC_NOPLAINTEXT, 0, 0 },
	{ BER_BVC("noactive"), SASL_SEC_NOACTIVE, 0, 0 },
	{ BER_BVC("passcred"), SASL_SEC_PASS_CREDENTIALS, 0, 0 },
	{ BER_BVC("forwardsec"), SASL_SEC_FORWARD_SECRECY, 0, 0 },
	{ BER_BVC("noanonymous"), SASL_SEC_NOANONYMOUS, 0, 0 },
	{ BER_BVC("minssf="), 0, GOT_MINSSF, 0 },
	{ BER_BVC("maxssf="), 0, GOT_MAXSSF, INT_MAX },
	{ BER_BVC("maxbufsize="), 0, GOT_MAXBUF, 65536 },
	{ BER_BVNULL, 0, 0, 0 }
};

void ldap_pvt_sasl_secprops_unparse(
	sasl_security_properties_t *secprops,
	struct berval *out )
{
	int i, l = 0;
	int comma;
	char *ptr;

	if ( secprops == NULL || out == NULL ) {
		return;
	}

	comma = 0;
	for ( i=0; !BER_BVISNULL( &sprops[i].key ); i++ ) {
		if ( sprops[i].ival ) {
			int v = 0;

			switch( sprops[i].ival ) {
			case GOT_MINSSF: v = secprops->min_ssf; break;
			case GOT_MAXSSF: v = secprops->max_ssf; break;
			case GOT_MAXBUF: v = secprops->maxbufsize; break;
			}
			/* It is the default, ignore it */
			if ( v == sprops[i].idef ) continue;

			l += sprops[i].key.bv_len + 24;
		} else if ( sprops[i].sflag ) {
			if ( sprops[i].sflag & secprops->security_flags ) {
				l += sprops[i].key.bv_len;
			}
		} else if ( secprops->security_flags == 0 ) {
			l += sprops[i].key.bv_len;
		}
		if ( comma ) l++;
		comma = 1;
	}
	l++;

	out->bv_val = LDAP_MALLOC( l );
	if ( out->bv_val == NULL ) {
		out->bv_len = 0;
		return;
	}

	ptr = out->bv_val;
	comma = 0;
	for ( i=0; !BER_BVISNULL( &sprops[i].key ); i++ ) {
		if ( sprops[i].ival ) {
			int v = 0;

			switch( sprops[i].ival ) {
			case GOT_MINSSF: v = secprops->min_ssf; break;
			case GOT_MAXSSF: v = secprops->max_ssf; break;
			case GOT_MAXBUF: v = secprops->maxbufsize; break;
			}
			/* It is the default, ignore it */
			if ( v == sprops[i].idef ) continue;

			if ( comma ) *ptr++ = ',';
			ptr += sprintf(ptr, "%s%d", sprops[i].key.bv_val, v );
			comma = 1;
		} else if ( sprops[i].sflag ) {
			if ( sprops[i].sflag & secprops->security_flags ) {
				if ( comma ) *ptr++ = ',';
				ptr += sprintf(ptr, "%s", sprops[i].key.bv_val );
				comma = 1;
			}
		} else if ( secprops->security_flags == 0 ) {
			if ( comma ) *ptr++ = ',';
			ptr += sprintf(ptr, "%s", sprops[i].key.bv_val );
			comma = 1;
		}
	}
	out->bv_len = ptr - out->bv_val;
}

int ldap_pvt_sasl_secprops(
	const char *in,
	sasl_security_properties_t *secprops )
{
	unsigned i, j, l;
	char **props;
	unsigned sflags = 0;
	int got_sflags = 0;
	sasl_ssf_t max_ssf = 0;
	int got_max_ssf = 0;
	sasl_ssf_t min_ssf = 0;
	int got_min_ssf = 0;
	unsigned maxbufsize = 0;
	int got_maxbufsize = 0;

	if( secprops == NULL ) {
		return LDAP_PARAM_ERROR;
	}
	props = ldap_str2charray( in, "," );
	if( props == NULL ) {
		return LDAP_PARAM_ERROR;
	}

	for( i=0; props[i]; i++ ) {
		l = strlen( props[i] );
		for ( j=0; !BER_BVISNULL( &sprops[j].key ); j++ ) {
			if ( l < sprops[j].key.bv_len ) continue;
			if ( strncasecmp( props[i], sprops[j].key.bv_val,
				sprops[j].key.bv_len )) continue;
			if ( sprops[j].ival ) {
				unsigned v;
				char *next = NULL;
				if ( !isdigit( (unsigned char)props[i][sprops[j].key.bv_len] ))
					continue;
				v = strtoul( &props[i][sprops[j].key.bv_len], &next, 10 );
				if ( next == &props[i][sprops[j].key.bv_len] || next[0] != '\0' ) continue;
				switch( sprops[j].ival ) {
				case GOT_MINSSF:
					min_ssf = v; got_min_ssf++; break;
				case GOT_MAXSSF:
					max_ssf = v; got_max_ssf++; break;
				case GOT_MAXBUF:
					maxbufsize = v; got_maxbufsize++; break;
				}
			} else {
				if ( props[i][sprops[j].key.bv_len] ) continue;
				if ( sprops[j].sflag )
					sflags |= sprops[j].sflag;
				else
					sflags = 0;
				got_sflags++;
			}
			break;
		}
		if ( BER_BVISNULL( &sprops[j].key )) {
			ldap_charray_free( props );
			return LDAP_NOT_SUPPORTED;
		}
	}

	if(got_sflags) {
		secprops->security_flags = sflags;
	}
	if(got_min_ssf) {
		secprops->min_ssf = min_ssf;
	}
	if(got_max_ssf) {
		secprops->max_ssf = max_ssf;
	}
	if(got_maxbufsize) {
		secprops->maxbufsize = maxbufsize;
	}

	ldap_charray_free( props );
	return LDAP_SUCCESS;
}

int
ldap_int_sasl_config( struct ldapoptions *lo, int option, const char *arg )
{
	int rc, i;

	switch( option ) {
	case LDAP_OPT_X_SASL_SECPROPS:
		rc = ldap_pvt_sasl_secprops( arg, &lo->ldo_sasl_secprops );
		if( rc == LDAP_SUCCESS ) return 0;
		break;
	case LDAP_OPT_X_SASL_CBINDING:
		i = ldap_pvt_sasl_cbinding_parse( arg );
		if ( i >= 0 ) {
			lo->ldo_sasl_cbinding = i;
			return 0;
		}
		break;
	}

	return -1;
}

int
ldap_int_sasl_get_option( LDAP *ld, int option, void *arg )
{
	if ( option == LDAP_OPT_X_SASL_MECHLIST ) {
		*(char ***)arg = (char **)sasl_global_listmech();
		return 0;
	}

	if ( ld == NULL )
		return -1;

	switch ( option ) {
		case LDAP_OPT_X_SASL_MECH: {
			*(char **)arg = ld->ld_options.ldo_def_sasl_mech
				? LDAP_STRDUP( ld->ld_options.ldo_def_sasl_mech ) : NULL;
		} break;
		case LDAP_OPT_X_SASL_REALM: {
			*(char **)arg = ld->ld_options.ldo_def_sasl_realm
				? LDAP_STRDUP( ld->ld_options.ldo_def_sasl_realm ) : NULL;
		} break;
		case LDAP_OPT_X_SASL_AUTHCID: {
			*(char **)arg = ld->ld_options.ldo_def_sasl_authcid
				? LDAP_STRDUP( ld->ld_options.ldo_def_sasl_authcid ) : NULL;
		} break;
		case LDAP_OPT_X_SASL_AUTHZID: {
			*(char **)arg = ld->ld_options.ldo_def_sasl_authzid
				? LDAP_STRDUP( ld->ld_options.ldo_def_sasl_authzid ) : NULL;
		} break;

		case LDAP_OPT_X_SASL_SSF: {
			int sc;
			sasl_ssf_t	*ssf;
			sasl_conn_t *ctx;

			if( ld->ld_defconn == NULL ) {
				return -1;
			}

			ctx = ld->ld_defconn->lconn_sasl_sockctx;

			if ( ctx == NULL ) {
				return -1;
			}

			sc = sasl_getprop( ctx, SASL_SSF,
				(SASL_CONST void **)(char *) &ssf );

			if ( sc != SASL_OK ) {
				return -1;
			}

			*(ber_len_t *)arg = *ssf;
		} break;

		case LDAP_OPT_X_SASL_SSF_EXTERNAL:
			/* this option is write only */
			return -1;

		case LDAP_OPT_X_SASL_SSF_MIN:
			*(ber_len_t *)arg = ld->ld_options.ldo_sasl_secprops.min_ssf;
			break;
		case LDAP_OPT_X_SASL_SSF_MAX:
			*(ber_len_t *)arg = ld->ld_options.ldo_sasl_secprops.max_ssf;
			break;
		case LDAP_OPT_X_SASL_MAXBUFSIZE:
			*(ber_len_t *)arg = ld->ld_options.ldo_sasl_secprops.maxbufsize;
			break;
		case LDAP_OPT_X_SASL_NOCANON:
			*(int *)arg = (int) LDAP_BOOL_GET(&ld->ld_options, LDAP_BOOL_SASL_NOCANON );
			break;

		case LDAP_OPT_X_SASL_USERNAME: {
			int sc;
			char *username;
			sasl_conn_t *ctx;

			if( ld->ld_defconn == NULL ) {
				return -1;
			}

			ctx = ld->ld_defconn->lconn_sasl_authctx;

			if ( ctx == NULL ) {
				return -1;
			}

			sc = sasl_getprop( ctx, SASL_USERNAME,
				(SASL_CONST void **)(char **) &username );

			if ( sc != SASL_OK ) {
				return -1;
			}

			*(char **)arg = username ? LDAP_STRDUP( username ) : NULL;
		} break;

		case LDAP_OPT_X_SASL_SECPROPS:
			/* this option is write only */
			return -1;

		case LDAP_OPT_X_SASL_CBINDING:
			*(int *)arg = ld->ld_options.ldo_sasl_cbinding;
			break;

#ifdef SASL_GSS_CREDS
		case LDAP_OPT_X_SASL_GSS_CREDS: {
			sasl_conn_t *ctx;
			int sc;

			if ( ld->ld_defconn == NULL )
				return -1;

			ctx = ld->ld_defconn->lconn_sasl_authctx;
			if ( ctx == NULL )
				return -1;

			sc = sasl_getprop( ctx, SASL_GSS_CREDS, arg );
			if ( sc != SASL_OK )
				return -1;
			}
			break;
#endif

		default:
			return -1;
	}
	return 0;
}

int
ldap_int_sasl_set_option( LDAP *ld, int option, void *arg )
{
	if ( ld == NULL )
		return -1;

	if ( arg == NULL && option != LDAP_OPT_X_SASL_NOCANON )
		return -1;

	switch ( option ) {
	case LDAP_OPT_X_SASL_SSF:
	case LDAP_OPT_X_SASL_USERNAME:
		/* This option is read-only */
		return -1;

	case LDAP_OPT_X_SASL_SSF_EXTERNAL: {
		int sc;
#if SASL_VERSION_MAJOR < 2
		sasl_external_properties_t extprops;
#else
		sasl_ssf_t sasl_ssf;
#endif
		sasl_conn_t *ctx;

		if( ld->ld_defconn == NULL ) {
			return -1;
		}

		ctx = ld->ld_defconn->lconn_sasl_authctx;

		if ( ctx == NULL ) {
			return -1;
		}

#if SASL_VERSION_MAJOR >= 2
		sasl_ssf = * (ber_len_t *)arg;
		sc = sasl_setprop( ctx, SASL_SSF_EXTERNAL, &sasl_ssf);
#else
		memset(&extprops, 0L, sizeof(extprops));

		extprops.ssf = * (ber_len_t *) arg;

		sc = sasl_setprop( ctx, SASL_SSF_EXTERNAL,
			(void *) &extprops );
#endif

		if ( sc != SASL_OK ) {
			return -1;
		}
		} break;

	case LDAP_OPT_X_SASL_SSF_MIN:
		ld->ld_options.ldo_sasl_secprops.min_ssf = *(ber_len_t *)arg;
		break;
	case LDAP_OPT_X_SASL_SSF_MAX:
		ld->ld_options.ldo_sasl_secprops.max_ssf = *(ber_len_t *)arg;
		break;
	case LDAP_OPT_X_SASL_MAXBUFSIZE:
		ld->ld_options.ldo_sasl_secprops.maxbufsize = *(ber_len_t *)arg;
		break;
	case LDAP_OPT_X_SASL_NOCANON:
		if ( arg == LDAP_OPT_OFF ) {
			LDAP_BOOL_CLR(&ld->ld_options, LDAP_BOOL_SASL_NOCANON );
		} else {
			LDAP_BOOL_SET(&ld->ld_options, LDAP_BOOL_SASL_NOCANON );
		}
		break;

	case LDAP_OPT_X_SASL_SECPROPS: {
		int sc;
		sc = ldap_pvt_sasl_secprops( (char *) arg,
			&ld->ld_options.ldo_sasl_secprops );

		return sc == LDAP_SUCCESS ? 0 : -1;
		}

	case LDAP_OPT_X_SASL_CBINDING:
		if ( !arg ) return -1;
		switch( *(int *) arg ) {
		case LDAP_OPT_X_SASL_CBINDING_NONE:
		case LDAP_OPT_X_SASL_CBINDING_TLS_UNIQUE:
		case LDAP_OPT_X_SASL_CBINDING_TLS_ENDPOINT:
			ld->ld_options.ldo_sasl_cbinding = *(int *) arg;
			return 0;
		}
		return -1;

#ifdef SASL_GSS_CREDS
	case LDAP_OPT_X_SASL_GSS_CREDS: {
		sasl_conn_t *ctx;
		int sc;

		if ( ld->ld_defconn == NULL )
			return -1;

		ctx = ld->ld_defconn->lconn_sasl_authctx;
		if ( ctx == NULL )
			return -1;

		sc = sasl_setprop( ctx, SASL_GSS_CREDS, arg );
		if ( sc != SASL_OK )
			return -1;
		}
		break;
#endif

	default:
		return -1;
	}
	return 0;
}

#ifdef LDAP_R_COMPILE
#define LDAP_DEBUG_R_SASL
void *ldap_pvt_sasl_mutex_new(void)
{
	ldap_pvt_thread_mutex_t *mutex;

	mutex = (ldap_pvt_thread_mutex_t *) LDAP_CALLOC( 1,
		sizeof(ldap_pvt_thread_mutex_t) );

	if ( ldap_pvt_thread_mutex_init( mutex ) == 0 ) {
		return mutex;
	}
	LDAP_FREE( mutex );
#ifndef LDAP_DEBUG_R_SASL
	assert( 0 );
#endif /* !LDAP_DEBUG_R_SASL */
	return NULL;
}

int ldap_pvt_sasl_mutex_lock(void *mutex)
{
#ifdef LDAP_DEBUG_R_SASL
	if ( mutex == NULL ) {
		return SASL_OK;
	}
#else /* !LDAP_DEBUG_R_SASL */
	assert( mutex != NULL );
#endif /* !LDAP_DEBUG_R_SASL */
	return ldap_pvt_thread_mutex_lock( (ldap_pvt_thread_mutex_t *)mutex )
		? SASL_FAIL : SASL_OK;
}

int ldap_pvt_sasl_mutex_unlock(void *mutex)
{
#ifdef LDAP_DEBUG_R_SASL
	if ( mutex == NULL ) {
		return SASL_OK;
	}
#else /* !LDAP_DEBUG_R_SASL */
	assert( mutex != NULL );
#endif /* !LDAP_DEBUG_R_SASL */
	return ldap_pvt_thread_mutex_unlock( (ldap_pvt_thread_mutex_t *)mutex )
		? SASL_FAIL : SASL_OK;
}

void ldap_pvt_sasl_mutex_dispose(void *mutex)
{
#ifdef LDAP_DEBUG_R_SASL
	if ( mutex == NULL ) {
		return;
	}
#else /* !LDAP_DEBUG_R_SASL */
	assert( mutex != NULL );
#endif /* !LDAP_DEBUG_R_SASL */
	(void) ldap_pvt_thread_mutex_destroy( (ldap_pvt_thread_mutex_t *)mutex );
	LDAP_FREE( mutex );
}
#endif

#else
int ldap_int_sasl_init( void )
{ return LDAP_SUCCESS; }

int ldap_int_sasl_close( LDAP *ld, LDAPConn *lc )
{ return LDAP_SUCCESS; }

int
ldap_int_sasl_bind(
	LDAP			*ld,
	const char		*dn,
	const char		*mechs,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	unsigned		flags,
	LDAP_SASL_INTERACT_PROC *interact,
	void			*defaults,
	LDAPMessage		*result,
	const char		**rmech,
	int				*msgid )
{ return LDAP_NOT_SUPPORTED; }

int
ldap_int_sasl_external(
	LDAP *ld,
	LDAPConn *conn,
	const char * authid,
	ber_len_t ssf )
{ return LDAP_SUCCESS; }

#endif /* HAVE_CYRUS_SASL */
