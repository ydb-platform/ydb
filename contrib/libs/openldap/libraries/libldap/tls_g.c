/* tls_g.c - Handle tls/ssl using GNUTLS. */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2008-2024 The OpenLDAP Foundation.
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
/* ACKNOWLEDGEMENTS: GNUTLS support written by Howard Chu and
 * Emily Backes; sponsored by The Written Word (thewrittenword.com)
 * and Stanford University (stanford.edu).
 */

#include "portable.h"

#ifdef HAVE_GNUTLS

#include "ldap_config.h"

#include <stdio.h>

#include <ac/stdlib.h>
#include <ac/errno.h>
#include <ac/socket.h>
#include <ac/string.h>
#include <ac/ctype.h>
#include <ac/time.h>
#include <ac/unistd.h>
#include <ac/param.h>
#include <ac/dirent.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "ldap-int.h"
#include "ldap-tls.h"

#error #include <gnutls/gnutls.h>
#error #include <gnutls/x509.h>
#error #include <gnutls/abstract.h>
#error #include <gnutls/crypto.h>

typedef struct tlsg_ctx {
	gnutls_certificate_credentials_t cred;
	gnutls_dh_params_t dh_params;
	unsigned long verify_depth;
	int refcount;
	int reqcert;
	gnutls_priority_t prios;
#ifdef LDAP_R_COMPILE
	ldap_pvt_thread_mutex_t ref_mutex;
#endif
} tlsg_ctx;

typedef struct tlsg_session {
	gnutls_session_t session;
	tlsg_ctx *ctx;
	struct berval peer_der_dn;
} tlsg_session;

static int tlsg_parse_ciphers( tlsg_ctx *ctx, char *suites );
static int tlsg_cert_verify( tlsg_session *s );

#ifdef LDAP_R_COMPILE

static void
tlsg_thr_init( void )
{
	/* do nothing */
}
#endif /* LDAP_R_COMPILE */

/*
 * Initialize TLS subsystem. Should be called only once.
 */
static int
tlsg_init( void )
{
	gnutls_global_init();
	return 0;
}

/*
 * Tear down the TLS subsystem. Should only be called once.
 */
static void
tlsg_destroy( void )
{
	gnutls_global_deinit();
}

static tls_ctx *
tlsg_ctx_new ( struct ldapoptions *lo )
{
	tlsg_ctx *ctx;

	ctx = ber_memcalloc ( 1, sizeof (*ctx) );
	if ( ctx ) {
		if ( gnutls_certificate_allocate_credentials( &ctx->cred )) {
			ber_memfree( ctx );
			return NULL;
		}
		ctx->refcount = 1;
		gnutls_priority_init( &ctx->prios, "NORMAL", NULL );
#ifdef LDAP_R_COMPILE
		ldap_pvt_thread_mutex_init( &ctx->ref_mutex );
#endif
	}
	return (tls_ctx *)ctx;
}

static void
tlsg_ctx_ref( tls_ctx *ctx )
{
	tlsg_ctx *c = (tlsg_ctx *)ctx;
	LDAP_MUTEX_LOCK( &c->ref_mutex );
	c->refcount++;
	LDAP_MUTEX_UNLOCK( &c->ref_mutex );
}

static void
tlsg_ctx_free ( tls_ctx *ctx )
{
	tlsg_ctx *c = (tlsg_ctx *)ctx;
	int refcount;

	if ( !c ) return;

	LDAP_MUTEX_LOCK( &c->ref_mutex );
	refcount = --c->refcount;
	LDAP_MUTEX_UNLOCK( &c->ref_mutex );
	if ( refcount )
		return;
	gnutls_priority_deinit( c->prios );
	gnutls_certificate_free_credentials( c->cred );
	if ( c->dh_params )
		gnutls_dh_params_deinit( c->dh_params );
	ber_memfree ( c );
}

static int
tlsg_getfile( const char *path, gnutls_datum_t *buf )
{
	int rc = -1, fd;
	struct stat st;
	char ebuf[128];

	fd = open( path, O_RDONLY );
	if ( fd < 0 ) {
		Debug2( LDAP_DEBUG_ANY,
			"TLS: opening `%s' failed: %s\n",
			path,
			AC_STRERROR_R( errno, ebuf, sizeof ebuf ));
		return -1;
	}
	if ( fstat( fd, &st ) == 0 ) {
		buf->size = st.st_size;
		buf->data = LDAP_MALLOC( st.st_size + 1 );
		if ( buf->data ) {
			rc = read( fd, buf->data, st.st_size );
			close( fd );
			if ( rc < st.st_size )
				rc = -1;
			else
				rc = 0;
		}
	}
	return rc;
}

/* This is the GnuTLS default */
#define	VERIFY_DEPTH	6

/*
 * initialize a new TLS context
 */
static int
tlsg_ctx_init( struct ldapoptions *lo, struct ldaptls *lt, int is_server, char *errmsg )
{
	tlsg_ctx *ctx = lo->ldo_tls_ctx;
	int rc;

 	if ( lo->ldo_tls_ciphersuite &&
		tlsg_parse_ciphers( ctx, lt->lt_ciphersuite )) {
		Debug1( LDAP_DEBUG_ANY,
 			   "TLS: could not set cipher list %s.\n",
			   lo->ldo_tls_ciphersuite );
		return -1;
 	}

	if (lo->ldo_tls_cacertdir != NULL) {
		char **dirs = ldap_str2charray( lt->lt_cacertdir, CERTPATHSEP );
		int i;
		for ( i=0; dirs[i]; i++ ) {
			rc = gnutls_certificate_set_x509_trust_dir(
				ctx->cred,
				dirs[i],
				GNUTLS_X509_FMT_PEM );
			if ( rc > 0 ) {
				Debug2( LDAP_DEBUG_TRACE,
					"TLS: loaded %d CA certificates from directory `%s'.\n",
					rc, dirs[i] );
			} else {
				Debug1( LDAP_DEBUG_ANY,
					"TLS: warning: no certificate found in CA certificate directory `%s'.\n",
					dirs[i] );
				/* only warn, no return */
				strncpy( errmsg, gnutls_strerror( rc ), ERRBUFSIZE );
			}
		}
		ldap_charray_free( dirs );
	}

	if (lo->ldo_tls_cacertfile != NULL) {
		rc = gnutls_certificate_set_x509_trust_file( 
			ctx->cred,
			lt->lt_cacertfile,
			GNUTLS_X509_FMT_PEM );
		if ( rc < 0 ) {
			Debug3( LDAP_DEBUG_ANY,
				"TLS: could not use CA certificate file `%s': %s (%d)\n",
				lo->ldo_tls_cacertfile,
				gnutls_strerror( rc ),
				rc );
			return -1;
		} else if ( rc == 0 ) {
			Debug1( LDAP_DEBUG_ANY,
				"TLS: warning: no certificate loaded from CA certificate file `%s'.\n",
				lo->ldo_tls_cacertfile );
			/* only warn, no return */
			strncpy( errmsg, gnutls_strerror( rc ), ERRBUFSIZE );
		}
	}

	if (lo->ldo_tls_cacert.bv_val != NULL ) {
		gnutls_datum_t buf;
		buf.data = (unsigned char *)lo->ldo_tls_cacert.bv_val;
		buf.size = lo->ldo_tls_cacert.bv_len;
		rc = gnutls_certificate_set_x509_trust_mem(
			ctx->cred,
			&buf,
			GNUTLS_X509_FMT_DER );
		if ( rc < 0 ) {
			Debug2( LDAP_DEBUG_ANY,
				"TLS: could not use CA certificate: %s (%d)\n",
				gnutls_strerror( rc ),
				rc );
			strncpy( errmsg, gnutls_strerror( rc ), ERRBUFSIZE );
			return -1;
		}
	}

	if (( lo->ldo_tls_certfile && lo->ldo_tls_keyfile ) ||
		( lo->ldo_tls_cert.bv_val && lo->ldo_tls_key.bv_val )) {
		gnutls_x509_privkey_t key;
		gnutls_datum_t buf;
		gnutls_x509_crt_t certs[VERIFY_DEPTH];
		unsigned int max = VERIFY_DEPTH;

		rc = gnutls_x509_privkey_init( &key );
		if ( rc ) {
			strncpy( errmsg, gnutls_strerror( rc ), ERRBUFSIZE );
			return -1;
		}

		/* OpenSSL builds the cert chain for us, but GnuTLS
		 * expects it to be present in the certfile. If it's
		 * not, we have to build it ourselves. So we have to
		 * do some special checks here...
		 */
		if ( lo->ldo_tls_key.bv_val ) {
			buf.data = (unsigned char *)lo->ldo_tls_key.bv_val;
			buf.size = lo->ldo_tls_key.bv_len;
			rc = gnutls_x509_privkey_import( key, &buf,
				GNUTLS_X509_FMT_DER );
		} else {
			rc = tlsg_getfile( lt->lt_keyfile, &buf );
			if ( rc ) {
				Debug1( LDAP_DEBUG_ANY,
					"TLS: could not use private key file `%s`.\n",
					lt->lt_keyfile);
				return -1;
			}
			rc = gnutls_x509_privkey_import( key, &buf,
				GNUTLS_X509_FMT_PEM );
			LDAP_FREE( buf.data );
		}
		if ( rc < 0 ) {
			Debug2( LDAP_DEBUG_ANY,
				"TLS: could not use private key: %s (%d)\n",
				gnutls_strerror( rc ),
				rc );
			strncpy( errmsg, gnutls_strerror( rc ), ERRBUFSIZE );
			return rc;
		}

		if ( lo->ldo_tls_cert.bv_val ) {
			buf.data = (unsigned char *)lo->ldo_tls_cert.bv_val;
			buf.size = lo->ldo_tls_cert.bv_len;
			rc = gnutls_x509_crt_list_import( certs, &max, &buf,
				GNUTLS_X509_FMT_DER, 0 );
		} else {
			rc = tlsg_getfile( lt->lt_certfile, &buf );
			if ( rc ) {
				Debug1( LDAP_DEBUG_ANY,
					"TLS: could not use certificate file `%s`.\n",
					lt->lt_certfile);
				return -1;
			}
			rc = gnutls_x509_crt_list_import( certs, &max, &buf,
				GNUTLS_X509_FMT_PEM, 0 );
			LDAP_FREE( buf.data );
		}
		if ( rc < 0 ) {
			Debug2( LDAP_DEBUG_ANY,
				"TLS: could not use certificate: %s (%d)\n",
				gnutls_strerror( rc ),
				rc );
			strncpy( errmsg, gnutls_strerror( rc ), ERRBUFSIZE );
			return rc;
		}

		/* If there's only one cert and it's not self-signed,
		 * then we have to build the cert chain.
		 */
		if ( max == 1 && !gnutls_x509_crt_check_issuer( certs[0], certs[0] )) {
			unsigned int i;
			for ( i = 1; i<VERIFY_DEPTH; i++ ) {
				if ( gnutls_certificate_get_issuer( ctx->cred, certs[i-1], &certs[i], 0 ))
					break;
				max++;
				/* If this CA is self-signed, we're done */
				if ( gnutls_x509_crt_check_issuer( certs[i], certs[i] ))
					break;
			}
		}
		rc = gnutls_certificate_set_x509_key( ctx->cred, certs, max, key );
		if ( rc ) {
			Debug2( LDAP_DEBUG_ANY,
				"TLS: could not use certificate with key: %s (%d)\n",
				gnutls_strerror( rc ),
				rc );
			strncpy( errmsg, gnutls_strerror( rc ), ERRBUFSIZE );
			return -1;
		}
	} else if (( lo->ldo_tls_certfile || lo->ldo_tls_keyfile )) {
		Debug0( LDAP_DEBUG_ANY,
		       "TLS: only one of certfile and keyfile specified\n" );
		return -1;
	} else if (( lo->ldo_tls_cert.bv_val || lo->ldo_tls_key.bv_val )) {
		Debug0( LDAP_DEBUG_ANY,
		       "TLS: only one of cert and key specified\n" );
		return -1;
	}

	if ( lo->ldo_tls_crlfile ) {
		rc = gnutls_certificate_set_x509_crl_file( 
			ctx->cred,
			lt->lt_crlfile,
			GNUTLS_X509_FMT_PEM );
		if ( rc < 0 ) {
			strncpy( errmsg, gnutls_strerror( rc ), ERRBUFSIZE );
			return -1;
		}
		rc = 0;
	}

	/* FIXME: ITS#5992 - this should be configurable,
	 * and V1 CA certs should be phased out ASAP.
	 */
	gnutls_certificate_set_verify_flags( ctx->cred,
		GNUTLS_VERIFY_ALLOW_X509_V1_CA_CRT );

	if ( is_server && lo->ldo_tls_dhfile ) {
		gnutls_datum_t buf;
		rc = tlsg_getfile( lo->ldo_tls_dhfile, &buf );
		if ( rc ) return -1;
		rc = gnutls_dh_params_init( &ctx->dh_params );
		if ( rc == 0 )
			rc = gnutls_dh_params_import_pkcs3( ctx->dh_params, &buf,
				GNUTLS_X509_FMT_PEM );
		LDAP_FREE( buf.data );
		if ( rc ) {
			strncpy( errmsg, gnutls_strerror( rc ), ERRBUFSIZE );
			return -1;
		}
		gnutls_certificate_set_dh_params( ctx->cred, ctx->dh_params );
	}

	ctx->reqcert = lo->ldo_tls_require_cert;

	return 0;
}

static tls_session *
tlsg_session_new ( tls_ctx * ctx, int is_server )
{
	tlsg_ctx *c = (tlsg_ctx *)ctx;
	tlsg_session *session;

	session = ber_memcalloc ( 1, sizeof (*session) );
	if ( !session )
		return NULL;

	session->ctx = c;
	gnutls_init( &session->session, is_server ? GNUTLS_SERVER : GNUTLS_CLIENT );
	gnutls_priority_set( session->session, c->prios );
	if ( c->cred )
		gnutls_credentials_set( session->session, GNUTLS_CRD_CERTIFICATE, c->cred );
	
	if ( is_server ) {
		int flag = 0;
		if ( c->reqcert ) {
			flag = GNUTLS_CERT_REQUEST;
			if ( c->reqcert == LDAP_OPT_X_TLS_DEMAND ||
				c->reqcert == LDAP_OPT_X_TLS_HARD )
				flag = GNUTLS_CERT_REQUIRE;
			gnutls_certificate_server_set_request( session->session, flag );
		}
	}
	return (tls_session *)session;
} 

static int
tlsg_session_accept( tls_session *session )
{
	tlsg_session *s = (tlsg_session *)session;
	int rc;

	rc = gnutls_handshake( s->session );
	if ( rc == 0 && s->ctx->reqcert != LDAP_OPT_X_TLS_NEVER ) {
		const gnutls_datum_t *peer_cert_list;
		unsigned int list_size;

		peer_cert_list = gnutls_certificate_get_peers( s->session, 
						&list_size );
		if ( !peer_cert_list && s->ctx->reqcert == LDAP_OPT_X_TLS_TRY )
			rc = 0;
		else {
			rc = tlsg_cert_verify( s );
			if ( rc && s->ctx->reqcert == LDAP_OPT_X_TLS_ALLOW )
				rc = 0;
		}
	}
	return rc;
}

static int
tlsg_session_connect( LDAP *ld, tls_session *session, const char *name_in )
{
	tlsg_session *s = (tlsg_session *)session;
	int rc;

	if ( name_in ) {
		rc = gnutls_server_name_set( s->session, GNUTLS_NAME_DNS, name_in, strlen(name_in) );
		if ( rc != GNUTLS_E_SUCCESS ) {
			return rc;
		}
	}

	return tlsg_session_accept( session);
}

static int
tlsg_session_upflags( Sockbuf *sb, tls_session *session, int rc )
{
	tlsg_session *s = (tlsg_session *)session;

	if ( rc != GNUTLS_E_INTERRUPTED && rc != GNUTLS_E_AGAIN )
		return 0;

	switch (gnutls_record_get_direction (s->session)) {
	case 0: 
		sb->sb_trans_needs_read = 1;
		return 1;
	case 1:
		sb->sb_trans_needs_write = 1;
		return 1;
	}
	return 0;
}

static char *
tlsg_session_errmsg( tls_session *sess, int rc, char *buf, size_t len )
{
	return (char *)gnutls_strerror( rc );
}

static void
tlsg_x509_cert_dn( struct berval *cert, struct berval *dn, int get_subject )
{
	BerElementBuffer berbuf;
	BerElement *ber = (BerElement *)&berbuf;
	ber_tag_t tag;
	ber_len_t len;
	ber_int_t i;

	ber_init2( ber, cert, LBER_USE_DER );
	tag = ber_skip_tag( ber, &len );	/* Sequence */
	tag = ber_skip_tag( ber, &len );	/* Sequence */
	tag = ber_peek_tag( ber, &len );	/* Context + Constructed (version) */
	if ( tag == 0xa0 ) {	/* Version is optional */
		tag = ber_skip_tag( ber, &len );
		tag = ber_get_int( ber, &i );	/* Int: Version */
	}
	tag = ber_skip_tag( ber, &len );	/* Int: Serial (can be longer than ber_int_t) */
	ber_skip_data( ber, len );
	tag = ber_skip_tag( ber, &len );	/* Sequence: Signature */
	ber_skip_data( ber, len );
	if ( !get_subject ) {
		tag = ber_peek_tag( ber, &len );	/* Sequence: Issuer DN */
	} else {
		tag = ber_skip_tag( ber, &len );
		ber_skip_data( ber, len );
		tag = ber_skip_tag( ber, &len );	/* Sequence: Validity */
		ber_skip_data( ber, len );
		tag = ber_peek_tag( ber, &len );	/* Sequence: Subject DN */
	}
	len = ber_ptrlen( ber );
	dn->bv_val = cert->bv_val + len;
	dn->bv_len = cert->bv_len - len;
}

static int
tlsg_session_my_dn( tls_session *session, struct berval *der_dn )
{
	tlsg_session *s = (tlsg_session *)session;
	const gnutls_datum_t *x;
	struct berval bv;

	x = gnutls_certificate_get_ours( s->session );

	if (!x) return LDAP_INVALID_CREDENTIALS;
	
	bv.bv_val = (char *) x->data;
	bv.bv_len = x->size;

	tlsg_x509_cert_dn( &bv, der_dn, 1 );
	return 0;
}

static int
tlsg_session_peer_dn( tls_session *session, struct berval *der_dn )
{
	tlsg_session *s = (tlsg_session *)session;
	if ( !s->peer_der_dn.bv_val ) {
		const gnutls_datum_t *peer_cert_list;
		unsigned int list_size;
		struct berval bv;

		peer_cert_list = gnutls_certificate_get_peers( s->session, 
							&list_size );
		if ( !peer_cert_list ) return LDAP_INVALID_CREDENTIALS;

		bv.bv_len = peer_cert_list->size;
		bv.bv_val = (char *) peer_cert_list->data;

		tlsg_x509_cert_dn( &bv, &s->peer_der_dn, 1 );
	}
	*der_dn = s->peer_der_dn;
	return 0;
}

/* what kind of hostname were we given? */
#define	IS_DNS	0
#define	IS_IP4	1
#define	IS_IP6	2

#define	CN_OID	"2.5.4.3"

static int
tlsg_session_chkhost( LDAP *ld, tls_session *session, const char *name_in )
{
	tlsg_session *s = (tlsg_session *)session;
	int i, ret;
	int chkSAN = ld->ld_options.ldo_tls_require_san, gotSAN = 0;
	const gnutls_datum_t *peer_cert_list;
	unsigned int list_size;
	char altname[NI_MAXHOST];
	size_t altnamesize;

	gnutls_x509_crt_t cert;
	const char *name;
	char *ptr;
	char *domain = NULL;
#ifdef LDAP_PF_INET6
	struct in6_addr addr;
#else
	struct in_addr addr;
#endif
	int len1 = 0, len2 = 0;
	int ntype = IS_DNS;

	if( ldap_int_hostname &&
		( !name_in || !strcasecmp( name_in, "localhost" ) ) )
	{
		name = ldap_int_hostname;
	} else {
		name = name_in;
	}

	peer_cert_list = gnutls_certificate_get_peers( s->session, 
						&list_size );
	if ( !peer_cert_list ) {
		Debug0( LDAP_DEBUG_ANY,
			"TLS: unable to get peer certificate.\n" );
		/* If this was a fatal condition, things would have
		 * aborted long before now.
		 */
		return LDAP_SUCCESS;
	}
	ret = gnutls_x509_crt_init( &cert );
	if ( ret < 0 )
		return LDAP_LOCAL_ERROR;
	ret = gnutls_x509_crt_import( cert, peer_cert_list, GNUTLS_X509_FMT_DER );
	if ( ret ) {
		gnutls_x509_crt_deinit( cert );
		return LDAP_LOCAL_ERROR;
	}

#ifdef LDAP_PF_INET6
	if (inet_pton(AF_INET6, name, &addr)) {
		ntype = IS_IP6;
	} else 
#endif
	if ((ptr = strrchr(name, '.')) && isdigit((unsigned char)ptr[1])) {
		if (inet_aton(name, (struct in_addr *)&addr)) ntype = IS_IP4;
	}
	
	if (ntype == IS_DNS) {
		len1 = strlen(name);
		domain = strchr(name, '.');
		if (domain) {
			len2 = len1 - (domain-name);
		}
	}

	if (chkSAN) {
	for ( i=0, ret=0; ret >= 0; i++ ) {
		altnamesize = sizeof(altname);
		ret = gnutls_x509_crt_get_subject_alt_name( cert, i, 
			altname, &altnamesize, NULL );
		if ( ret < 0 ) break;

		gotSAN = 1;
		/* ignore empty */
		if ( altnamesize == 0 ) continue;

		if ( ret == GNUTLS_SAN_DNSNAME ) {
			if (ntype != IS_DNS) continue;
	
			/* Is this an exact match? */
			if ((len1 == altnamesize) && !strncasecmp(name, altname, len1)) {
				break;
			}

			/* Is this a wildcard match? */
			if (domain && (altname[0] == '*') && (altname[1] == '.') &&
				(len2 == altnamesize-1) && !strncasecmp(domain, &altname[1], len2))
			{
				break;
			}
		} else if ( ret == GNUTLS_SAN_IPADDRESS ) {
			if (ntype == IS_DNS) continue;

#ifdef LDAP_PF_INET6
			if (ntype == IS_IP6 && altnamesize != sizeof(struct in6_addr)) {
				continue;
			} else
#endif
			if (ntype == IS_IP4 && altnamesize != sizeof(struct in_addr)) {
				continue;
			}
			if (!memcmp(altname, &addr, altnamesize)) {
				break;
			}
		}
	}
	if ( ret >= 0 ) {
		ret = LDAP_SUCCESS;
	}
	}
	if (ret != LDAP_SUCCESS && chkSAN) {
		switch(chkSAN) {
		case LDAP_OPT_X_TLS_DEMAND:
		case LDAP_OPT_X_TLS_HARD:
			if (!gotSAN) {
				Debug0( LDAP_DEBUG_ANY,
					"TLS: unable to get subjectAltName from peer certificate.\n" );
				ret = LDAP_CONNECT_ERROR;
				if ( ld->ld_error ) {
					LDAP_FREE( ld->ld_error );
				}
				ld->ld_error = LDAP_STRDUP(
					_("TLS: unable to get subjectAltName from peer certificate"));
				goto done;
			}
			/* FALLTHRU */
		case LDAP_OPT_X_TLS_TRY:
			if (gotSAN) {
				Debug1( LDAP_DEBUG_ANY, "TLS: hostname (%s) does not match "
					"subjectAltName in certificate.\n",
					name );
				ret = LDAP_CONNECT_ERROR;
				if ( ld->ld_error ) {
					LDAP_FREE( ld->ld_error );
				}
				ld->ld_error = LDAP_STRDUP(
					_("TLS: hostname does not match subjectAltName in peer certificate"));
				goto done;
			}
			break;
		case LDAP_OPT_X_TLS_ALLOW:
			break;
		}
	}

	if ( ret != LDAP_SUCCESS ){
		/* find the last CN */
		i=0;
		do {
			altnamesize = 0;
			ret = gnutls_x509_crt_get_dn_by_oid( cert, CN_OID,
				i, 1, altname, &altnamesize );
			if ( ret == GNUTLS_E_SHORT_MEMORY_BUFFER )
				i++;
			else
				break;
		} while ( 1 );

		if ( i ) {
			altnamesize = sizeof(altname);
			ret = gnutls_x509_crt_get_dn_by_oid( cert, CN_OID,
				i-1, 0, altname, &altnamesize );
		}

		if ( ret < 0 ) {
			Debug0( LDAP_DEBUG_ANY,
				"TLS: unable to get common name from peer certificate.\n" );
			ret = LDAP_CONNECT_ERROR;
			if ( ld->ld_error ) {
				LDAP_FREE( ld->ld_error );
			}
			ld->ld_error = LDAP_STRDUP(
				_("TLS: unable to get CN from peer certificate"));

		} else {
			ret = LDAP_LOCAL_ERROR;
			if ( !len1 ) len1 = strlen( name );
			if ( len1 == altnamesize && strncasecmp(name, altname, altnamesize) == 0 ) {
				ret = LDAP_SUCCESS;

			} else if (( altname[0] == '*' ) && ( altname[1] == '.' )) {
					/* Is this a wildcard match? */
				if( domain &&
					(len2 == altnamesize-1) && !strncasecmp(domain, &altname[1], len2)) {
					ret = LDAP_SUCCESS;
				}
			}
		}

		if( ret == LDAP_LOCAL_ERROR ) {
			altname[altnamesize] = '\0';
			Debug2( LDAP_DEBUG_ANY, "TLS: hostname (%s) does not match "
				"common name in certificate (%s).\n", 
				name, altname );
			ret = LDAP_CONNECT_ERROR;
			if ( ld->ld_error ) {
				LDAP_FREE( ld->ld_error );
			}
			ld->ld_error = LDAP_STRDUP(
				_("TLS: hostname does not match name in peer certificate"));
		}
	}
done:
	gnutls_x509_crt_deinit( cert );
	return ret;
}

static int
tlsg_session_strength( tls_session *session )
{
	tlsg_session *s = (tlsg_session *)session;
	gnutls_cipher_algorithm_t c;

	c = gnutls_cipher_get( s->session );
	return gnutls_cipher_get_key_size( c ) * 8;
}

static int
tlsg_session_unique( tls_session *sess, struct berval *buf, int is_server)
{
	tlsg_session *s = (tlsg_session *)sess;
	gnutls_datum_t cb;
	int rc;

	rc = gnutls_session_channel_binding( s->session, GNUTLS_CB_TLS_UNIQUE, &cb );
	if ( rc == 0 ) {
		int len = cb.size;
		if ( len > buf->bv_len )
			len = buf->bv_len;
		buf->bv_len = len;
		memcpy( buf->bv_val, cb.data, len );
		return len;
	}
	return 0;
}

static int
tlsg_session_endpoint( tls_session *sess, struct berval *buf, int is_server )
{
	tlsg_session *s = (tlsg_session *)sess;
	const gnutls_datum_t *cert_data;
	gnutls_x509_crt_t server_cert;
	gnutls_digest_algorithm_t md;
	int sign_algo, md_len, rc;

	if ( is_server )
		cert_data = gnutls_certificate_get_ours( s->session );
	else
		cert_data = gnutls_certificate_get_peers( s->session, NULL );

	if ( cert_data == NULL )
		return 0;

	rc = gnutls_x509_crt_init( &server_cert );
	if ( rc != GNUTLS_E_SUCCESS )
		return 0;

	rc = gnutls_x509_crt_import( server_cert, cert_data, GNUTLS_X509_FMT_DER );
	if ( rc != GNUTLS_E_SUCCESS ) {
		gnutls_x509_crt_deinit( server_cert );
		return 0;
	}

	sign_algo = gnutls_x509_crt_get_signature_algorithm( server_cert );
	gnutls_x509_crt_deinit( server_cert );
	if ( sign_algo <= GNUTLS_SIGN_UNKNOWN )
		return 0;

	md = gnutls_sign_get_hash_algorithm( sign_algo );
	if ( md == GNUTLS_DIG_UNKNOWN )
		return 0;

	/* See RFC 5929 */
	switch (md) {
	case GNUTLS_DIG_NULL:
	case GNUTLS_DIG_MD2:
	case GNUTLS_DIG_MD5:
	case GNUTLS_DIG_SHA1:
		md = GNUTLS_DIG_SHA256;
	}

	md_len = gnutls_hash_get_len( md );
	if ( md_len == 0 || md_len > buf->bv_len )
		return 0;

	rc = gnutls_hash_fast( md, cert_data->data, cert_data->size, buf->bv_val );
	if ( rc != GNUTLS_E_SUCCESS )
		return 0;

	buf->bv_len = md_len;

	return md_len;
}

static const char *
tlsg_session_version( tls_session *sess )
{
	tlsg_session *s = (tlsg_session *)sess;
	return gnutls_protocol_get_name(gnutls_protocol_get_version( s->session ));
}

static const char *
tlsg_session_cipher( tls_session *sess )
{
	tlsg_session *s = (tlsg_session *)sess;
	return gnutls_cipher_get_name(gnutls_cipher_get( s->session ));
}

static int
tlsg_session_peercert( tls_session *sess, struct berval *der )
{
	tlsg_session *s = (tlsg_session *)sess;
	const gnutls_datum_t *peer_cert_list;
	unsigned int list_size;

	peer_cert_list = gnutls_certificate_get_peers( s->session, &list_size );
	if (!peer_cert_list)
		return -1;
	der->bv_len = peer_cert_list[0].size;
	der->bv_val = LDAP_MALLOC( der->bv_len );
	if (!der->bv_val)
		return -1;
	memcpy(der->bv_val, peer_cert_list[0].data, der->bv_len);
	return 0;
}

static int
tlsg_session_pinning( LDAP *ld, tls_session *sess, char *hashalg, struct berval *hash )
{
	tlsg_session *s = (tlsg_session *)sess;
	const gnutls_datum_t *cert_list;
	unsigned int cert_list_size = 0;
	gnutls_x509_crt_t crt;
	gnutls_pubkey_t pubkey;
	gnutls_datum_t key = {};
	gnutls_digest_algorithm_t alg;
	struct berval keyhash;
	size_t len;
	int rc = -1;

	if ( hashalg ) {
		alg = gnutls_digest_get_id( hashalg );
		if ( alg == GNUTLS_DIG_UNKNOWN ) {
			Debug1( LDAP_DEBUG_ANY, "tlsg_session_pinning: "
					"unknown hashing algorithm for GnuTLS: '%s'\n",
					hashalg );
			return rc;
		}
	}

	cert_list = gnutls_certificate_get_peers( s->session, &cert_list_size );
	if ( cert_list_size == 0 ) {
		return rc;
	}

	if ( gnutls_x509_crt_init( &crt ) < 0 ) {
		return rc;
	}

	if ( gnutls_x509_crt_import( crt, &cert_list[0], GNUTLS_X509_FMT_DER ) ) {
		goto done;
	}

	if ( gnutls_pubkey_init( &pubkey ) ) {
		goto done;
	}

	if ( gnutls_pubkey_import_x509( pubkey, crt, 0 ) < 0 ) {
		goto done;
	}

	gnutls_pubkey_export( pubkey, GNUTLS_X509_FMT_DER, key.data, &len );
	if ( len <= 0 ) {
		goto done;
	}

	key.data = LDAP_MALLOC( len );
	if ( !key.data ) {
		goto done;
	}

	key.size = len;

	if ( gnutls_pubkey_export( pubkey, GNUTLS_X509_FMT_DER,
				key.data, &len ) < 0 ) {
		goto done;
	}

	if ( hashalg ) {
		keyhash.bv_len = gnutls_hash_get_len( alg );
		keyhash.bv_val = LDAP_MALLOC( keyhash.bv_len );
		if ( !keyhash.bv_val || gnutls_fingerprint( alg, &key,
					keyhash.bv_val, &keyhash.bv_len ) < 0 ) {
			goto done;
		}
	} else {
		keyhash.bv_val = (char *)key.data;
		keyhash.bv_len = key.size;
	}

	if ( ber_bvcmp( hash, &keyhash ) ) {
		rc = LDAP_CONNECT_ERROR;
		Debug0( LDAP_DEBUG_ANY, "tlsg_session_pinning: "
				"public key hash does not match provided pin.\n" );
		if ( ld->ld_error ) {
			LDAP_FREE( ld->ld_error );
		}
		ld->ld_error = LDAP_STRDUP(
			_("TLS: public key hash does not match provided pin"));
	} else {
		rc = LDAP_SUCCESS;
	}

done:
	if ( pubkey ) {
		gnutls_pubkey_deinit( pubkey );
	}
	if ( crt ) {
		gnutls_x509_crt_deinit( crt );
	}
	if ( keyhash.bv_val != (char *)key.data ) {
		LDAP_FREE( keyhash.bv_val );
	}
	if ( key.data ) {
		LDAP_FREE( key.data );
	}
	return rc;
}

/* suites is a string of colon-separated cipher suite names. */
static int
tlsg_parse_ciphers( tlsg_ctx *ctx, char *suites )
{
	const char *err;
	int rc = gnutls_priority_init( &ctx->prios, suites, &err );
	if ( rc )
		ctx->prios = NULL;
	return rc;
}

/*
 * TLS support for LBER Sockbufs
 */

struct tls_data {
	tlsg_session		*session;
	Sockbuf_IO_Desc		*sbiod;
};

static ssize_t
tlsg_recv( gnutls_transport_ptr_t ptr, void *buf, size_t len )
{
	struct tls_data		*p;

	if ( buf == NULL || len <= 0 ) return 0;

	p = (struct tls_data *)ptr;

	if ( p == NULL || p->sbiod == NULL ) {
		return 0;
	}

	return LBER_SBIOD_READ_NEXT( p->sbiod, buf, len );
}

static ssize_t
tlsg_send( gnutls_transport_ptr_t ptr, const void *buf, size_t len )
{
	struct tls_data		*p;
	
	if ( buf == NULL || len <= 0 ) return 0;
	
	p = (struct tls_data *)ptr;

	if ( p == NULL || p->sbiod == NULL ) {
		return 0;
	}

	return LBER_SBIOD_WRITE_NEXT( p->sbiod, (char *)buf, len );
}

static int
tlsg_sb_setup( Sockbuf_IO_Desc *sbiod, void *arg )
{
	struct tls_data		*p;
	tlsg_session	*session = arg;

	assert( sbiod != NULL );

	p = LBER_MALLOC( sizeof( *p ) );
	if ( p == NULL ) {
		return -1;
	}
	
	gnutls_transport_set_ptr( session->session, (gnutls_transport_ptr_t)p );
	gnutls_transport_set_pull_function( session->session, tlsg_recv );
	gnutls_transport_set_push_function( session->session, tlsg_send );
	p->session = session;
	p->sbiod = sbiod;
	sbiod->sbiod_pvt = p;
	return 0;
}

static int
tlsg_sb_remove( Sockbuf_IO_Desc *sbiod )
{
	struct tls_data		*p;
	
	assert( sbiod != NULL );
	assert( sbiod->sbiod_pvt != NULL );

	p = (struct tls_data *)sbiod->sbiod_pvt;
	gnutls_deinit ( p->session->session );
	LBER_FREE( p->session );
	LBER_FREE( sbiod->sbiod_pvt );
	sbiod->sbiod_pvt = NULL;
	return 0;
}

static int
tlsg_sb_close( Sockbuf_IO_Desc *sbiod )
{
	struct tls_data		*p;
	
	assert( sbiod != NULL );
	assert( sbiod->sbiod_pvt != NULL );

	p = (struct tls_data *)sbiod->sbiod_pvt;
	gnutls_bye ( p->session->session, GNUTLS_SHUT_WR );
	return 0;
}

static int
tlsg_sb_ctrl( Sockbuf_IO_Desc *sbiod, int opt, void *arg )
{
	struct tls_data		*p;
	
	assert( sbiod != NULL );
	assert( sbiod->sbiod_pvt != NULL );

	p = (struct tls_data *)sbiod->sbiod_pvt;
	
	if ( opt == LBER_SB_OPT_GET_SSL ) {
		*((tlsg_session **)arg) = p->session;
		return 1;
		
	} else if ( opt == LBER_SB_OPT_DATA_READY ) {
		if( gnutls_record_check_pending( p->session->session ) > 0 ) {
			return 1;
		}
	}
	
	return LBER_SBIOD_CTRL_NEXT( sbiod, opt, arg );
}

static ber_slen_t
tlsg_sb_read( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len)
{
	struct tls_data		*p;
	ber_slen_t		ret;

	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

	p = (struct tls_data *)sbiod->sbiod_pvt;

	ret = gnutls_record_recv ( p->session->session, buf, len );
	switch (ret) {
	case GNUTLS_E_INTERRUPTED:
	case GNUTLS_E_AGAIN:
		sbiod->sbiod_sb->sb_trans_needs_read = 1;
		sock_errset(EWOULDBLOCK);
		ret = 0;
		break;
	case GNUTLS_E_REHANDSHAKE:
		for ( ret = gnutls_handshake ( p->session->session );
		      ret == GNUTLS_E_INTERRUPTED || ret == GNUTLS_E_AGAIN;
		      ret = gnutls_handshake ( p->session->session ) );
		sbiod->sbiod_sb->sb_trans_needs_read = 1;
		ret = 0;
		break;
	default:
		sbiod->sbiod_sb->sb_trans_needs_read = 0;
	}
	return ret;
}

static ber_slen_t
tlsg_sb_write( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len)
{
	struct tls_data		*p;
	ber_slen_t		ret;

	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

	p = (struct tls_data *)sbiod->sbiod_pvt;

	ret = gnutls_record_send ( p->session->session, (char *)buf, len );

	if ( ret == GNUTLS_E_INTERRUPTED || ret == GNUTLS_E_AGAIN ) {
		sbiod->sbiod_sb->sb_trans_needs_write = 1;
		sock_errset(EWOULDBLOCK);
		ret = 0;
	} else {
		sbiod->sbiod_sb->sb_trans_needs_write = 0;
	}
	return ret;
}

static Sockbuf_IO tlsg_sbio =
{
	tlsg_sb_setup,		/* sbi_setup */
	tlsg_sb_remove,		/* sbi_remove */
	tlsg_sb_ctrl,		/* sbi_ctrl */
	tlsg_sb_read,		/* sbi_read */
	tlsg_sb_write,		/* sbi_write */
	tlsg_sb_close		/* sbi_close */
};

/* Certs are not automatically verified during the handshake */
static int
tlsg_cert_verify( tlsg_session *ssl )
{
	unsigned int status = 0;
	int err;
	time_t now = time(0);
	time_t peertime;

	err = gnutls_certificate_verify_peers2( ssl->session, &status );
	if ( err < 0 ) {
		Debug1( LDAP_DEBUG_ANY,"TLS: gnutls_certificate_verify_peers2 failed %d\n",
			err );
		return -1;
	}
	if ( status ) {
		Debug1( LDAP_DEBUG_TRACE,"TLS: peer cert untrusted or revoked (0x%x)\n",
			status );
		return -1;
	}
	peertime = gnutls_certificate_expiration_time_peers( ssl->session );
	if ( peertime == (time_t) -1 ) {
		Debug0( LDAP_DEBUG_ANY, "TLS: gnutls_certificate_expiration_time_peers failed\n" );
		return -1;
	}
	if ( peertime < now ) {
		Debug0( LDAP_DEBUG_ANY, "TLS: peer certificate is expired\n" );
		return -1;
	}
	peertime = gnutls_certificate_activation_time_peers( ssl->session );
	if ( peertime == (time_t) -1 ) {
		Debug0( LDAP_DEBUG_ANY, "TLS: gnutls_certificate_activation_time_peers failed\n" );
		return -1;
	}
	if ( peertime > now ) {
		Debug0( LDAP_DEBUG_ANY, "TLS: peer certificate not yet active\n" );
		return -1;
	}
	return 0;
}

tls_impl ldap_int_tls_impl = {
	"GnuTLS",

	tlsg_init,
	tlsg_destroy,

	tlsg_ctx_new,
	tlsg_ctx_ref,
	tlsg_ctx_free,
	tlsg_ctx_init,

	tlsg_session_new,
	tlsg_session_connect,
	tlsg_session_accept,
	tlsg_session_upflags,
	tlsg_session_errmsg,
	tlsg_session_my_dn,
	tlsg_session_peer_dn,
	tlsg_session_chkhost,
	tlsg_session_strength,
	tlsg_session_unique,
	tlsg_session_endpoint,
	tlsg_session_version,
	tlsg_session_cipher,
	tlsg_session_peercert,
	tlsg_session_pinning,

	&tlsg_sbio,

#ifdef LDAP_R_COMPILE
	tlsg_thr_init,
#else
	NULL,
#endif

	0
};

#endif /* HAVE_GNUTLS */
