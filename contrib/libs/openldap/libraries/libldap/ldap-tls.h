/*  ldap-tls.h - TLS defines & prototypes internal to the LDAP library */
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

#ifndef	_LDAP_TLS_H
#define	_LDAP_TLS_H 1

struct tls_impl;

struct tls_ctx;
struct tls_session;

#define CERTPATHSEP	";"

typedef struct tls_ctx tls_ctx;
typedef struct tls_session tls_session;

typedef int (TI_tls_init)(void);
typedef void (TI_tls_destroy)(void);

typedef tls_ctx *(TI_ctx_new)(struct ldapoptions *lo);
typedef void (TI_ctx_ref)(tls_ctx *ctx);
typedef void (TI_ctx_free)(tls_ctx *ctx);
#define ERRBUFSIZE	256
typedef int (TI_ctx_init)(struct ldapoptions *lo, struct ldaptls *lt, int is_server, char *errmsg);

typedef tls_session *(TI_session_new)(tls_ctx *ctx, int is_server);
typedef int (TI_session_connect)(LDAP *ld, tls_session *s, const char *name_in);
typedef int (TI_session_accept)(tls_session *s);
typedef int (TI_session_upflags)(Sockbuf *sb, tls_session *s, int rc);
typedef char *(TI_session_errmsg)(tls_session *s, int rc, char *buf, size_t len );
typedef int (TI_session_dn)(tls_session *sess, struct berval *dn);
typedef int (TI_session_chkhost)(LDAP *ld, tls_session *s, const char *name_in);
typedef int (TI_session_strength)(tls_session *sess);
typedef int (TI_session_unique)(tls_session *sess, struct berval *buf, int is_server);
typedef int (TI_session_endpoint)(tls_session *sess, struct berval *buf, int is_server);
typedef const char *(TI_session_name)(tls_session *s);
typedef int (TI_session_peercert)(tls_session *s, struct berval *der);
typedef int (TI_session_pinning)(LDAP *ld, tls_session *s, char *hashalg, struct berval *hash);

typedef void (TI_thr_init)(void);

typedef struct tls_impl {
	const char *ti_name;

	TI_tls_init *ti_tls_init;	/* library initialization */
	TI_tls_destroy *ti_tls_destroy;

	TI_ctx_new *ti_ctx_new;
	TI_ctx_ref *ti_ctx_ref;
	TI_ctx_free *ti_ctx_free;
	TI_ctx_init *ti_ctx_init;

	TI_session_new *ti_session_new;
	TI_session_connect *ti_session_connect;
	TI_session_accept *ti_session_accept;
	TI_session_upflags *ti_session_upflags;
	TI_session_errmsg *ti_session_errmsg;
	TI_session_dn *ti_session_my_dn;
	TI_session_dn *ti_session_peer_dn;
	TI_session_chkhost *ti_session_chkhost;
	TI_session_strength *ti_session_strength;
	TI_session_unique *ti_session_unique;
	TI_session_endpoint *ti_session_endpoint;
	TI_session_name *ti_session_version;
	TI_session_name *ti_session_cipher;
	TI_session_peercert *ti_session_peercert;
	TI_session_pinning *ti_session_pinning;

	Sockbuf_IO *ti_sbio;

	TI_thr_init *ti_thr_init;

	int ti_inited;
} tls_impl;

extern tls_impl ldap_int_tls_impl;

#endif /* _LDAP_TLS_H */
