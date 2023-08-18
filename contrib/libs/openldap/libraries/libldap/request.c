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
/* Portions Copyright (c) 1995 Regents of the University of Michigan.
 * All rights reserved.
 */
/* This notice applies to changes, created by or for Novell, Inc.,
 * to preexisting works for which notices appear elsewhere in this file.
 *
 * Copyright (C) 1999, 2000 Novell, Inc. All Rights Reserved.
 *
 * THIS WORK IS SUBJECT TO U.S. AND INTERNATIONAL COPYRIGHT LAWS AND TREATIES.
 * USE, MODIFICATION, AND REDISTRIBUTION OF THIS WORK IS SUBJECT TO VERSION
 * 2.0.1 OF THE OPENLDAP PUBLIC LICENSE, A COPY OF WHICH IS AVAILABLE AT
 * HTTP://WWW.OPENLDAP.ORG/LICENSE.HTML OR IN THE FILE "LICENSE" IN THE
 * TOP-LEVEL DIRECTORY OF THE DISTRIBUTION. ANY USE OR EXPLOITATION OF THIS
 * WORK OTHER THAN AS AUTHORIZED IN VERSION 2.0.1 OF THE OPENLDAP PUBLIC
 * LICENSE, OR OTHER PRIOR WRITTEN CONSENT FROM NOVELL, COULD SUBJECT THE
 * PERPETRATOR TO CRIMINAL AND CIVIL LIABILITY. 
 *---
 * Modification to OpenLDAP source by Novell, Inc.
 * April 2000 sfs  Added code to chase V3 referrals
 *  request.c - sending of ldap requests; handling of referrals
 *---
 * Note: A verbatim copy of version 2.0.1 of the OpenLDAP Public License 
 * can be found in the file "build/LICENSE-2.0.1" in this distribution
 * of OpenLDAP Software.
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/errno.h>
#include <ac/param.h>
#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>
#include <ac/unistd.h>

#include "ldap-int.h"
#include "lber.h"

/* used by ldap_send_server_request and ldap_new_connection */
#ifdef LDAP_R_COMPILE
#define LDAP_CONN_LOCK_IF(nolock) \
	{ if (nolock) LDAP_MUTEX_LOCK( &ld->ld_conn_mutex ); }
#define LDAP_CONN_UNLOCK_IF(nolock) \
	{ if (nolock) LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex ); }
#define LDAP_REQ_LOCK_IF(nolock) \
	{ if (nolock) LDAP_MUTEX_LOCK( &ld->ld_req_mutex ); }
#define LDAP_REQ_UNLOCK_IF(nolock) \
	{ if (nolock) LDAP_MUTEX_UNLOCK( &ld->ld_req_mutex ); }
#define LDAP_RES_LOCK_IF(nolock) \
	{ if (nolock) LDAP_MUTEX_LOCK( &ld->ld_res_mutex ); }
#define LDAP_RES_UNLOCK_IF(nolock) \
	{ if (nolock) LDAP_MUTEX_UNLOCK( &ld->ld_res_mutex ); }
#else
#define LDAP_CONN_LOCK_IF(nolock)
#define LDAP_CONN_UNLOCK_IF(nolock)
#define LDAP_REQ_LOCK_IF(nolock)
#define LDAP_REQ_UNLOCK_IF(nolock)
#define LDAP_RES_LOCK_IF(nolock)
#define LDAP_RES_UNLOCK_IF(nolock)
#endif

static LDAPConn *find_connection LDAP_P(( LDAP *ld, LDAPURLDesc *srv, int any ));
static void use_connection LDAP_P(( LDAP *ld, LDAPConn *lc ));
static void ldap_free_request_int LDAP_P(( LDAP *ld, LDAPRequest *lr ));

static BerElement *
re_encode_request( LDAP *ld,
	BerElement *origber,
	ber_int_t msgid,
	int sref,
	LDAPURLDesc *srv,
	int *type );

BerElement *
ldap_alloc_ber_with_options( LDAP *ld )
{
	BerElement	*ber;

	ber = ber_alloc_t( ld->ld_lberoptions );
	if ( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
	}

	return( ber );
}


void
ldap_set_ber_options( LDAP *ld, BerElement *ber )
{
	/* ld_lberoptions is constant, hence no lock */
	ber->ber_options = ld->ld_lberoptions;
}


/* sets needed mutexes - no mutexes set to this point */
ber_int_t
ldap_send_initial_request(
	LDAP *ld,
	ber_tag_t msgtype,
	const char *dn,
	BerElement *ber,
	ber_int_t msgid)
{
	int rc = 1;
	ber_socket_t sd = AC_SOCKET_INVALID;

	Debug0( LDAP_DEBUG_TRACE, "ldap_send_initial_request\n" );

	LDAP_MUTEX_LOCK( &ld->ld_conn_mutex );
	if ( ber_sockbuf_ctrl( ld->ld_sb, LBER_SB_OPT_GET_FD, &sd ) == -1 ) {
		/* not connected yet */
		rc = ldap_open_defconn( ld );
		if ( rc == 0 ) {
			ber_sockbuf_ctrl( ld->ld_defconn->lconn_sb,
				LBER_SB_OPT_GET_FD, &sd );
		}
	}
	if ( ld->ld_defconn && ld->ld_defconn->lconn_status == LDAP_CONNST_CONNECTING )
		rc = ldap_int_check_async_open( ld, sd );
	if( rc < 0 ) {
		ber_free( ber, 1 );
		LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
		return( -1 );
	} else if ( rc == 0 ) {
		Debug0( LDAP_DEBUG_TRACE,
			"ldap_open_defconn: successful\n" );
	}

#ifdef LDAP_CONNECTIONLESS
	if (LDAP_IS_UDP(ld)) {
		if (msgtype == LDAP_REQ_BIND) {
			LDAP_MUTEX_LOCK( &ld->ld_options.ldo_mutex );
			if (ld->ld_options.ldo_cldapdn)
				ldap_memfree(ld->ld_options.ldo_cldapdn);
			ld->ld_options.ldo_cldapdn = ldap_strdup(dn);
			ber_free( ber, 1 );
			LDAP_MUTEX_UNLOCK( &ld->ld_options.ldo_mutex );
			LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
			return 0;
		}
		if (msgtype != LDAP_REQ_ABANDON && msgtype != LDAP_REQ_SEARCH)
		{
			ber_free( ber, 1 );
			LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
			return LDAP_PARAM_ERROR;
		}
	}
#endif
	LDAP_MUTEX_LOCK( &ld->ld_req_mutex );
	rc = ldap_send_server_request( ld, ber, msgid, NULL,
		NULL, NULL, NULL, 0, 0 );
	LDAP_MUTEX_UNLOCK( &ld->ld_req_mutex );
	LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
	return(rc);
}


/* protected by conn_mutex */
int
ldap_int_flush_request(
	LDAP *ld,
	LDAPRequest *lr )
{
	LDAPConn *lc = lr->lr_conn;

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_conn_mutex );
	if ( ber_flush2( lc->lconn_sb, lr->lr_ber, LBER_FLUSH_FREE_NEVER ) != 0 ) {
		if (( sock_errno() == EAGAIN ) || ( sock_errno() == ENOTCONN )) {
			/* ENOTCONN is returned in Solaris 10 */
			/* need to continue write later */
			lr->lr_status = LDAP_REQST_WRITING;
			ldap_mark_select_write( ld, lc->lconn_sb );
			ld->ld_errno = LDAP_BUSY;
			return -2;
		} else {
			ld->ld_errno = LDAP_SERVER_DOWN;
			ldap_free_request( ld, lr );
			ldap_free_connection( ld, lc, 0, 0 );
			return( -1 );
		}
	} else {
		if ( lr->lr_parent == NULL ) {
			lr->lr_ber->ber_end = lr->lr_ber->ber_ptr;
			lr->lr_ber->ber_ptr = lr->lr_ber->ber_buf;
		}
		lr->lr_status = LDAP_REQST_INPROGRESS;

		/* sent -- waiting for a response */
		ldap_mark_select_read( ld, lc->lconn_sb );
		ldap_clear_select_write( ld, lc->lconn_sb );
	}
	return 0;
}

/*
 * protected by req_mutex
 * if m_noconn then protect using conn_lock
 * else already protected with conn_lock
 * if m_res then also protected by res_mutex
 */

int
ldap_send_server_request(
	LDAP *ld,
	BerElement *ber,
	ber_int_t msgid,
	LDAPRequest *parentreq,
	LDAPURLDesc **srvlist,
	LDAPConn *lc,
	LDAPreqinfo *bind,
	int m_noconn,
	int m_res )
{
	LDAPRequest	*lr;
	int		incparent, rc;

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_req_mutex );
	Debug0( LDAP_DEBUG_TRACE, "ldap_send_server_request\n" );

	incparent = 0;
	ld->ld_errno = LDAP_SUCCESS;	/* optimistic */

	LDAP_CONN_LOCK_IF(m_noconn);
	if ( lc == NULL ) {
		if ( srvlist == NULL ) {
			lc = ld->ld_defconn;
		} else {
			lc = find_connection( ld, *srvlist, 1 );
			if ( lc == NULL ) {
				if ( (bind != NULL) && (parentreq != NULL) ) {
					/* Remember the bind in the parent */
					incparent = 1;
					++parentreq->lr_outrefcnt;
				}
				lc = ldap_new_connection( ld, srvlist, 0,
					1, bind, 1, m_res );
			}
		}
	}

	/* async connect... */
	if ( lc != NULL && lc->lconn_status == LDAP_CONNST_CONNECTING ) {
		ber_socket_t	sd = AC_SOCKET_ERROR;
		struct timeval	tv = { 0 };

		ber_sockbuf_ctrl( lc->lconn_sb, LBER_SB_OPT_GET_FD, &sd );

		/* poll ... */
		switch ( ldap_int_poll( ld, sd, &tv, 1 ) ) {
		case 0:
			/* go on! */
			lc->lconn_status = LDAP_CONNST_CONNECTED;
			break;

		case -2:
			/* async only occurs if a network timeout is set */

			/* honor network timeout */
			LDAP_MUTEX_LOCK( &ld->ld_options.ldo_mutex );
			if ( time( NULL ) - lc->lconn_created <= ld->ld_options.ldo_tm_net.tv_sec )
			{
				/* caller will have to call again */
				ld->ld_errno = LDAP_X_CONNECTING;
			}
			LDAP_MUTEX_UNLOCK( &ld->ld_options.ldo_mutex );
			/* fallthru */

		default:
			/* error */
			break;
		}
	}

	if ( lc == NULL || lc->lconn_status != LDAP_CONNST_CONNECTED ) {
		if ( ld->ld_errno == LDAP_SUCCESS ) {
			ld->ld_errno = LDAP_SERVER_DOWN;
		}

		ber_free( ber, 1 );
		if ( incparent ) {
			/* Forget about the bind */
			--parentreq->lr_outrefcnt; 
		}
		LDAP_CONN_UNLOCK_IF(m_noconn);
		return( -1 );
	}

	use_connection( ld, lc );

#ifdef LDAP_CONNECTIONLESS
	if ( LDAP_IS_UDP( ld )) {
		BerElement tmpber = *ber;
		ber_rewind( &tmpber );
		LDAP_MUTEX_LOCK( &ld->ld_options.ldo_mutex );
		rc = ber_write( &tmpber, ld->ld_options.ldo_peer,
			sizeof( struct sockaddr_storage ), 0 );
		LDAP_MUTEX_UNLOCK( &ld->ld_options.ldo_mutex );
		if ( rc == -1 ) {
			ld->ld_errno = LDAP_ENCODING_ERROR;
			ber_free( ber, 1 );
			LDAP_CONN_UNLOCK_IF(m_noconn);
			return rc;
		}
	}
#endif

	/* If we still have an incomplete write, try to finish it before
	 * dealing with the new request. If we don't finish here, return
	 * LDAP_BUSY and let the caller retry later. We only allow a single
	 * request to be in WRITING state.
	 */
	rc = 0;
	if ( ld->ld_requests != NULL ) {
		TAvlnode *node = ldap_tavl_end( ld->ld_requests, TAVL_DIR_RIGHT );
		LDAPRequest *lr;

		assert( node != NULL );
		lr = node->avl_data;
		if ( lr->lr_status == LDAP_REQST_WRITING &&
				ldap_int_flush_request( ld, lr ) < 0 ) {
			rc = -1;
		}
	}
	if ( rc ) {
		ber_free( ber, 1 );
		LDAP_CONN_UNLOCK_IF(m_noconn);
		return rc;
	}

	lr = (LDAPRequest *)LDAP_CALLOC( 1, sizeof( LDAPRequest ) );
	if ( lr == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		ldap_free_connection( ld, lc, 0, 0 );
		ber_free( ber, 1 );
		if ( incparent ) {
			/* Forget about the bind */
			--parentreq->lr_outrefcnt; 
		}
		LDAP_CONN_UNLOCK_IF(m_noconn);
		return( -1 );
	} 
	lr->lr_msgid = msgid;
	lr->lr_status = LDAP_REQST_INPROGRESS;
	lr->lr_res_errno = LDAP_SUCCESS;	/* optimistic */
	lr->lr_ber = ber;
	lr->lr_conn = lc;
	if ( parentreq != NULL ) {	/* sub-request */
		if ( !incparent ) { 
			/* Increment if we didn't do it before the bind */
			++parentreq->lr_outrefcnt;
		}
		lr->lr_origid = parentreq->lr_origid;
		lr->lr_parentcnt = ++parentreq->lr_parentcnt;
		lr->lr_parent = parentreq;
		lr->lr_refnext = parentreq->lr_child;
		parentreq->lr_child = lr;
	} else {			/* original request */
		lr->lr_origid = lr->lr_msgid;
	}

	/* Extract requestDN for future reference */
#ifdef LDAP_CONNECTIONLESS
	if ( !LDAP_IS_UDP(ld) )
#endif
	{
		BerElement tmpber = *ber;
		ber_int_t	bint;
		ber_tag_t	tag, rtag;

		ber_reset( &tmpber, 1 );
		rtag = ber_scanf( &tmpber, "{it", /*}*/ &bint, &tag );
		switch ( tag ) {
		case LDAP_REQ_BIND:
			rtag = ber_scanf( &tmpber, "{i" /*}*/, &bint );
			break;
		case LDAP_REQ_DELETE:
			break;
		default:
			rtag = ber_scanf( &tmpber, "{" /*}*/ );
		case LDAP_REQ_ABANDON:
			break;
		}
		if ( tag != LDAP_REQ_ABANDON ) {
			ber_skip_tag( &tmpber, &lr->lr_dn.bv_len );
			lr->lr_dn.bv_val = tmpber.ber_ptr;
		}
	}

	rc = ldap_tavl_insert( &ld->ld_requests, lr, ldap_req_cmp, ldap_avl_dup_error );
	assert( rc == LDAP_SUCCESS );

	ld->ld_errno = LDAP_SUCCESS;
	if ( ldap_int_flush_request( ld, lr ) == -1 ) {
		msgid = -1;
	}

	LDAP_CONN_UNLOCK_IF(m_noconn);
	return( msgid );
}

/* return 0 if no StartTLS ext, 1 if present, 2 if critical */
static int
find_tls_ext( LDAPURLDesc *srv )
{
	int i, crit;
	char *ext;

	if ( !srv->lud_exts )
		return 0;

	for (i=0; srv->lud_exts[i]; i++) {
		crit = 0;
		ext = srv->lud_exts[i];
		if ( ext[0] == '!') {
			ext++;
			crit = 1;
		}
		if ( !strcasecmp( ext, "StartTLS" ) ||
			!strcasecmp( ext, "X-StartTLS" ) ||
			!strcmp( ext, LDAP_EXOP_START_TLS )) {
			return crit + 1;
		}
	}
	return 0;
}

/*
 * always protected by conn_mutex
 * optionally protected by req_mutex and res_mutex
 */
LDAPConn *
ldap_new_connection( LDAP *ld, LDAPURLDesc **srvlist, int use_ldsb,
	int connect, LDAPreqinfo *bind, int m_req, int m_res )
{
	LDAPConn	*lc;
	int		async = 0;

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_conn_mutex );
	Debug3( LDAP_DEBUG_TRACE, "ldap_new_connection %d %d %d\n",
		use_ldsb, connect, (bind != NULL) );
	/*
	 * make a new LDAP server connection
	 * XXX open connection synchronously for now
	 */
	lc = (LDAPConn *)LDAP_CALLOC( 1, sizeof( LDAPConn ) );
	if ( lc == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return( NULL );
	}
	
	if ( use_ldsb ) {
		assert( ld->ld_sb != NULL );
		lc->lconn_sb = ld->ld_sb;

	} else {
		lc->lconn_sb = ber_sockbuf_alloc();
		if ( lc->lconn_sb == NULL ) {
			LDAP_FREE( (char *)lc );
			ld->ld_errno = LDAP_NO_MEMORY;
			return( NULL );
		}
	}

	if ( connect ) {
		LDAPURLDesc	**srvp, *srv = NULL;

		async = LDAP_BOOL_GET( &ld->ld_options, LDAP_BOOL_CONNECT_ASYNC );

		for ( srvp = srvlist; *srvp != NULL; srvp = &(*srvp)->lud_next ) {
			int		rc;

			rc = ldap_int_open_connection( ld, lc, *srvp, async );
			if ( rc != -1 ) {
				srv = *srvp;

				/* If we fully connected, async is moot */
				if ( rc == 0 )
					async = 0;

				if ( ld->ld_urllist_proc && ( !async || rc != -2 ) ) {
					ld->ld_urllist_proc( ld, srvlist, srvp, ld->ld_urllist_params );
				}

				break;
			}
		}

		if ( srv == NULL ) {
			if ( !use_ldsb ) {
				ber_sockbuf_free( lc->lconn_sb );
			}
			LDAP_FREE( (char *)lc );
			ld->ld_errno = LDAP_SERVER_DOWN;
			return( NULL );
		}

		lc->lconn_server = ldap_url_dup( srv );
		if ( !lc->lconn_server ) {
			if ( !use_ldsb )
				ber_sockbuf_free( lc->lconn_sb );
			LDAP_FREE( (char *)lc );
			ld->ld_errno = LDAP_NO_MEMORY;
			return( NULL );
		}
	}

	lc->lconn_status = async ? LDAP_CONNST_CONNECTING : LDAP_CONNST_CONNECTED;
	lc->lconn_next = ld->ld_conns;
	ld->ld_conns = lc;

	if ( connect ) {
#ifdef HAVE_TLS
		if ( lc->lconn_server->lud_exts ) {
			int rc, ext = find_tls_ext( lc->lconn_server );
			if ( ext ) {
				LDAPConn	*savedefconn;

				savedefconn = ld->ld_defconn;
				++lc->lconn_refcnt;	/* avoid premature free */
				ld->ld_defconn = lc;

				LDAP_REQ_UNLOCK_IF(m_req);
				LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
				LDAP_RES_UNLOCK_IF(m_res);
				rc = ldap_start_tls_s( ld, NULL, NULL );
				LDAP_RES_LOCK_IF(m_res);
				LDAP_MUTEX_LOCK( &ld->ld_conn_mutex );
				LDAP_REQ_LOCK_IF(m_req);
				ld->ld_defconn = savedefconn;
				--lc->lconn_refcnt;

				if ( rc != LDAP_SUCCESS && ext == 2 ) {
					ldap_free_connection( ld, lc, 1, 0 );
					return NULL;
				}
			}
		}
#endif
	}

	if ( bind != NULL ) {
		int		err = 0;
		LDAPConn	*savedefconn;

		/* Set flag to prevent additional referrals
		 * from being processed on this
		 * connection until the bind has completed
		 */
		lc->lconn_rebind_inprogress = 1;
		/* V3 rebind function */
		if ( ld->ld_rebind_proc != NULL) {
			LDAPURLDesc	*srvfunc;

			srvfunc = ldap_url_dup( *srvlist );
			if ( srvfunc == NULL ) {
				ld->ld_errno = LDAP_NO_MEMORY;
				err = -1;
			} else {
				savedefconn = ld->ld_defconn;
				++lc->lconn_refcnt;	/* avoid premature free */
				ld->ld_defconn = lc;

				Debug0( LDAP_DEBUG_TRACE, "Call application rebind_proc\n" );
				LDAP_REQ_UNLOCK_IF(m_req);
				LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
				LDAP_RES_UNLOCK_IF(m_res);
				err = (*ld->ld_rebind_proc)( ld,
					bind->ri_url, bind->ri_request, bind->ri_msgid,
					ld->ld_rebind_params );
				LDAP_RES_LOCK_IF(m_res);
				LDAP_MUTEX_LOCK( &ld->ld_conn_mutex );
				LDAP_REQ_LOCK_IF(m_req);

				ld->ld_defconn = savedefconn;
				--lc->lconn_refcnt;

				if ( err != 0 ) {
					err = -1;
					ldap_free_connection( ld, lc, 1, 0 );
					lc = NULL;
				}
				ldap_free_urldesc( srvfunc );
			}

		} else {
			int		msgid, rc;
			struct berval	passwd = BER_BVNULL;

			savedefconn = ld->ld_defconn;
			++lc->lconn_refcnt;	/* avoid premature free */
			ld->ld_defconn = lc;

			Debug0( LDAP_DEBUG_TRACE,
				"anonymous rebind via ldap_sasl_bind(\"\")\n" );

			LDAP_REQ_UNLOCK_IF(m_req);
			LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
			LDAP_RES_UNLOCK_IF(m_res);
			rc = ldap_sasl_bind( ld, "", LDAP_SASL_SIMPLE, &passwd,
				NULL, NULL, &msgid );
			if ( rc != LDAP_SUCCESS ) {
				err = -1;

			} else {
				for ( err = 1; err > 0; ) {
					struct timeval	tv = { 0, 100000 };
					LDAPMessage	*res = NULL;

					switch ( ldap_result( ld, msgid, LDAP_MSG_ALL, &tv, &res ) ) {
					case -1:
						err = -1;
						break;

					case 0:
#ifdef LDAP_R_COMPILE
						ldap_pvt_thread_yield();
#endif
						break;

					case LDAP_RES_BIND:
						rc = ldap_parse_result( ld, res, &err, NULL, NULL, NULL, NULL, 1 );
						if ( rc != LDAP_SUCCESS ) {
							err = -1;

						} else if ( err != LDAP_SUCCESS ) {
							err = -1;
						}
						/* else err == LDAP_SUCCESS == 0 */
						break;

					default:
						Debug3( LDAP_DEBUG_TRACE,
							"ldap_new_connection %p: "
							"unexpected response %d "
							"from BIND request id=%d\n",
							(void *) ld, ldap_msgtype( res ), msgid );
						err = -1;
						break;
					}
				}
			}
			LDAP_RES_LOCK_IF(m_res);
			LDAP_MUTEX_LOCK( &ld->ld_conn_mutex );
			LDAP_REQ_LOCK_IF(m_req);
			ld->ld_defconn = savedefconn;
			--lc->lconn_refcnt;

			if ( err != 0 ) {
				ldap_free_connection( ld, lc, 1, 0 );
				lc = NULL;
			}
		}
		if ( lc != NULL )
			lc->lconn_rebind_inprogress = 0;
	}
	return( lc );
}


/* protected by ld_conn_mutex */
static LDAPConn *
find_connection( LDAP *ld, LDAPURLDesc *srv, int any )
/*
 * return an existing connection (if any) to the server srv
 * if "any" is non-zero, check for any server in the "srv" chain
 */
{
	LDAPConn	*lc;
	LDAPURLDesc	*lcu, *lsu;
	int lcu_port, lsu_port;
	int found = 0;

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_conn_mutex );
	for ( lc = ld->ld_conns; lc != NULL; lc = lc->lconn_next ) {
		lcu = lc->lconn_server;
		lcu_port = ldap_pvt_url_scheme_port( lcu->lud_scheme,
			lcu->lud_port );

		for ( lsu = srv; lsu != NULL; lsu = lsu->lud_next ) {
			lsu_port = ldap_pvt_url_scheme_port( lsu->lud_scheme,
				lsu->lud_port );

			if ( lsu_port == lcu_port
				&& strcmp( lcu->lud_scheme, lsu->lud_scheme ) == 0
				&& lcu->lud_host != NULL && lsu->lud_host != NULL
				&& strcasecmp( lsu->lud_host, lcu->lud_host ) == 0 )
			{
				found = 1;
				break;
			}

			if ( !any ) break;
		}
		if ( found )
			break;
	}
	return lc;
}



/* protected by ld_conn_mutex */
static void
use_connection( LDAP *ld, LDAPConn *lc )
{
	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_conn_mutex );
	++lc->lconn_refcnt;
	lc->lconn_lastused = time( NULL );
}


/* protected by ld_conn_mutex */
void
ldap_free_connection( LDAP *ld, LDAPConn *lc, int force, int unbind )
{
	LDAPConn	*tmplc, *prevlc;

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_conn_mutex );
	Debug2( LDAP_DEBUG_TRACE,
		"ldap_free_connection %d %d\n",
		force, unbind );

	if ( force || --lc->lconn_refcnt <= 0 ) {
		/* remove from connections list first */

		for ( prevlc = NULL, tmplc = ld->ld_conns;
			tmplc != NULL;
			tmplc = tmplc->lconn_next )
		{
			if ( tmplc == lc ) {
				if ( prevlc == NULL ) {
				    ld->ld_conns = tmplc->lconn_next;
				} else {
				    prevlc->lconn_next = tmplc->lconn_next;
				}
				if ( ld->ld_defconn == lc ) {
					ld->ld_defconn = NULL;
				}
				break;
			}
			prevlc = tmplc;
		}

		/* process connection callbacks */
		{
			struct ldapoptions *lo;
			ldaplist *ll;
			ldap_conncb *cb;

			lo = &ld->ld_options;
			LDAP_MUTEX_LOCK( &lo->ldo_mutex );
			if ( lo->ldo_conn_cbs ) {
				for ( ll=lo->ldo_conn_cbs; ll; ll=ll->ll_next ) {
					cb = ll->ll_data;
					cb->lc_del( ld, lc->lconn_sb, cb );
				}
			}
			LDAP_MUTEX_UNLOCK( &lo->ldo_mutex );
			lo = LDAP_INT_GLOBAL_OPT();
			LDAP_MUTEX_LOCK( &lo->ldo_mutex );
			if ( lo->ldo_conn_cbs ) {
				for ( ll=lo->ldo_conn_cbs; ll; ll=ll->ll_next ) {
					cb = ll->ll_data;
					cb->lc_del( ld, lc->lconn_sb, cb );
				}
			}
			LDAP_MUTEX_UNLOCK( &lo->ldo_mutex );
		}

		if ( lc->lconn_status == LDAP_CONNST_CONNECTED ) {
			ldap_mark_select_clear( ld, lc->lconn_sb );
			if ( unbind ) {
				ldap_send_unbind( ld, lc->lconn_sb,
						NULL, NULL );
			}
		}

		if ( lc->lconn_ber != NULL ) {
			ber_free( lc->lconn_ber, 1 );
		}

		ldap_int_sasl_close( ld, lc );

		ldap_free_urllist( lc->lconn_server );

		/* FIXME: is this at all possible?
		 * ldap_ld_free() in unbind.c calls ldap_free_connection()
		 * with force == 1 __after__ explicitly calling
		 * ldap_tavl_free on ld->ld_requests */
		if ( force ) {
			ldap_tavl_free( ld->ld_requests, ldap_do_free_request );
			ld->ld_requests = NULL;
		}

		if ( lc->lconn_sb != ld->ld_sb ) {
			ber_sockbuf_free( lc->lconn_sb );
		} else {
			ber_int_sb_close( lc->lconn_sb );
		}

		if ( lc->lconn_rebind_queue != NULL) {
			int i;
			for( i = 0; lc->lconn_rebind_queue[i] != NULL; i++ ) {
				LDAP_VFREE( lc->lconn_rebind_queue[i] );
			}
			LDAP_FREE( lc->lconn_rebind_queue );
		}

		LDAP_FREE( lc );

		Debug0( LDAP_DEBUG_TRACE,
			"ldap_free_connection: actually freed\n" );

	} else {
		lc->lconn_lastused = time( NULL );
		Debug1( LDAP_DEBUG_TRACE, "ldap_free_connection: refcnt %d\n",
				lc->lconn_refcnt );
	}
}


/* Protects self with ld_conn_mutex */
#ifdef LDAP_DEBUG
void
ldap_dump_connection( LDAP *ld, LDAPConn *lconns, int all )
{
	LDAPConn	*lc;
   	char		timebuf[32];

	Debug2( LDAP_DEBUG_TRACE, "** ld %p Connection%s:\n", (void *)ld, all ? "s" : "" );
	LDAP_MUTEX_LOCK( &ld->ld_conn_mutex );
	for ( lc = lconns; lc != NULL; lc = lc->lconn_next ) {
		if ( lc->lconn_server != NULL ) {
			Debug3( LDAP_DEBUG_TRACE, "* host: %s  port: %d%s\n",
				( lc->lconn_server->lud_host == NULL ) ? "(null)"
				: lc->lconn_server->lud_host,
				lc->lconn_server->lud_port, ( lc->lconn_sb ==
				ld->ld_sb ) ? "  (default)" : "" );
		}
		if ( lc->lconn_sb != NULL ) {
			char 			from[LDAP_IPADDRLEN];
			struct berval 	frombv = BER_BVC(from);
			ber_socket_t 	sb;
			if ( ber_sockbuf_ctrl( lc->lconn_sb, LBER_SB_OPT_GET_FD, &sb ) == 1 ) {
				Sockaddr sin;
				socklen_t len = sizeof( sin );
				if ( getsockname( sb, (struct sockaddr *)&sin, &len ) == 0 ) {
					ldap_pvt_sockaddrstr( &sin, &frombv );
					Debug1( LDAP_DEBUG_TRACE, "* from: %s\n",
						( from == NULL ) ? "(null)" : from );
				}
			}
		}
		Debug2( LDAP_DEBUG_TRACE, "  refcnt: %d  status: %s\n", lc->lconn_refcnt,
			( lc->lconn_status == LDAP_CONNST_NEEDSOCKET )
				? "NeedSocket" :
				( lc->lconn_status == LDAP_CONNST_CONNECTING )
					? "Connecting" : "Connected" );
		Debug2( LDAP_DEBUG_TRACE, "  last used: %s%s\n",
			ldap_pvt_ctime( &lc->lconn_lastused, timebuf ),
			lc->lconn_rebind_inprogress ? "  rebind in progress" : "" );
		if ( lc->lconn_rebind_inprogress ) {
			if ( lc->lconn_rebind_queue != NULL) {
				int	i;

				for ( i = 0; lc->lconn_rebind_queue[i] != NULL; i++ ) {
					int	j;
					for( j = 0; lc->lconn_rebind_queue[i][j] != 0; j++ ) {
						Debug3( LDAP_DEBUG_TRACE, "    queue %d entry %d - %s\n",
							i, j, lc->lconn_rebind_queue[i][j] );
					}
				}
			} else {
				Debug0( LDAP_DEBUG_TRACE, "    queue is empty\n" );
			}
		}
		Debug0( LDAP_DEBUG_TRACE, "\n" );
		if ( !all ) {
			break;
		}
	}
	LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
}


/* protected by req_mutex and res_mutex */
void
ldap_dump_requests_and_responses( LDAP *ld )
{
	LDAPMessage	*lm, *l;
	TAvlnode *node;
	int		i;

	Debug1( LDAP_DEBUG_TRACE, "** ld %p Outstanding Requests:\n",
		(void *)ld );
	node = ldap_tavl_end( ld->ld_requests, TAVL_DIR_LEFT );
	if ( node == NULL ) {
		Debug0( LDAP_DEBUG_TRACE, "   Empty\n" );
	}
	for ( i = 0 ; node != NULL; i++, node = ldap_tavl_next( node, TAVL_DIR_RIGHT ) ) {
		LDAPRequest	*lr = node->avl_data;

		Debug3( LDAP_DEBUG_TRACE, " * msgid %d,  origid %d, status %s\n",
			lr->lr_msgid, lr->lr_origid,
			( lr->lr_status == LDAP_REQST_INPROGRESS ) ? "InProgress" :
			( lr->lr_status == LDAP_REQST_CHASINGREFS ) ? "ChasingRefs" :
			( lr->lr_status == LDAP_REQST_NOTCONNECTED ) ? "NotConnected" :
			( lr->lr_status == LDAP_REQST_WRITING ) ? "Writing" :
			( lr->lr_status == LDAP_REQST_COMPLETED ) ? "RequestCompleted"
				: "InvalidStatus" );
		Debug2( LDAP_DEBUG_TRACE, "   outstanding referrals %d, parent count %d\n",
			lr->lr_outrefcnt, lr->lr_parentcnt );
	}
	Debug3( LDAP_DEBUG_TRACE, "  ld %p request count %d (abandoned %lu)\n",
		(void *)ld, i, ld->ld_nabandoned );
	Debug1( LDAP_DEBUG_TRACE, "** ld %p Response Queue:\n", (void *)ld );
	if ( ( lm = ld->ld_responses ) == NULL ) {
		Debug0( LDAP_DEBUG_TRACE, "   Empty\n" );
	}
	for ( i = 0; lm != NULL; lm = lm->lm_next, i++ ) {
		Debug2( LDAP_DEBUG_TRACE, " * msgid %d,  type %lu\n",
		    lm->lm_msgid, (unsigned long)lm->lm_msgtype );
		if ( lm->lm_chain != NULL ) {
			Debug0( LDAP_DEBUG_TRACE, "   chained responses:\n" );
			for ( l = lm->lm_chain; l != NULL; l = l->lm_chain ) {
				Debug2( LDAP_DEBUG_TRACE,
					"  * msgid %d,  type %lu\n",
					l->lm_msgid,
					(unsigned long)l->lm_msgtype );
			}
		}
	}
	Debug2( LDAP_DEBUG_TRACE, "  ld %p response count %d\n", (void *)ld, i );
}
#endif /* LDAP_DEBUG */

/* protected by req_mutex */
void
ldap_do_free_request( void *arg )
{
	LDAPRequest *lr = arg;

	Debug3( LDAP_DEBUG_TRACE, "ldap_do_free_request: "
			"asked to free lr %p msgid %d refcnt %d\n",
			(void *) lr, lr->lr_msgid, lr->lr_refcnt );
	/* if lr_refcnt > 0, the request has been looked up 
	 * by ldap_find_request_by_msgid(); if in the meanwhile
	 * the request is free()'d by someone else, just decrease
	 * the reference count; later on, it will be freed. */
	if ( lr->lr_refcnt > 0 ) {
		assert( lr->lr_refcnt == 1 );
		lr->lr_refcnt = -lr->lr_refcnt;
		return;
	}

	if ( lr->lr_ber != NULL ) {
		ber_free( lr->lr_ber, 1 );
		lr->lr_ber = NULL;
	}

	if ( lr->lr_res_error != NULL ) {
		LDAP_FREE( lr->lr_res_error );
		lr->lr_res_error = NULL;
	}

	if ( lr->lr_res_matched != NULL ) {
		LDAP_FREE( lr->lr_res_matched );
		lr->lr_res_matched = NULL;
	}

	LDAP_FREE( lr );
}

int
ldap_req_cmp( const void *l, const void *r )
{
	const LDAPRequest *left = l, *right = r;
	return left->lr_msgid - right->lr_msgid;
}

/* protected by req_mutex */
static void
ldap_free_request_int( LDAP *ld, LDAPRequest *lr )
{
	LDAPRequest *removed;

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_req_mutex );
	removed = ldap_tavl_delete( &ld->ld_requests, lr, ldap_req_cmp );
	assert( !removed || removed == lr );
	Debug3( LDAP_DEBUG_TRACE, "ldap_free_request_int: "
			"lr %p msgid %d%s removed\n",
			(void *) lr, lr->lr_msgid, removed ? "" : " not" );

	ldap_do_free_request( lr );
}

/* protected by req_mutex */
void
ldap_free_request( LDAP *ld, LDAPRequest *lr )
{
	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_req_mutex );
	Debug2( LDAP_DEBUG_TRACE, "ldap_free_request (origid %d, msgid %d)\n",
		lr->lr_origid, lr->lr_msgid );

	/* free all referrals (child requests) */
	while ( lr->lr_child ) {
		ldap_free_request( ld, lr->lr_child );
	}

	if ( lr->lr_parent != NULL ) {
		LDAPRequest     **lrp;

		--lr->lr_parent->lr_outrefcnt;
		for ( lrp = &lr->lr_parent->lr_child;
			*lrp && *lrp != lr;
			lrp = &(*lrp)->lr_refnext );

		if ( *lrp == lr ) {
			*lrp = lr->lr_refnext;
		}
	}
	ldap_free_request_int( ld, lr );
}

/*
 * call first time with *cntp = -1
 * when returns *cntp == -1, no referrals are left
 *
 * NOTE: may replace *refsp, or shuffle the contents
 * of the original array.
 */
static int ldap_int_nextref(
	LDAP			*ld,
	char			***refsp,
	int			*cntp,
	void			*params )
{
	assert( refsp != NULL );
	assert( *refsp != NULL );
	assert( cntp != NULL );

	if ( *cntp < -1 ) {
		*cntp = -1;
		return -1;
	}

	(*cntp)++;

	if ( (*refsp)[ *cntp ] == NULL ) {
		*cntp = -1;
	}

	return 0;
}

/*
 * Chase v3 referrals
 *
 * Parameters:
 *  (IN) ld = LDAP connection handle
 *  (IN) lr = LDAP Request structure
 *  (IN) refs = array of pointers to referral strings that we will chase
 *              The array will be free'd by this function when no longer needed
 *  (IN) sref != 0 if following search reference
 *  (OUT) errstrp = Place to return a string of referrals which could not be followed
 *  (OUT) hadrefp = 1 if successfully followed referral
 *
 * Return value - number of referrals followed
 *
 * Protected by res_mutex, conn_mutex and req_mutex	(try_read1msg)
 */
int
ldap_chase_v3referrals( LDAP *ld, LDAPRequest *lr, char **refs, int sref, char **errstrp, int *hadrefp )
{
	char		*unfollowed;
	int		 unfollowedcnt = 0;
	LDAPRequest	*origreq;
	LDAPURLDesc	*srv = NULL;
	BerElement	*ber;
	char		**refarray = NULL;
	LDAPConn	*lc;
	int			 rc, count, i, j, id;
	LDAPreqinfo  rinfo;
	LDAP_NEXTREF_PROC	*nextref_proc = ld->ld_nextref_proc ? ld->ld_nextref_proc : ldap_int_nextref;

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_res_mutex );
	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_conn_mutex );
	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_req_mutex );
	Debug0( LDAP_DEBUG_TRACE, "ldap_chase_v3referrals\n" );

	ld->ld_errno = LDAP_SUCCESS;	/* optimistic */
	*hadrefp = 0;

	unfollowed = NULL;
	rc = count = 0;

	/* If no referrals in array, return */
	if ( (refs == NULL) || ( (refs)[0] == NULL) ) {
		rc = 0;
		goto done;
	}

	/* Check for hop limit exceeded */
	if ( lr->lr_parentcnt >= ld->ld_refhoplimit ) {
		Debug1( LDAP_DEBUG_ANY,
		    "more than %d referral hops (dropping)\n", ld->ld_refhoplimit );
		ld->ld_errno = LDAP_REFERRAL_LIMIT_EXCEEDED;
		rc = -1;
		goto done;
	}

	/* find original request */
	for ( origreq = lr;
		origreq->lr_parent != NULL;
		origreq = origreq->lr_parent )
	{
		/* empty */ ;
	}

	refarray = refs;
	refs = NULL;

	/* parse out & follow referrals */
	/* NOTE: if nextref_proc == ldap_int_nextref, params is ignored */
	i = -1;
	for ( nextref_proc( ld, &refarray, &i, ld->ld_nextref_params );
			i != -1;
			nextref_proc( ld, &refarray, &i, ld->ld_nextref_params ) )
	{

		/* Parse the referral URL */
		rc = ldap_url_parse_ext( refarray[i], &srv, LDAP_PVT_URL_PARSE_NOEMPTY_DN );
		if ( rc != LDAP_URL_SUCCESS ) {
			/* ldap_url_parse_ext() returns LDAP_URL_* errors
			 * which do not map on API errors */
			ld->ld_errno = LDAP_PARAM_ERROR;
			rc = -1;
			goto done;
		}

		if( srv->lud_crit_exts ) {
			int ok = 0;
#ifdef HAVE_TLS
			/* If StartTLS is the only critical ext, OK. */
			if ( find_tls_ext( srv ) == 2 && srv->lud_crit_exts == 1 )
				ok = 1;
#endif
			if ( !ok ) {
				/* we do not support any other extensions */
				ld->ld_errno = LDAP_NOT_SUPPORTED;
				rc = -1;
				goto done;
			}
		}

		/* check connection for re-bind in progress */
		if (( lc = find_connection( ld, srv, 1 )) != NULL ) {
			/* See if we've already requested this DN with this conn */
			LDAPRequest *lp;
			int looped = 0;
			ber_len_t len = srv->lud_dn ? strlen( srv->lud_dn ) : 0;
			for ( lp = origreq; lp; ) {
				if ( lp->lr_conn == lc
					&& len == lp->lr_dn.bv_len
					&& len
					&& strncmp( srv->lud_dn, lp->lr_dn.bv_val, len ) == 0 )
				{
					looped = 1;
					break;
				}
				if ( lp == origreq ) {
					lp = lp->lr_child;
				} else {
					lp = lp->lr_refnext;
				}
			}
			if ( looped ) {
				ldap_free_urllist( srv );
				srv = NULL;
				ld->ld_errno = LDAP_CLIENT_LOOP;
				rc = -1;
				continue;
			}

			if ( lc->lconn_rebind_inprogress ) {
				/* We are already chasing a referral or search reference and a
				 * bind on that connection is in progress.  We must queue
				 * referrals on that connection, so we don't get a request
				 * going out before the bind operation completes. This happens
				 * if two search references come in one behind the other
				 * for the same server with different contexts.
				 */
				Debug1( LDAP_DEBUG_TRACE,
					"ldap_chase_v3referrals: queue referral \"%s\"\n",
					refarray[i] );
				if( lc->lconn_rebind_queue == NULL ) {
					/* Create a referral list */
					lc->lconn_rebind_queue =
						(char ***) LDAP_MALLOC( sizeof(void *) * 2);

					if( lc->lconn_rebind_queue == NULL) {
						ld->ld_errno = LDAP_NO_MEMORY;
						rc = -1;
						goto done;
					}

					lc->lconn_rebind_queue[0] = refarray;
					lc->lconn_rebind_queue[1] = NULL;
					refarray = NULL;

				} else {
					/* Count how many referral arrays we already have */
					for( j = 0; lc->lconn_rebind_queue[j] != NULL; j++) {
						/* empty */;
					}

					/* Add the new referral to the list */
					lc->lconn_rebind_queue = (char ***) LDAP_REALLOC(
						lc->lconn_rebind_queue, sizeof(void *) * (j + 2));

					if( lc->lconn_rebind_queue == NULL ) {
						ld->ld_errno = LDAP_NO_MEMORY;
						rc = -1;
						goto done;
					}
					lc->lconn_rebind_queue[j] = refarray;
					lc->lconn_rebind_queue[j+1] = NULL;
					refarray = NULL;
				}

				/* We have queued the referral/reference, now just return */
				rc = 0;
				*hadrefp = 1;
				count = 1; /* Pretend we already followed referral */
				goto done;
			}
		} 
		/* Re-encode the request with the new starting point of the search.
		 * Note: In the future we also need to replace the filter if one
		 * was provided with the search reference
		 */

		/* For references we don't want old dn if new dn empty */
		if ( sref && srv->lud_dn == NULL ) {
			srv->lud_dn = LDAP_STRDUP( "" );
		}

		LDAP_NEXT_MSGID( ld, id );
		ber = re_encode_request( ld, origreq->lr_ber, id,
			sref, srv, &rinfo.ri_request );

		if( ber == NULL ) {
			ld->ld_errno = LDAP_ENCODING_ERROR;
			rc = -1;
			goto done;
		}

		Debug2( LDAP_DEBUG_TRACE,
			"ldap_chase_v3referral: msgid %d, url \"%s\"\n",
			lr->lr_msgid, refarray[i] );

		/* Send the new request to the server - may require a bind */
		rinfo.ri_msgid = origreq->lr_origid;
		rinfo.ri_url = refarray[i];
		rc = ldap_send_server_request( ld, ber, id,
			origreq, &srv, NULL, &rinfo, 0, 1 );
		if ( rc < 0 ) {
			/* Failure, try next referral in the list */
			Debug3( LDAP_DEBUG_ANY, "Unable to chase referral \"%s\" (%d: %s)\n",
				refarray[i], ld->ld_errno, ldap_err2string( ld->ld_errno ) );
			unfollowedcnt += ldap_append_referral( ld, &unfollowed, refarray[i] );
			ldap_free_urllist( srv );
			srv = NULL;
			ld->ld_errno = LDAP_REFERRAL;
		} else {
			/* Success, no need to try this referral list further */
			rc = 0;
			++count;
			*hadrefp = 1;

			/* check if there is a queue of referrals that came in during bind */
			if ( lc == NULL) {
				lc = find_connection( ld, srv, 1 );
				if ( lc == NULL ) {
					ld->ld_errno = LDAP_OPERATIONS_ERROR;
					rc = -1;
					LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
					goto done;
				}
			}

			if ( lc->lconn_rebind_queue != NULL ) {
				/* Release resources of previous list */
				LDAP_VFREE( refarray );
				refarray = NULL;
				ldap_free_urllist( srv );
				srv = NULL;

				/* Pull entries off end of queue so list always null terminated */
				for( j = 0; lc->lconn_rebind_queue[j] != NULL; j++ )
					;
				refarray = lc->lconn_rebind_queue[j - 1];
				lc->lconn_rebind_queue[j-1] = NULL;
				/* we pulled off last entry from queue, free queue */
				if ( j == 1 ) {
					LDAP_FREE( lc->lconn_rebind_queue );
					lc->lconn_rebind_queue = NULL;
				}
				/* restart the loop the with new referral list */
				i = -1;
				continue;
			}
			break; /* referral followed, break out of for loop */
		}
	} /* end for loop */
done:
	LDAP_VFREE( refarray );
	ldap_free_urllist( srv );
	LDAP_FREE( *errstrp );
	
	if( rc == 0 ) {
		*errstrp = NULL;
		LDAP_FREE( unfollowed );
		return count;
	} else {
		*errstrp = unfollowed;
		return rc;
	}
}

/*
 * XXX merging of errors in this routine needs to be improved
 * Protected by res_mutex, conn_mutex and req_mutex	(try_read1msg)
 */
int
ldap_chase_referrals( LDAP *ld,
	LDAPRequest *lr,
	char **errstrp,
	int sref,
	int *hadrefp )
{
	int		rc, count, id;
	unsigned	len;
	char		*p, *ref, *unfollowed;
	LDAPRequest	*origreq;
	LDAPURLDesc	*srv;
	BerElement	*ber;
	LDAPreqinfo  rinfo;
	LDAPConn	*lc;

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_res_mutex );
	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_conn_mutex );
	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_req_mutex );
	Debug0( LDAP_DEBUG_TRACE, "ldap_chase_referrals\n" );

	ld->ld_errno = LDAP_SUCCESS;	/* optimistic */
	*hadrefp = 0;

	if ( *errstrp == NULL ) {
		return( 0 );
	}

	len = strlen( *errstrp );
	for ( p = *errstrp; len >= LDAP_REF_STR_LEN; ++p, --len ) {
		if ( strncasecmp( p, LDAP_REF_STR, LDAP_REF_STR_LEN ) == 0 ) {
			*p = '\0';
			p += LDAP_REF_STR_LEN;
			break;
		}
	}

	if ( len < LDAP_REF_STR_LEN ) {
		return( 0 );
	}

	if ( lr->lr_parentcnt >= ld->ld_refhoplimit ) {
		Debug1( LDAP_DEBUG_ANY,
		    "more than %d referral hops (dropping)\n",
		    ld->ld_refhoplimit );
		    /* XXX report as error in ld->ld_errno? */
		    return( 0 );
	}

	/* find original request */
	for ( origreq = lr; origreq->lr_parent != NULL;
	     origreq = origreq->lr_parent ) {
		/* empty */;
	}

	unfollowed = NULL;
	rc = count = 0;

	/* parse out & follow referrals */
	for ( ref = p; rc == 0 && ref != NULL; ref = p ) {
		p = strchr( ref, '\n' );
		if ( p != NULL ) {
			*p++ = '\0';
		}

		rc = ldap_url_parse_ext( ref, &srv, LDAP_PVT_URL_PARSE_NOEMPTY_DN );
		if ( rc != LDAP_URL_SUCCESS ) {
			Debug2( LDAP_DEBUG_TRACE,
				"ignoring %s referral <%s>\n",
				ref, rc == LDAP_URL_ERR_BADSCHEME ? "unknown" : "incorrect" );
			rc = ldap_append_referral( ld, &unfollowed, ref );
			*hadrefp = 1;
			continue;
		}

		Debug1( LDAP_DEBUG_TRACE,
		    "chasing LDAP referral: <%s>\n", ref );

		*hadrefp = 1;

		/* See if we've already been here */
		if (( lc = find_connection( ld, srv, 1 )) != NULL ) {
			LDAPRequest *lp;
			int looped = 0;
			ber_len_t len = srv->lud_dn ? strlen( srv->lud_dn ) : 0;
			for ( lp = lr; lp; lp = lp->lr_parent ) {
				if ( lp->lr_conn == lc
					&& len == lp->lr_dn.bv_len )
				{
					if ( len && strncmp( srv->lud_dn, lp->lr_dn.bv_val, len ) )
							continue;
					looped = 1;
					break;
				}
			}
			if ( looped ) {
				ldap_free_urllist( srv );
				ld->ld_errno = LDAP_CLIENT_LOOP;
				rc = -1;
				continue;
			}
		}

		LDAP_NEXT_MSGID( ld, id );
		ber = re_encode_request( ld, origreq->lr_ber,
		    id, sref, srv, &rinfo.ri_request );

		if ( ber == NULL ) {
			ldap_free_urllist( srv );
			return -1 ;
		}

		/* copy the complete referral for rebind process */
		rinfo.ri_url = LDAP_STRDUP( ref );

		rinfo.ri_msgid = origreq->lr_origid;

		rc = ldap_send_server_request( ld, ber, id,
			lr, &srv, NULL, &rinfo, 0, 1 );
		LDAP_FREE( rinfo.ri_url );

		if( rc >= 0 ) {
			++count;
		} else {
			Debug3( LDAP_DEBUG_ANY,
				"Unable to chase referral \"%s\" (%d: %s)\n", 
				ref, ld->ld_errno, ldap_err2string( ld->ld_errno ) );
			rc = ldap_append_referral( ld, &unfollowed, ref );
		}

		ldap_free_urllist(srv);
	}

	LDAP_FREE( *errstrp );
	*errstrp = unfollowed;

	return(( rc == 0 ) ? count : rc );
}


int
ldap_append_referral( LDAP *ld, char **referralsp, char *s )
{
	int	first;

	if ( *referralsp == NULL ) {
		first = 1;
		*referralsp = (char *)LDAP_MALLOC( strlen( s ) + LDAP_REF_STR_LEN
		    + 1 );
	} else {
		first = 0;
		*referralsp = (char *)LDAP_REALLOC( *referralsp,
		    strlen( *referralsp ) + strlen( s ) + 2 );
	}

	if ( *referralsp == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return( -1 );
	}

	if ( first ) {
		strcpy( *referralsp, LDAP_REF_STR );
	} else {
		strcat( *referralsp, "\n" );
	}
	strcat( *referralsp, s );

	return( 0 );
}



static BerElement *
re_encode_request( LDAP *ld,
	BerElement *origber,
	ber_int_t msgid,
	int sref,
	LDAPURLDesc *srv,
	int *type )
{
	/*
	 * XXX this routine knows way too much about how the lber library works!
	 */
	ber_int_t	along;
	ber_tag_t	tag;
	ber_tag_t	rtag;
	ber_int_t	ver;
	ber_int_t	scope;
	int		rc;
	BerElement	tmpber, *ber;
	struct berval		dn;

	Debug2( LDAP_DEBUG_TRACE,
	    "re_encode_request: new msgid %ld, new dn <%s>\n",
	    (long) msgid,
		( srv == NULL || srv->lud_dn == NULL) ? "NONE" : srv->lud_dn );

	tmpber = *origber;

	/*
	 * all LDAP requests are sequences that start with a message id.
	 * For all except delete, this is followed by a sequence that is
	 * tagged with the operation code.  For delete, the provided DN
	 * is not wrapped by a sequence.
	 */
	rtag = ber_scanf( &tmpber, "{it", /*}*/ &along, &tag );

	if ( rtag == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		return( NULL );
	}

	assert( tag != 0);
	if ( tag == LDAP_REQ_BIND ) {
		/* bind requests have a version number before the DN & other stuff */
		rtag = ber_scanf( &tmpber, "{im" /*}*/, &ver, &dn );

	} else if ( tag == LDAP_REQ_DELETE ) {
		/* delete requests don't have a DN wrapping sequence */
		rtag = ber_scanf( &tmpber, "m", &dn );

	} else if ( tag == LDAP_REQ_SEARCH ) {
		/* search requests need to be re-scope-ed */
		rtag = ber_scanf( &tmpber, "{me" /*"}"*/, &dn, &scope );

		if( srv->lud_scope != LDAP_SCOPE_DEFAULT ) {
			/* use the scope provided in reference */
			scope = srv->lud_scope;

		} else if ( sref ) {
			/* use scope implied by previous operation
			 *   base -> base
			 *   one -> base
			 *   subtree -> subtree
			 *   subordinate -> subtree
			 */
			switch( scope ) {
			default:
			case LDAP_SCOPE_BASE:
			case LDAP_SCOPE_ONELEVEL:
				scope = LDAP_SCOPE_BASE;
				break;
			case LDAP_SCOPE_SUBTREE:
			case LDAP_SCOPE_SUBORDINATE:
				scope = LDAP_SCOPE_SUBTREE;
				break;
			}
		}

	} else {
		rtag = ber_scanf( &tmpber, "{m" /*}*/, &dn );
	}

	if( rtag == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		return NULL;
	}

	/* restore character zero'd out by ber_scanf*/
	dn.bv_val[dn.bv_len] = tmpber.ber_tag;

	if (( ber = ldap_alloc_ber_with_options( ld )) == NULL ) {
		return NULL;
	}

	if ( srv->lud_dn ) {
		ber_str2bv( srv->lud_dn, 0, 0, &dn );
	}

	if ( tag == LDAP_REQ_BIND ) {
		rc = ber_printf( ber, "{it{iO" /*}}*/, msgid, tag, ver, &dn );
	} else if ( tag == LDAP_REQ_DELETE ) {
		rc = ber_printf( ber, "{itON}", msgid, tag, &dn );
	} else if ( tag == LDAP_REQ_SEARCH ) {
		rc = ber_printf( ber, "{it{Oe" /*}}*/, msgid, tag, &dn, scope );
	} else {
		rc = ber_printf( ber, "{it{O" /*}}*/, msgid, tag, &dn );
	}

	if ( rc == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return NULL;
	}

	if ( tag != LDAP_REQ_DELETE && (
		ber_write(ber, tmpber.ber_ptr, ( tmpber.ber_end - tmpber.ber_ptr ), 0)
		!= ( tmpber.ber_end - tmpber.ber_ptr ) ||
	    ber_printf( ber, /*{{*/ "N}N}" ) == -1 ) )
	{
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return NULL;
	}

#ifdef LDAP_DEBUG
	if ( ldap_debug & LDAP_DEBUG_PACKETS ) {
		Debug0( LDAP_DEBUG_ANY, "re_encode_request new request is:\n" );
		ber_log_dump( LDAP_DEBUG_BER, ldap_debug, ber, 0 );
	}
#endif /* LDAP_DEBUG */

	*type = tag;	/* return request type */
	return ber;
}


/* protected by req_mutex */
LDAPRequest *
ldap_find_request_by_msgid( LDAP *ld, ber_int_t msgid )
{
	LDAPRequest	*lr, needle = {0};
	needle.lr_msgid = msgid;

	lr = ldap_tavl_find( ld->ld_requests, &needle, ldap_req_cmp );
	if ( lr != NULL && lr->lr_status != LDAP_REQST_COMPLETED ) {
		/* lr_refcnt is only negative when we removed it from ld_requests
		 * already, it is positive if we have sub-requests (referrals) */
		assert( lr->lr_refcnt >= 0 );
		lr->lr_refcnt++;
		Debug3( LDAP_DEBUG_TRACE, "ldap_find_request_by_msgid: "
				"msgid %d, lr %p lr->lr_refcnt = %d\n",
				msgid, (void *) lr, lr->lr_refcnt );
		return lr;
	}

	Debug2( LDAP_DEBUG_TRACE, "ldap_find_request_by_msgid: "
			"msgid %d, lr %p\n", msgid, (void *) lr );
	return NULL;
}

/* protected by req_mutex */
void
ldap_return_request( LDAP *ld, LDAPRequest *lrx, int freeit )
{
	LDAPRequest	*lr;

	lr = ldap_tavl_find( ld->ld_requests, lrx, ldap_req_cmp );
	Debug2( LDAP_DEBUG_TRACE, "ldap_return_request: "
			"lrx %p, lr %p\n", (void *) lrx, (void *) lr );
	if ( lr ) {
		assert( lr == lrx );
		if ( lr->lr_refcnt > 0 ) {
			lr->lr_refcnt--;
		} else if ( lr->lr_refcnt < 0 ) {
			lr->lr_refcnt++;
			if ( lr->lr_refcnt == 0 ) {
				lr = NULL;
			}
		}
	}
	Debug3( LDAP_DEBUG_TRACE, "ldap_return_request: "
			"lrx->lr_msgid %d, lrx->lr_refcnt is now %d, lr is %s present\n",
			lrx->lr_msgid, lrx->lr_refcnt, lr ? "still" : "not" );
	/* The request is not tracked anymore */
	if ( lr == NULL ) {
		ldap_free_request_int( ld, lrx );
	} else if ( freeit ) {
		ldap_free_request( ld, lrx );
	}
}
