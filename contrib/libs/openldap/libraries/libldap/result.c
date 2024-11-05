/* result.c - wait for an ldap result */
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
 * April 2000 sfs Add code to process V3 referrals and search results
 *---
 * Note: A verbatim copy of version 2.0.1 of the OpenLDAP Public License 
 * can be found in the file "build/LICENSE-2.0.1" in this distribution
 * of OpenLDAP Software.
 */

/*
 * LDAPv3 (RFC 4511)
 *	LDAPResult ::= SEQUENCE {
 *		resultCode			ENUMERATED { ... },
 *		matchedDN			LDAPDN,
 *		diagnosticMessage		LDAPString,
 *		referral			[3] Referral OPTIONAL
 *	}
 *	Referral ::= SEQUENCE OF LDAPURL	(one or more)
 *	LDAPURL ::= LDAPString			(limited to URL chars)
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/errno.h>
#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>
#include <ac/unistd.h>

#include "ldap-int.h"
#include "ldap_log.h"
#include "lutil.h"

static int ldap_abandoned LDAP_P(( LDAP *ld, ber_int_t msgid ));
static int ldap_mark_abandoned LDAP_P(( LDAP *ld, ber_int_t msgid ));
static int wait4msg LDAP_P(( LDAP *ld, ber_int_t msgid, int all, struct timeval *timeout,
	LDAPMessage **result ));
static ber_tag_t try_read1msg LDAP_P(( LDAP *ld, ber_int_t msgid,
	int all, LDAPConn *lc, LDAPMessage **result ));
static ber_tag_t build_result_ber LDAP_P(( LDAP *ld, BerElement **bp, LDAPRequest *lr ));
static void merge_error_info LDAP_P(( LDAP *ld, LDAPRequest *parentr, LDAPRequest *lr ));
static LDAPMessage * chkResponseList LDAP_P(( LDAP *ld, int msgid, int all));

#define LDAP_MSG_X_KEEP_LOOKING		(-2)


/*
 * ldap_result - wait for an ldap result response to a message from the
 * ldap server.  If msgid is LDAP_RES_ANY (-1), any message will be
 * accepted.  If msgid is LDAP_RES_UNSOLICITED (0), any unsolicited
 * message is accepted.  Otherwise ldap_result will wait for a response
 * with msgid.  If all is LDAP_MSG_ONE (0) the first message with id
 * msgid will be accepted, otherwise, ldap_result will wait for all
 * responses with id msgid and then return a pointer to the entire list
 * of messages.  In general, this is only useful for search responses,
 * which can be of three message types (zero or more entries, zero or
 * search references, followed by an ldap result).  An extension to
 * LDAPv3 allows partial extended responses to be returned in response
 * to any request.  The type of the first message received is returned.
 * When waiting, any messages that have been abandoned/discarded are 
 * discarded.
 *
 * Example:
 *	ldap_result( s, msgid, all, timeout, result )
 */
int
ldap_result(
	LDAP *ld,
	int msgid,
	int all,
	struct timeval *timeout,
	LDAPMessage **result )
{
	int		rc;

	assert( ld != NULL );
	assert( result != NULL );

	Debug2( LDAP_DEBUG_TRACE, "ldap_result ld %p msgid %d\n", (void *)ld, msgid );

	if (ld->ld_errno == LDAP_LOCAL_ERROR || ld->ld_errno == LDAP_SERVER_DOWN)
		return -1;

	LDAP_MUTEX_LOCK( &ld->ld_res_mutex );
	rc = wait4msg( ld, msgid, all, timeout, result );
	LDAP_MUTEX_UNLOCK( &ld->ld_res_mutex );

	return rc;
}

/* protected by res_mutex */
static LDAPMessage *
chkResponseList(
	LDAP *ld,
	int msgid,
	int all)
{
	LDAPMessage	*lm, **lastlm, *nextlm;
	int		cnt = 0;

	/*
	 * Look through the list of responses we have received on
	 * this association and see if the response we're interested in
	 * is there.  If it is, return it.  If not, call wait4msg() to
	 * wait until it arrives or timeout occurs.
	 */

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_res_mutex );

	Debug3( LDAP_DEBUG_TRACE,
		"ldap_chkResponseList ld %p msgid %d all %d\n",
		(void *)ld, msgid, all );

	lastlm = &ld->ld_responses;
	for ( lm = ld->ld_responses; lm != NULL; lm = nextlm ) {
		nextlm = lm->lm_next;
		++cnt;

		if ( ldap_abandoned( ld, lm->lm_msgid ) ) {
			Debug2( LDAP_DEBUG_ANY,
				"response list msg abandoned, "
				"msgid %d message type %s\n",
				lm->lm_msgid, ldap_int_msgtype2str( lm->lm_msgtype ) );

			switch ( lm->lm_msgtype ) {
			case LDAP_RES_SEARCH_ENTRY:
			case LDAP_RES_SEARCH_REFERENCE:
			case LDAP_RES_INTERMEDIATE:
				break;

			default:
				/* there's no need to keep the id
				 * in the abandoned list any longer */
				ldap_mark_abandoned( ld, lm->lm_msgid );
				break;
			}

			/* Remove this entry from list */
			*lastlm = nextlm;

			ldap_msgfree( lm );

			continue;
		}

		if ( msgid == LDAP_RES_ANY || lm->lm_msgid == msgid ) {
			LDAPMessage	*tmp;

			if ( all == LDAP_MSG_ONE ||
				all == LDAP_MSG_RECEIVED ||
				msgid == LDAP_RES_UNSOLICITED )
			{
				break;
			}

			tmp = lm->lm_chain_tail;
			if ( tmp->lm_msgtype == LDAP_RES_SEARCH_ENTRY ||
				tmp->lm_msgtype == LDAP_RES_SEARCH_REFERENCE ||
				tmp->lm_msgtype == LDAP_RES_INTERMEDIATE )
			{
				tmp = NULL;
			}

			if ( tmp == NULL ) {
				lm = NULL;
			}

			break;
		}
		lastlm = &lm->lm_next;
	}

	if ( lm != NULL ) {
		/* Found an entry, remove it from the list */
		if ( all == LDAP_MSG_ONE && lm->lm_chain != NULL ) {
			*lastlm = lm->lm_chain;
			lm->lm_chain->lm_next = lm->lm_next;
			lm->lm_chain->lm_chain_tail = ( lm->lm_chain_tail != lm ) ? lm->lm_chain_tail : lm->lm_chain;
			lm->lm_chain = NULL;
			lm->lm_chain_tail = NULL;
		} else {
			*lastlm = lm->lm_next;
		}
		lm->lm_next = NULL;
	}

#ifdef LDAP_DEBUG
	if ( lm == NULL) {
		Debug1( LDAP_DEBUG_TRACE,
			"ldap_chkResponseList returns ld %p NULL\n", (void *)ld );
	} else {
		Debug3( LDAP_DEBUG_TRACE,
			"ldap_chkResponseList returns ld %p msgid %d, type 0x%02lx\n",
			(void *)ld, lm->lm_msgid, (unsigned long)lm->lm_msgtype );
	}
#endif

	return lm;
}

/* protected by res_mutex */
static int
wait4msg(
	LDAP *ld,
	ber_int_t msgid,
	int all,
	struct timeval *timeout,
	LDAPMessage **result )
{
	int		rc;
	struct timeval	tv = { 0 },
			tv0 = { 0 },
			start_time_tv = { 0 },
			*tvp = NULL;
	LDAPConn	*lc;

	assert( ld != NULL );
	assert( result != NULL );

	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_res_mutex );

	if ( timeout == NULL && ld->ld_options.ldo_tm_api.tv_sec >= 0 ) {
		tv = ld->ld_options.ldo_tm_api;
		timeout = &tv;
	}

#ifdef LDAP_DEBUG
	if ( timeout == NULL ) {
		Debug2( LDAP_DEBUG_TRACE, "wait4msg ld %p msgid %d (infinite timeout)\n",
			(void *)ld, msgid );
	} else {
		Debug3( LDAP_DEBUG_TRACE, "wait4msg ld %p msgid %d (timeout %ld usec)\n",
			(void *)ld, msgid, (long)timeout->tv_sec * 1000000 + timeout->tv_usec );
	}
#endif /* LDAP_DEBUG */

	if ( timeout != NULL && timeout->tv_sec != -1 ) {
		tv0 = *timeout;
		tv = *timeout;
		tvp = &tv;
#ifdef HAVE_GETTIMEOFDAY
		gettimeofday( &start_time_tv, NULL );
#else /* ! HAVE_GETTIMEOFDAY */
		start_time_tv.tv_sec = time( NULL );
		start_time_tv.tv_usec = 0;
#endif /* ! HAVE_GETTIMEOFDAY */
	}
		    
	rc = LDAP_MSG_X_KEEP_LOOKING;
	while ( rc == LDAP_MSG_X_KEEP_LOOKING ) {
#ifdef LDAP_DEBUG
		if ( ldap_debug & LDAP_DEBUG_TRACE ) {
			Debug3( LDAP_DEBUG_TRACE, "wait4msg continue ld %p msgid %d all %d\n",
				(void *)ld, msgid, all );
			ldap_dump_connection( ld, ld->ld_conns, 1 );
			LDAP_MUTEX_LOCK( &ld->ld_req_mutex );
			ldap_dump_requests_and_responses( ld );
			LDAP_MUTEX_UNLOCK( &ld->ld_req_mutex );
		}
#endif /* LDAP_DEBUG */

		if ( ( *result = chkResponseList( ld, msgid, all ) ) != NULL ) {
			rc = (*result)->lm_msgtype;

		} else {
			int lc_ready = 0;

			LDAP_MUTEX_LOCK( &ld->ld_conn_mutex );
			for ( lc = ld->ld_conns; lc != NULL; lc = lc->lconn_next ) {
				if ( ber_sockbuf_ctrl( lc->lconn_sb,
					LBER_SB_OPT_DATA_READY, NULL ) )
				{
					lc_ready = 2;	/* ready at ber level, not socket level */
					break;
				}
			}

			if ( !lc_ready ) {
				int err;
				rc = ldap_int_select( ld, tvp );
				if ( rc == -1 ) {
					err = sock_errno();
#ifdef LDAP_DEBUG
					Debug1( LDAP_DEBUG_TRACE,
						"ldap_int_select returned -1: errno %d\n",
						err );
#endif
				}

				if ( rc == 0 || ( rc == -1 && (
					!LDAP_BOOL_GET(&ld->ld_options, LDAP_BOOL_RESTART)
						|| err != EINTR ) ) )
				{
					ld->ld_errno = (rc == -1 ? LDAP_SERVER_DOWN :
						LDAP_TIMEOUT);
					LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
					return( rc );
				}

				if ( rc == -1 ) {
					rc = LDAP_MSG_X_KEEP_LOOKING;	/* select interrupted: loop */

				} else {
					lc_ready = 1;
				}
			}
			if ( lc_ready ) {
				LDAPConn *lnext;
				int serviced = 0;
				rc = LDAP_MSG_X_KEEP_LOOKING;
				LDAP_MUTEX_LOCK( &ld->ld_req_mutex );
				if ( ld->ld_requests != NULL ) {
					TAvlnode *node = ldap_tavl_end( ld->ld_requests, TAVL_DIR_RIGHT );
					LDAPRequest *lr;

					assert( node != NULL );
					lr = node->avl_data;
					if ( lr->lr_status == LDAP_REQST_WRITING &&
							ldap_is_write_ready( ld, lr->lr_conn->lconn_sb ) ) {
						serviced = 1;
						ldap_int_flush_request( ld, lr );
					}
				}
				for ( lc = ld->ld_conns;
					rc == LDAP_MSG_X_KEEP_LOOKING && lc != NULL;
					lc = lnext )
				{
					if ( lc->lconn_status == LDAP_CONNST_CONNECTED &&
						ldap_is_read_ready( ld, lc->lconn_sb ) )
					{
						serviced = 1;
						/* Don't let it get freed out from under us */
						++lc->lconn_refcnt;
						rc = try_read1msg( ld, msgid, all, lc, result );
						lnext = lc->lconn_next;

						/* Only take locks if we're really freeing */
						if ( lc->lconn_refcnt <= 1 ) {
							ldap_free_connection( ld, lc, 0, 1 );
						} else {
							--lc->lconn_refcnt;
						}
					} else {
						lnext = lc->lconn_next;
					}
				}
				LDAP_MUTEX_UNLOCK( &ld->ld_req_mutex );
				/* Quit looping if no one handled any socket events */
				if (!serviced && lc_ready == 1)
					rc = -1;
			}
			LDAP_MUTEX_UNLOCK( &ld->ld_conn_mutex );
		}

		if ( rc == LDAP_MSG_X_KEEP_LOOKING && tvp != NULL ) {
			struct timeval	curr_time_tv = { 0 },
					delta_time_tv = { 0 };

#ifdef HAVE_GETTIMEOFDAY
			gettimeofday( &curr_time_tv, NULL );
#else /* ! HAVE_GETTIMEOFDAY */
			curr_time_tv.tv_sec = time( NULL );
			curr_time_tv.tv_usec = 0;
#endif /* ! HAVE_GETTIMEOFDAY */

			/* delta_time = tmp_time - start_time */
			delta_time_tv.tv_sec = curr_time_tv.tv_sec - start_time_tv.tv_sec;
			delta_time_tv.tv_usec = curr_time_tv.tv_usec - start_time_tv.tv_usec;
			if ( delta_time_tv.tv_usec < 0 ) {
				delta_time_tv.tv_sec--;
				delta_time_tv.tv_usec += 1000000;
			}

			/* tv0 < delta_time ? */
			if ( ( tv0.tv_sec < delta_time_tv.tv_sec ) ||
			     ( ( tv0.tv_sec == delta_time_tv.tv_sec ) && ( tv0.tv_usec < delta_time_tv.tv_usec ) ) )
			{
				rc = 0; /* timed out */
				ld->ld_errno = LDAP_TIMEOUT;
				break;
			}

			/* tv0 -= delta_time */
			tv0.tv_sec -= delta_time_tv.tv_sec;
			tv0.tv_usec -= delta_time_tv.tv_usec;
			if ( tv0.tv_usec < 0 ) {
				tv0.tv_sec--;
				tv0.tv_usec += 1000000;
			}

			tv.tv_sec = tv0.tv_sec;
			tv.tv_usec = tv0.tv_usec;

			Debug3( LDAP_DEBUG_TRACE, "wait4msg ld %p %ld s %ld us to go\n",
				(void *)ld, (long) tv.tv_sec, (long) tv.tv_usec );

			start_time_tv.tv_sec = curr_time_tv.tv_sec;
			start_time_tv.tv_usec = curr_time_tv.tv_usec;
		}
	}

	return( rc );
}


/* protected by res_mutex, conn_mutex and req_mutex */
static ber_tag_t
try_read1msg(
	LDAP *ld,
	ber_int_t msgid,
	int all,
	LDAPConn *lc,
	LDAPMessage **result )
{
	BerElement	*ber;
	LDAPMessage	*newmsg, *l, *prev;
	ber_int_t	id;
	ber_tag_t	tag;
	ber_len_t	len;
	int		foundit = 0;
	LDAPRequest	*lr, *tmplr, dummy_lr = { 0 };
	BerElement	tmpber;
	int		rc, refer_cnt, hadref, simple_request, err;
	ber_int_t	lderr = -1;

#ifdef LDAP_CONNECTIONLESS
	LDAPMessage	*tmp = NULL, *chain_head = NULL;
	int		moremsgs = 0, isv2 = 0;
#endif

	assert( ld != NULL );
	assert( lc != NULL );
	
	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_res_mutex );
	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_conn_mutex );
	LDAP_ASSERT_MUTEX_OWNER( &ld->ld_req_mutex );

	Debug3( LDAP_DEBUG_TRACE, "read1msg: ld %p msgid %d all %d\n",
		(void *)ld, msgid, all );

retry:
	if ( lc->lconn_ber == NULL ) {
		lc->lconn_ber = ldap_alloc_ber_with_options( ld );

		if ( lc->lconn_ber == NULL ) {
			return -1;
		}
	}

	ber = lc->lconn_ber;
	assert( LBER_VALID (ber) );

	/* get the next message */
	sock_errset(0);
#ifdef LDAP_CONNECTIONLESS
	if ( LDAP_IS_UDP(ld) ) {
		struct sockaddr_storage from;
		if ( ber_int_sb_read( lc->lconn_sb, &from, sizeof(struct sockaddr_storage) ) < 0 )
			goto fail;
		if ( ld->ld_options.ldo_version == LDAP_VERSION2 ) isv2 = 1;
	}
nextresp3:
#endif
	tag = ber_get_next( lc->lconn_sb, &len, ber );
	switch ( tag ) {
	case LDAP_TAG_MESSAGE:
		/*
	 	 * We read a complete message.
	 	 * The connection should no longer need this ber.
	 	 */
		lc->lconn_ber = NULL;
		break;

	default:
		/*
		 * We read a BerElement that isn't LDAP or the stream has desync'd.
		 * In either case, anything we read from now on is probably garbage,
		 * just drop the connection.
		 */
		ber_free( ber, 1 );
		lc->lconn_ber = NULL;
		/* FALLTHRU */

	case LBER_DEFAULT:
fail:
		err = sock_errno();
#ifdef LDAP_DEBUG		   
		Debug1( LDAP_DEBUG_CONNS,
			"ber_get_next failed, errno=%d.\n", err );
#endif		   
		if ( err == EWOULDBLOCK ) return LDAP_MSG_X_KEEP_LOOKING;
		if ( err == EAGAIN ) return LDAP_MSG_X_KEEP_LOOKING;
		ld->ld_errno = LDAP_SERVER_DOWN;
		if ( !LDAP_BOOL_GET( &ld->ld_options, LDAP_BOOL_KEEPCONN )) {
			--lc->lconn_refcnt;
		}
		lc->lconn_status = 0;
		return -1;
	}

	/* message id */
	if ( ber_get_int( ber, &id ) == LBER_ERROR ) {
		ber_free( ber, 1 );
		ld->ld_errno = LDAP_DECODING_ERROR;
		return( -1 );
	}

	/* id == 0 iff unsolicited notification message (RFC 4511) */

	/* id < 0 is invalid, just toss it. FIXME: should we disconnect? */
	if ( id < 0 ) {
		goto retry_ber;
	}
	
	/* if it's been abandoned, toss it */
	if ( id > 0 ) {
		if ( ldap_abandoned( ld, id ) ) {
			/* the message type */
			tag = ber_peek_tag( ber, &len );
			switch ( tag ) {
			case LDAP_RES_SEARCH_ENTRY:
			case LDAP_RES_SEARCH_REFERENCE:
			case LDAP_RES_INTERMEDIATE:
			case LBER_ERROR:
				break;

			default:
				/* there's no need to keep the id
				 * in the abandoned list any longer */
				ldap_mark_abandoned( ld, id );
				break;
			}

			Debug3( LDAP_DEBUG_ANY,
				"abandoned/discarded ld %p msgid %d message type %s\n",
				(void *)ld, id, ldap_int_msgtype2str( tag ) );

retry_ber:
			ber_free( ber, 1 );
			if ( ber_sockbuf_ctrl( lc->lconn_sb, LBER_SB_OPT_DATA_READY, NULL ) ) {
				goto retry;
			}
			return( LDAP_MSG_X_KEEP_LOOKING );	/* continue looking */
		}

		lr = ldap_find_request_by_msgid( ld, id );
		if ( lr == NULL ) {
			const char	*msg = "unknown";

			/* the message type */
			tag = ber_peek_tag( ber, &len );
			switch ( tag ) {
			case LBER_ERROR:
				break;

			default:
				msg = ldap_int_msgtype2str( tag );
				break;
			}

			Debug3( LDAP_DEBUG_ANY,
				"no request for response on ld %p msgid %d message type %s (tossing)\n",
				(void *)ld, id, msg );

			goto retry_ber;
		}

#ifdef LDAP_CONNECTIONLESS
		if ( LDAP_IS_UDP(ld) && isv2 ) {
			ber_scanf(ber, "x{");
		}
nextresp2:
		;
#endif
	}

	/* the message type */
	tag = ber_peek_tag( ber, &len );
	if ( tag == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		ber_free( ber, 1 );
		return( -1 );
	}

	Debug3( LDAP_DEBUG_TRACE,
		"read1msg: ld %p msgid %d message type %s\n",
		(void *)ld, id, ldap_int_msgtype2str( tag ) );

	if ( id == 0 ) {
		/* unsolicited notification message (RFC 4511) */
		if ( tag != LDAP_RES_EXTENDED ) {
			/* toss it */
			goto retry_ber;

			/* strictly speaking, it's an error; from RFC 4511:

4.4.  Unsolicited Notification

   An unsolicited notification is an LDAPMessage sent from the server to
   the client that is not in response to any LDAPMessage received by the
   server.  It is used to signal an extraordinary condition in the
   server or in the LDAP session between the client and the server.  The
   notification is of an advisory nature, and the server will not expect
   any response to be returned from the client.

   The unsolicited notification is structured as an LDAPMessage in which
   the messageID is zero and protocolOp is set to the extendedResp
   choice using the ExtendedResponse type (See Section 4.12).  The
   responseName field of the ExtendedResponse always contains an LDAPOID
   that is unique for this notification.

			 * however, since unsolicited responses
			 * are of advisory nature, better
			 * toss it, right now
			 */

#if 0
			ld->ld_errno = LDAP_DECODING_ERROR;
			ber_free( ber, 1 );
			return( -1 );
#endif
		}

		lr = &dummy_lr;
	}

	id = lr->lr_origid;
	refer_cnt = 0;
	hadref = simple_request = 0;
	rc = LDAP_MSG_X_KEEP_LOOKING;	/* default is to keep looking (no response found) */
	lr->lr_res_msgtype = tag;

	/*
	 * Check for V3 search reference
	 */
	if ( tag == LDAP_RES_SEARCH_REFERENCE ) {
		if ( ld->ld_version > LDAP_VERSION2 ) {
			/* This is a V3 search reference */
			if ( LDAP_BOOL_GET(&ld->ld_options, LDAP_BOOL_REFERRALS) ||
					lr->lr_parent != NULL )
			{
				char **refs = NULL;
				tmpber = *ber;

				/* Get the referral list */
				if ( ber_scanf( &tmpber, "{v}", &refs ) == LBER_ERROR ) {
					rc = LDAP_DECODING_ERROR;

				} else {
					/* Note: refs array is freed by ldap_chase_v3referrals */
					refer_cnt = ldap_chase_v3referrals( ld, lr, refs,
						1, &lr->lr_res_error, &hadref );
					if ( refer_cnt > 0 ) {
						/* successfully chased reference */
						/* If haven't got end search, set chasing referrals */
						if ( lr->lr_status != LDAP_REQST_COMPLETED ) {
							lr->lr_status = LDAP_REQST_CHASINGREFS;
							Debug1( LDAP_DEBUG_TRACE,
								"read1msg:  search ref chased, "
								"mark request chasing refs, "
								"id = %d\n",
								lr->lr_msgid );
						}
					}
				}
			}
		}

	} else if ( tag != LDAP_RES_SEARCH_ENTRY && tag != LDAP_RES_INTERMEDIATE ) {
		/* All results that just return a status, i.e. don't return data
		 * go through the following code.  This code also chases V2 referrals
		 * and checks if all referrals have been chased.
		 */
		char		*lr_res_error = NULL;

		tmpber = *ber; 	/* struct copy */
		if ( ber_scanf( &tmpber, "{eAA", &lderr,
				&lr->lr_res_matched, &lr_res_error )
				!= LBER_ERROR )
		{
			if ( lr_res_error != NULL ) {
				if ( lr->lr_res_error != NULL ) {
					(void)ldap_append_referral( ld, &lr->lr_res_error, lr_res_error );
					LDAP_FREE( (char *)lr_res_error );

				} else {
					lr->lr_res_error = lr_res_error;
				}
				lr_res_error = NULL;
			}

			/* Do we need to check for referrals? */
			if ( tag != LDAP_RES_BIND &&
				( LDAP_BOOL_GET(&ld->ld_options, LDAP_BOOL_REFERRALS) ||
					lr->lr_parent != NULL ))
			{
				char		**refs = NULL;
				ber_len_t	len;

				/* Check if V3 referral */
				if ( ber_peek_tag( &tmpber, &len ) == LDAP_TAG_REFERRAL ) {
					if ( ld->ld_version > LDAP_VERSION2 ) {
						/* Get the referral list */
						if ( ber_scanf( &tmpber, "{v}", &refs) == LBER_ERROR) {
							rc = LDAP_DECODING_ERROR;
							lr->lr_status = LDAP_REQST_COMPLETED;
							Debug2( LDAP_DEBUG_TRACE,
								"read1msg: referral decode error, "
								"mark request completed, ld %p msgid %d\n",
								(void *)ld, lr->lr_msgid );

						} else {
							/* Chase the referral 
							 * refs array is freed by ldap_chase_v3referrals
							 */
							refer_cnt = ldap_chase_v3referrals( ld, lr, refs,
								0, &lr->lr_res_error, &hadref );
							lr->lr_status = LDAP_REQST_COMPLETED;
							Debug3( LDAP_DEBUG_TRACE,
								"read1msg: referral %s chased, "
								"mark request completed, ld %p msgid %d\n",
								refer_cnt > 0 ? "" : "not",
								(void *)ld, lr->lr_msgid);
							if ( refer_cnt < 0 ) {
								refer_cnt = 0;
							}
						}
					}
				} else {
					switch ( lderr ) {
					case LDAP_SUCCESS:
					case LDAP_COMPARE_TRUE:
					case LDAP_COMPARE_FALSE:
						break;

					default:
						if ( lr->lr_res_error == NULL ) {
							break;
						}

						/* pedantic, should never happen */
						if ( lr->lr_res_error[ 0 ] == '\0' ) {
							LDAP_FREE( lr->lr_res_error );
							lr->lr_res_error = NULL;
							break;	
						}

						/* V2 referrals are in error string */
						refer_cnt = ldap_chase_referrals( ld, lr,
							&lr->lr_res_error, -1, &hadref );
						lr->lr_status = LDAP_REQST_COMPLETED;
						Debug1( LDAP_DEBUG_TRACE,
							"read1msg:  V2 referral chased, "
							"mark request completed, id = %d\n",
							lr->lr_msgid );
						break;
					}
				}
			}

			/* save errno, message, and matched string */
			if ( !hadref || lr->lr_res_error == NULL ) {
				lr->lr_res_errno =
					lderr == LDAP_PARTIAL_RESULTS
					? LDAP_SUCCESS : lderr;

			} else if ( ld->ld_errno != LDAP_SUCCESS ) {
				lr->lr_res_errno = ld->ld_errno;

			} else {
				lr->lr_res_errno = LDAP_PARTIAL_RESULTS;
			}
		}

		/* in any case, don't leave any lr_res_error 'round */
		if ( lr_res_error ) {
			LDAP_FREE( lr_res_error );
		}

		Debug2( LDAP_DEBUG_TRACE,
			"read1msg: ld %p %d new referrals\n",
			(void *)ld, refer_cnt );

		if ( refer_cnt != 0 ) {	/* chasing referrals */
			ber_free( ber, 1 );
			ber = NULL;
			if ( refer_cnt < 0 ) {
				ldap_return_request( ld, lr, 0 );
				return( -1 );	/* fatal error */
			}
			lr->lr_res_errno = LDAP_SUCCESS; /* successfully chased referral */
			if ( lr->lr_res_matched ) {
				LDAP_FREE( lr->lr_res_matched );
				lr->lr_res_matched = NULL;
			}

		} else {
			if ( lr->lr_outrefcnt <= 0 && lr->lr_parent == NULL ) {
				/* request without any referrals */
				simple_request = ( hadref ? 0 : 1 );

			} else {
				/* request with referrals or child request */
				ber_free( ber, 1 );
				ber = NULL;
			}

			lr->lr_status = LDAP_REQST_COMPLETED; /* declare this request done */
			Debug2( LDAP_DEBUG_TRACE,
				"read1msg:  mark request completed, ld %p msgid %d\n",
				(void *)ld, lr->lr_msgid );
			tmplr = lr;
			while ( lr->lr_parent != NULL ) {
				merge_error_info( ld, lr->lr_parent, lr );

				lr = lr->lr_parent;
				if ( --lr->lr_outrefcnt > 0 ) {
					break;	/* not completely done yet */
				}
			}
			/* ITS#6744: Original lr was refcounted when we retrieved it,
			 * must release it now that we're working with the parent
			 */
			if ( tmplr->lr_parent ) {
				ldap_return_request( ld, tmplr, 0 );
			}

			/* Check if all requests are finished, lr is now parent */
			tmplr = lr;
			if ( tmplr->lr_status == LDAP_REQST_COMPLETED ) {
				for ( tmplr = lr->lr_child;
					tmplr != NULL;
					tmplr = tmplr->lr_refnext )
				{
					if ( tmplr->lr_status != LDAP_REQST_COMPLETED ) break;
				}
			}

			/* This is the parent request if the request has referrals */
			if ( lr->lr_outrefcnt <= 0 &&
				lr->lr_parent == NULL &&
				tmplr == NULL )
			{
				id = lr->lr_msgid;
				tag = lr->lr_res_msgtype;
				Debug2( LDAP_DEBUG_TRACE, "request done: ld %p msgid %d\n",
					(void *)ld, id );
				Debug3( LDAP_DEBUG_TRACE,
					"res_errno: %d, res_error: <%s>, "
					"res_matched: <%s>\n",
					lr->lr_res_errno,
					lr->lr_res_error ? lr->lr_res_error : "",
					lr->lr_res_matched ? lr->lr_res_matched : "" );
				if ( !simple_request ) {
					ber_free( ber, 1 );
					ber = NULL;
					if ( build_result_ber( ld, &ber, lr )
					    == LBER_ERROR )
					{
						rc = -1; /* fatal error */
					}
				}

				if ( lr != &dummy_lr ) {
					ldap_return_request( ld, lr, 1 );
				}
				lr = NULL;
			}

			/*
			 * RFC 4511 unsolicited (id == 0) responses
			 * shouldn't necessarily end the connection
			 */
			if ( lc != NULL && id != 0 &&
			     !LDAP_BOOL_GET( &ld->ld_options, LDAP_BOOL_KEEPCONN )) {
				--lc->lconn_refcnt;
				lc = NULL;
			}
		}
	}

	if ( lr != NULL ) {
		if ( lr != &dummy_lr ) {
			ldap_return_request( ld, lr, 0 );
		}
		lr = NULL;
	}

	if ( ber == NULL ) {
		return( rc );
	}

	/* try to handle unsolicited responses as appropriate */
	if ( id == 0 && msgid > LDAP_RES_UNSOLICITED ) {
		int	is_nod = 0;

		tag = ber_peek_tag( &tmpber, &len );

		/* we have a res oid */
		if ( tag == LDAP_TAG_EXOP_RES_OID ) {
			static struct berval	bv_nod = BER_BVC( LDAP_NOTICE_OF_DISCONNECTION );
			struct berval		resoid = BER_BVNULL;

			if ( ber_scanf( &tmpber, "m", &resoid ) == LBER_ERROR ) {
				ld->ld_errno = LDAP_DECODING_ERROR;
				ber_free( ber, 1 );
				return -1;
			}

			assert( !BER_BVISEMPTY( &resoid ) );

			is_nod = ber_bvcmp( &resoid, &bv_nod ) == 0;

			tag = ber_peek_tag( &tmpber, &len );
		}

#if 0 /* don't need right now */
		/* we have res data */
		if ( tag == LDAP_TAG_EXOP_RES_VALUE ) {
			struct berval resdata;

			if ( ber_scanf( &tmpber, "m", &resdata ) == LBER_ERROR ) {
				ld->ld_errno = LDAP_DECODING_ERROR;
				ber_free( ber, 0 );
				return ld->ld_errno;
			}

			/* use it... */
		}
#endif

		/* handle RFC 4511 "Notice of Disconnection" locally */

		if ( is_nod ) {
			if ( tag == LDAP_TAG_EXOP_RES_VALUE ) {
				ld->ld_errno = LDAP_DECODING_ERROR;
				ber_free( ber, 1 );
				return -1;
			}

			/* get rid of the connection... */
			if ( lc != NULL &&
			     !LDAP_BOOL_GET( &ld->ld_options, LDAP_BOOL_KEEPCONN )) {
				--lc->lconn_refcnt;
			}

			/* need to return -1, because otherwise
			 * a valid result is expected */
			ld->ld_errno = lderr;
			return -1;
		}
	}

	/* make a new ldap message */
	newmsg = (LDAPMessage *) LDAP_CALLOC( 1, sizeof(LDAPMessage) );
	if ( newmsg == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return( -1 );
	}
	newmsg->lm_msgid = (int)id;
	newmsg->lm_msgtype = tag;
	newmsg->lm_ber = ber;
	newmsg->lm_chain_tail = newmsg;

#ifdef LDAP_CONNECTIONLESS
	/* CLDAP replies all fit in a single datagram. In LDAPv2 RFC1798
	 * the responses are all a sequence wrapped in one message. In
	 * LDAPv3 each response is in its own message. The datagram must
	 * end with a SearchResult. We can't just parse each response in
	 * separate calls to try_read1msg because the header info is only
	 * present at the beginning of the datagram, not at the beginning
	 * of each response. So parse all the responses at once and queue
	 * them up, then pull off the first response to return to the
	 * caller when all parsing is complete.
	 */
	if ( LDAP_IS_UDP(ld) ) {
		/* If not a result, look for more */
		if ( tag != LDAP_RES_SEARCH_RESULT ) {
			int ok = 0;
			moremsgs = 1;
			if (isv2) {
				/* LDAPv2: dup the current ber, skip past the current
				 * response, and see if there are any more after it.
				 */
				ber = ber_dup( ber );
				ber_scanf( ber, "x" );
				if ( ber_peek_tag( ber, &len ) != LBER_DEFAULT ) {
					/* There's more - dup the ber buffer so they can all be
					 * individually freed by ldap_msgfree.
					 */
					struct berval bv;
					ber_get_option( ber, LBER_OPT_BER_REMAINING_BYTES, &len );
					bv.bv_val = LDAP_MALLOC( len );
					if ( bv.bv_val ) {
						ok = 1;
						ber_read( ber, bv.bv_val, len );
						bv.bv_len = len;
						ber_init2( ber, &bv, ld->ld_lberoptions );
					}
				}
			} else {
				/* LDAPv3: Just allocate a new ber. Since this is a buffered
				 * datagram, if the sockbuf is readable we still have data
				 * to parse.
				 */
				ber = ldap_alloc_ber_with_options( ld );
				if ( ber == NULL ) {
					ld->ld_errno = LDAP_NO_MEMORY;
					return -1;
				}

				if ( ber_sockbuf_ctrl( lc->lconn_sb, LBER_SB_OPT_DATA_READY, NULL ) ) ok = 1;
			}
			/* set up response chain */
			if ( tmp == NULL ) {
				newmsg->lm_next = ld->ld_responses;
				ld->ld_responses = newmsg;
				chain_head = newmsg;
			} else {
				tmp->lm_chain = newmsg;
			}
			chain_head->lm_chain_tail = newmsg;
			tmp = newmsg;
			/* "ok" means there's more to parse */
			if ( ok ) {
				if ( isv2 ) {
					goto nextresp2;

				} else {
					goto nextresp3;
				}
			} else {
				/* got to end of datagram without a SearchResult. Free
				 * our dup'd ber, but leave any buffer alone. For v2 case,
				 * the previous response is still using this buffer. For v3,
				 * the new ber has no buffer to free yet.
				 */
				ber_free( ber, 0 );
				return -1;
			}
		} else if ( moremsgs ) {
		/* got search result, and we had multiple responses in 1 datagram.
		 * stick the result onto the end of the chain, and then pull the
		 * first response off the head of the chain.
		 */
			tmp->lm_chain = newmsg;
			chain_head->lm_chain_tail = newmsg;
			*result = chkResponseList( ld, msgid, all );
			ld->ld_errno = LDAP_SUCCESS;
			return( (*result)->lm_msgtype );
		}
	}
#endif /* LDAP_CONNECTIONLESS */

	/* is this the one we're looking for? */
	if ( msgid == LDAP_RES_ANY || id == msgid ) {
		if ( all == LDAP_MSG_ONE
			|| ( newmsg->lm_msgtype != LDAP_RES_SEARCH_RESULT
				&& newmsg->lm_msgtype != LDAP_RES_SEARCH_ENTRY
				&& newmsg->lm_msgtype != LDAP_RES_INTERMEDIATE
			  	&& newmsg->lm_msgtype != LDAP_RES_SEARCH_REFERENCE ) )
		{
			*result = newmsg;
			ld->ld_errno = LDAP_SUCCESS;
			return( tag );

		} else if ( newmsg->lm_msgtype == LDAP_RES_SEARCH_RESULT) {
			foundit = 1;	/* return the chain later */
		}
	}

	/* 
	 * if not, we must add it to the list of responses.  if
	 * the msgid is already there, it must be part of an existing
	 * search response.
	 */

	prev = NULL;
	for ( l = ld->ld_responses; l != NULL; l = l->lm_next ) {
		if ( l->lm_msgid == newmsg->lm_msgid ) {
			break;
		}
		prev = l;
	}

	/* not part of an existing search response */
	if ( l == NULL ) {
		if ( foundit ) {
			*result = newmsg;
			goto exit;
		}

		newmsg->lm_next = ld->ld_responses;
		ld->ld_responses = newmsg;
		goto exit;
	}

	Debug3( LDAP_DEBUG_TRACE, "adding response ld %p msgid %d type %ld:\n",
		(void *)ld, newmsg->lm_msgid, (long) newmsg->lm_msgtype );

	/* part of a search response - add to end of list of entries */
	l->lm_chain_tail->lm_chain = newmsg;
	l->lm_chain_tail = newmsg;

	/* return the whole chain if that's what we were looking for */
	if ( foundit ) {
		if ( prev == NULL ) {
			ld->ld_responses = l->lm_next;
		} else {
			prev->lm_next = l->lm_next;
		}
		*result = l;
	}

exit:
	if ( foundit ) {
		ld->ld_errno = LDAP_SUCCESS;
		return( tag );
	}
	if ( lc && ber_sockbuf_ctrl( lc->lconn_sb, LBER_SB_OPT_DATA_READY, NULL ) ) {
		goto retry;
	}
	return( LDAP_MSG_X_KEEP_LOOKING );	/* continue looking */
}


static ber_tag_t
build_result_ber( LDAP *ld, BerElement **bp, LDAPRequest *lr )
{
	ber_len_t	len;
	ber_tag_t	tag;
	ber_int_t	along;
	BerElement *ber;

	*bp = NULL;
	ber = ldap_alloc_ber_with_options( ld );

	if( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return LBER_ERROR;
	}

	if ( ber_printf( ber, "{it{ess}}", lr->lr_msgid,
		lr->lr_res_msgtype, lr->lr_res_errno,
		lr->lr_res_matched ? lr->lr_res_matched : "",
		lr->lr_res_error ? lr->lr_res_error : "" ) == -1 )
	{
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( LBER_ERROR );
	}

	ber_reset( ber, 1 );

	if ( ber_skip_tag( ber, &len ) == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		ber_free( ber, 1 );
		return( LBER_ERROR );
	}

	if ( ber_get_enum( ber, &along ) == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		ber_free( ber, 1 );
		return( LBER_ERROR );
	}

	tag = ber_peek_tag( ber, &len );

	if ( tag == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		ber_free( ber, 1 );
		return( LBER_ERROR );
	}

	*bp = ber;
	return tag;
}


/*
 * Merge error information in "lr" with "parentr" error code and string.
 */
static void
merge_error_info( LDAP *ld, LDAPRequest *parentr, LDAPRequest *lr )
{
	if ( lr->lr_res_errno == LDAP_PARTIAL_RESULTS ) {
		parentr->lr_res_errno = lr->lr_res_errno;
		if ( lr->lr_res_error != NULL ) {
			(void)ldap_append_referral( ld, &parentr->lr_res_error,
				lr->lr_res_error );
		}

	} else if ( lr->lr_res_errno != LDAP_SUCCESS &&
		parentr->lr_res_errno == LDAP_SUCCESS )
	{
		parentr->lr_res_errno = lr->lr_res_errno;
		if ( parentr->lr_res_error != NULL ) {
			LDAP_FREE( parentr->lr_res_error );
		}
		parentr->lr_res_error = lr->lr_res_error;
		lr->lr_res_error = NULL;
		if ( LDAP_NAME_ERROR( lr->lr_res_errno ) ) {
			if ( parentr->lr_res_matched != NULL ) {
				LDAP_FREE( parentr->lr_res_matched );
			}
			parentr->lr_res_matched = lr->lr_res_matched;
			lr->lr_res_matched = NULL;
		}
	}

	Debug1( LDAP_DEBUG_TRACE, "merged parent (id %d) error info:  ",
		parentr->lr_msgid );
	Debug3( LDAP_DEBUG_TRACE, "result errno %d, error <%s>, matched <%s>\n",
		parentr->lr_res_errno,
		parentr->lr_res_error ?  parentr->lr_res_error : "",
		parentr->lr_res_matched ?  parentr->lr_res_matched : "" );
}



int
ldap_msgtype( LDAPMessage *lm )
{
	assert( lm != NULL );
	return ( lm != NULL ) ? (int)lm->lm_msgtype : -1;
}


int
ldap_msgid( LDAPMessage *lm )
{
	assert( lm != NULL );

	return ( lm != NULL ) ? lm->lm_msgid : -1;
}


const char *
ldap_int_msgtype2str( ber_tag_t tag )
{
	switch( tag ) {
	case LDAP_RES_ADD: return "add";
	case LDAP_RES_BIND: return "bind";
	case LDAP_RES_COMPARE: return "compare";
	case LDAP_RES_DELETE: return "delete";
	case LDAP_RES_EXTENDED: return "extended-result";
	case LDAP_RES_INTERMEDIATE: return "intermediate";
	case LDAP_RES_MODIFY: return "modify";
	case LDAP_RES_RENAME: return "rename";
	case LDAP_RES_SEARCH_ENTRY: return "search-entry";
	case LDAP_RES_SEARCH_REFERENCE: return "search-reference";
	case LDAP_RES_SEARCH_RESULT: return "search-result";
	}
	return "unknown";
}

int
ldap_msgfree( LDAPMessage *lm )
{
	LDAPMessage	*next;
	int		type = 0;

	Debug0( LDAP_DEBUG_TRACE, "ldap_msgfree\n" );

	for ( ; lm != NULL; lm = next ) {
		next = lm->lm_chain;
		type = lm->lm_msgtype;
		ber_free( lm->lm_ber, 1 );
		LDAP_FREE( (char *) lm );
	}

	return type;
}

/*
 * ldap_msgdelete - delete a message.  It returns:
 *	0	if the entire message was deleted
 *	-1	if the message was not found, or only part of it was found
 */
int
ldap_msgdelete( LDAP *ld, int msgid )
{
	LDAPMessage	*lm, *prev;
	int		rc = 0;

	assert( ld != NULL );

	Debug2( LDAP_DEBUG_TRACE, "ldap_msgdelete ld=%p msgid=%d\n",
		(void *)ld, msgid );

	LDAP_MUTEX_LOCK( &ld->ld_res_mutex );
	prev = NULL;
	for ( lm = ld->ld_responses; lm != NULL; lm = lm->lm_next ) {
		if ( lm->lm_msgid == msgid ) {
			break;
		}
		prev = lm;
	}

	if ( lm == NULL ) {
		rc = -1;

	} else {
		if ( prev == NULL ) {
			ld->ld_responses = lm->lm_next;
		} else {
			prev->lm_next = lm->lm_next;
		}
	}
	LDAP_MUTEX_UNLOCK( &ld->ld_res_mutex );
	if ( lm ) {
		switch ( ldap_msgfree( lm ) ) {
		case LDAP_RES_SEARCH_ENTRY:
		case LDAP_RES_SEARCH_REFERENCE:
		case LDAP_RES_INTERMEDIATE:
			rc = -1;
			break;

		default:
			break;
		}
	}

	return rc;
}


/*
 * ldap_abandoned
 *
 * return the location of the message id in the array of abandoned
 * message ids, or -1
 */
static int
ldap_abandoned( LDAP *ld, ber_int_t msgid )
{
	int	ret, idx;
	assert( msgid >= 0 );

	LDAP_MUTEX_LOCK( &ld->ld_abandon_mutex );
	ret = ldap_int_bisect_find( ld->ld_abandoned, ld->ld_nabandoned, msgid, &idx );
	LDAP_MUTEX_UNLOCK( &ld->ld_abandon_mutex );
	return ret;
}

/*
 * ldap_mark_abandoned
 */
static int
ldap_mark_abandoned( LDAP *ld, ber_int_t msgid )
{
	int	ret, idx;

	assert( msgid >= 0 );
	LDAP_MUTEX_LOCK( &ld->ld_abandon_mutex );
	ret = ldap_int_bisect_find( ld->ld_abandoned, ld->ld_nabandoned, msgid, &idx );
	if (ret <= 0) {		/* error or already deleted by another thread */
		LDAP_MUTEX_UNLOCK( &ld->ld_abandon_mutex );
		return ret;
	}
	/* still in abandoned array, so delete */
	ret = ldap_int_bisect_delete( &ld->ld_abandoned, &ld->ld_nabandoned,
		msgid, idx );
	LDAP_MUTEX_UNLOCK( &ld->ld_abandon_mutex );
	return ret;
}
