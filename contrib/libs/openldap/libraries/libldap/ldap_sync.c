/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 2006-2024 The OpenLDAP Foundation.
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
/* ACKNOWLEDGEMENTS:
 * This program was originally developed by Pierangelo Masarati
 * for inclusion in OpenLDAP Software.
 */

/*
 * Proof-of-concept API that implement the client-side
 * of the "LDAP Content Sync Operation" (RFC 4533)
 */

#include "portable.h"

#include <ac/time.h>

#include "ldap-int.h"

#ifdef LDAP_SYNC_TRACE
static const char *
ldap_sync_state2str( int state )
{
	switch ( state ) {
	case LDAP_SYNC_PRESENT:
		return "LDAP_SYNC_PRESENT";

	case LDAP_SYNC_ADD:
		return "LDAP_SYNC_ADD";

	case LDAP_SYNC_MODIFY:
		return "LDAP_SYNC_MODIFY";

	case LDAP_SYNC_DELETE:
		return "LDAP_SYNC_DELETE";

	default:
		return "(unknown)";
	}
}
#endif

/*
 * initialize the persistent search structure
 */
ldap_sync_t *
ldap_sync_initialize( ldap_sync_t *ls_in )
{
	ldap_sync_t	*ls = ls_in;

	if ( ls == NULL ) {
		ls = ldap_memalloc( sizeof( ldap_sync_t ) );
		if ( ls == NULL ) {
			return NULL;
		}
	}
	memset( ls, 0, sizeof( ldap_sync_t ) );

	ls->ls_scope = LDAP_SCOPE_SUBTREE;
	ls->ls_timeout = -1;

	return ls;
}

/*
 * destroy the persistent search structure
 */
void
ldap_sync_destroy( ldap_sync_t *ls, int freeit )
{
	assert( ls != NULL );

	if ( ls->ls_base != NULL ) {
		ldap_memfree( ls->ls_base );
		ls->ls_base = NULL;
	}

	if ( ls->ls_filter != NULL ) {
		ldap_memfree( ls->ls_filter );
		ls->ls_filter = NULL;
	}

	if ( ls->ls_attrs != NULL ) {
		int	i;

		for ( i = 0; ls->ls_attrs[ i ] != NULL; i++ ) {
			ldap_memfree( ls->ls_attrs[ i ] );
		}
		ldap_memfree( ls->ls_attrs );
		ls->ls_attrs = NULL;
	}

	if ( ls->ls_ld != NULL ) {
		(void)ldap_unbind_ext( ls->ls_ld, NULL, NULL );
#ifdef LDAP_SYNC_TRACE
		fprintf( stderr, "ldap_unbind_ext()\n" );
#endif /* LDAP_SYNC_TRACE */
		ls->ls_ld = NULL;
	}

	if ( ls->ls_cookie.bv_val != NULL ) {
		ldap_memfree( ls->ls_cookie.bv_val );
		ls->ls_cookie.bv_val = NULL;
	}

	if ( freeit ) {
		ldap_memfree( ls );
	}
}

/*
 * handle the LDAP_RES_SEARCH_ENTRY response
 */
static int
ldap_sync_search_entry( ldap_sync_t *ls, LDAPMessage *res )
{
	LDAPControl		**ctrls = NULL;
	int			rc = LDAP_OTHER,
				i;
	BerElement		*ber = NULL;
	struct berval		entryUUID = { 0 },
				cookie = { 0 };
	int			state = -1;
	ber_len_t		len;
	ldap_sync_refresh_t	phase;

#ifdef LDAP_SYNC_TRACE
	fprintf( stderr, "\tgot LDAP_RES_SEARCH_ENTRY\n" );
#endif /* LDAP_SYNC_TRACE */

	assert( ls != NULL );
	assert( res != NULL );

	phase = ls->ls_refreshPhase;

	/* OK */

	/* extract:
	 * - data
	 * - entryUUID
	 *
	 * check that:
	 * - Sync State Control is "add"
	 */

	/* the control MUST be present */

	/* extract controls */
	ldap_get_entry_controls( ls->ls_ld, res, &ctrls );
	if ( ctrls == NULL ) {
		goto done;
	}

	/* lookup the sync state control */
	for ( i = 0; ctrls[ i ] != NULL; i++ ) {
		if ( strcmp( ctrls[ i ]->ldctl_oid, LDAP_CONTROL_SYNC_STATE ) == 0 ) {
			break;
		}
	}

	/* control must be present; there might be other... */
	if ( ctrls[ i ] == NULL ) {
		goto done;
	}

	/* extract data */
	ber = ber_init( &ctrls[ i ]->ldctl_value );
	if ( ber == NULL ) {
		goto done;
	}
	/* scan entryUUID in-place ("m") */
	if ( ber_scanf( ber, "{em" /*"}"*/, &state, &entryUUID ) == LBER_ERROR
		|| entryUUID.bv_len == 0 )
	{
		goto done;
	}

	if ( ber_peek_tag( ber, &len ) == LDAP_TAG_SYNC_COOKIE ) {
		/* scan cookie in-place ("m") */
		if ( ber_scanf( ber, /*"{"*/ "m}", &cookie ) == LBER_ERROR ) {
			goto done;
		}
		if ( cookie.bv_val != NULL ) {
			ber_bvreplace( &ls->ls_cookie, &cookie );
		}
#ifdef LDAP_SYNC_TRACE
		fprintf( stderr, "\t\tgot cookie=%s\n",
			cookie.bv_val ? cookie.bv_val : "(null)" );
#endif /* LDAP_SYNC_TRACE */
	}

	switch ( state ) {
	case LDAP_SYNC_PRESENT:
	case LDAP_SYNC_DELETE:
	case LDAP_SYNC_ADD:
	case LDAP_SYNC_MODIFY:
		/* NOTE: ldap_sync_refresh_t is defined
		 * as the corresponding LDAP_SYNC_*
		 * for the 4 above cases */
		phase = state;
#ifdef LDAP_SYNC_TRACE
		fprintf( stderr, "\t\tgot syncState=%s\n", ldap_sync_state2str( state ) );
#endif /* LDAP_SYNC_TRACE */
		break;

	default:
#ifdef LDAP_SYNC_TRACE
		fprintf( stderr, "\t\tgot unknown syncState=%d\n", state );
#endif /* LDAP_SYNC_TRACE */
		goto done;
	}

	rc = ls->ls_search_entry
		? ls->ls_search_entry( ls, res, &entryUUID, phase )
		: LDAP_SUCCESS;

done:;
	if ( ber != NULL ) {
		ber_free( ber, 1 );
	}

	if ( ctrls != NULL ) {
		ldap_controls_free( ctrls );
	}

	return rc;
}

/*
 * handle the LDAP_RES_SEARCH_REFERENCE response
 * (to be implemented yet)
 */
static int
ldap_sync_search_reference( ldap_sync_t *ls, LDAPMessage *res )
{
	int		rc = 0;

#ifdef LDAP_SYNC_TRACE
	fprintf( stderr, "\tgot LDAP_RES_SEARCH_REFERENCE\n" );
#endif /* LDAP_SYNC_TRACE */

	assert( ls != NULL );
	assert( res != NULL );

	if ( ls->ls_search_reference ) {
		rc = ls->ls_search_reference( ls, res );
	}

	return rc;
}

/*
 * handle the LDAP_RES_SEARCH_RESULT response
 */
static int
ldap_sync_search_result( ldap_sync_t *ls, LDAPMessage *res )
{
	int		err;
	char		*matched = NULL,
			*msg = NULL;
	LDAPControl	**ctrls = NULL;
	int		rc;
	int		refreshDeletes = -1;

#ifdef LDAP_SYNC_TRACE
	fprintf( stderr, "\tgot LDAP_RES_SEARCH_RESULT\n" );
#endif /* LDAP_SYNC_TRACE */

	assert( ls != NULL );
	assert( res != NULL );

	/* should not happen in refreshAndPersist... */
	rc = ldap_parse_result( ls->ls_ld,
		res, &err, &matched, &msg, NULL, &ctrls, 0 );
#ifdef LDAP_SYNC_TRACE
	fprintf( stderr,
		"\tldap_parse_result(%d, \"%s\", \"%s\") == %d\n",
		err,
		matched ? matched : "",
		msg ? msg : "",
		rc );
#endif /* LDAP_SYNC_TRACE */
	if ( rc == LDAP_SUCCESS ) {
		rc = err;
	}

	ls->ls_refreshPhase = LDAP_SYNC_CAPI_DONE;

	switch ( rc ) {
	case LDAP_SUCCESS: {
		int		i;
		BerElement	*ber = NULL;
		ber_len_t	len;
		struct berval	cookie = { 0 };

		rc = LDAP_OTHER;

		/* deal with control; then fallthru to handler */
		if ( ctrls == NULL ) {
			goto done;
		}

		/* lookup the sync state control */
		for ( i = 0; ctrls[ i ] != NULL; i++ ) {
			if ( strcmp( ctrls[ i ]->ldctl_oid,
				LDAP_CONTROL_SYNC_DONE ) == 0 )
			{
				break;
			}
		}

		/* control must be present; there might be other... */
		if ( ctrls[ i ] == NULL ) {
			goto done;
		}

		/* extract data */
		ber = ber_init( &ctrls[ i ]->ldctl_value );
		if ( ber == NULL ) {
			goto done;
		}

		if ( ber_scanf( ber, "{" /*"}"*/) == LBER_ERROR ) {
			goto ber_done;
		}
		if ( ber_peek_tag( ber, &len ) == LDAP_TAG_SYNC_COOKIE ) {
			if ( ber_scanf( ber, "m", &cookie ) == LBER_ERROR ) {
				goto ber_done;
			}
			if ( cookie.bv_val != NULL ) {
				ber_bvreplace( &ls->ls_cookie, &cookie );
			}
#ifdef LDAP_SYNC_TRACE
			fprintf( stderr, "\t\tgot cookie=%s\n",
				cookie.bv_val ? cookie.bv_val : "(null)" );
#endif /* LDAP_SYNC_TRACE */
		}

		refreshDeletes = 0;
		if ( ber_peek_tag( ber, &len ) == LDAP_TAG_REFRESHDELETES ) {
			if ( ber_scanf( ber, "b", &refreshDeletes ) == LBER_ERROR ) {
				goto ber_done;
			}
			if ( refreshDeletes ) {
				refreshDeletes = 1;
			}
		}

		if ( ber_scanf( ber, /*"{"*/ "}" ) != LBER_ERROR ) {
			rc = LDAP_SUCCESS;
		}

	ber_done:;
		ber_free( ber, 1 );
		if ( rc != LDAP_SUCCESS ) {
			break;
		}

#ifdef LDAP_SYNC_TRACE
		fprintf( stderr, "\t\tgot refreshDeletes=%s\n",
			refreshDeletes ? "TRUE" : "FALSE" );
#endif /* LDAP_SYNC_TRACE */

		/* FIXME: what should we do with the refreshDelete? */
		switch ( refreshDeletes ) {
		case 0:
			ls->ls_refreshPhase = LDAP_SYNC_CAPI_PRESENTS;
			break;

		default:
			ls->ls_refreshPhase = LDAP_SYNC_CAPI_DELETES;
			break;
		}

		} /* fallthru */

	case LDAP_SYNC_REFRESH_REQUIRED:
		/* TODO: check for Sync Done Control */
		/* FIXME: perhaps the handler should be called
		 * also in case of failure; we'll deal with this 
		 * later when implementing refreshOnly */
		if ( ls->ls_search_result ) {
			err = ls->ls_search_result( ls, res, refreshDeletes );
		}
		break;
	}

done:;
	if ( matched != NULL ) {
		ldap_memfree( matched );
	}

	if ( msg != NULL ) {
		ldap_memfree( msg );
	}

	if ( ctrls != NULL ) {
		ldap_controls_free( ctrls );
	}

	ls->ls_refreshPhase = LDAP_SYNC_CAPI_DONE;

	return rc;
}

/*
 * handle the LDAP_RES_INTERMEDIATE response
 */
static int
ldap_sync_search_intermediate( ldap_sync_t *ls, LDAPMessage *res, int *refreshDone )
{
	int			rc;
	char			*retoid = NULL;
        struct berval		*retdata = NULL;
	BerElement		*ber = NULL;
	ber_len_t		len;
	ber_tag_t		syncinfo_tag;
	struct berval		cookie;
	int			refreshDeletes = 0;
	BerVarray		syncUUIDs = NULL;
	ldap_sync_refresh_t	phase;

#ifdef LDAP_SYNC_TRACE
	fprintf( stderr, "\tgot LDAP_RES_INTERMEDIATE\n" );
#endif /* LDAP_SYNC_TRACE */

	assert( ls != NULL );
	assert( res != NULL );
	assert( refreshDone != NULL );

	*refreshDone = 0;

	rc = ldap_parse_intermediate( ls->ls_ld, res,
		&retoid, &retdata, NULL, 0 );
#ifdef LDAP_SYNC_TRACE
	fprintf( stderr, "\t%sldap_parse_intermediate(%s) == %d\n",
		rc != LDAP_SUCCESS ? "!!! " : "",
		retoid == NULL ? "\"\"" : retoid,
		rc );
#endif /* LDAP_SYNC_TRACE */
	/* parsing must be successful, and yield the OID
	 * of the sync info intermediate response */
	if ( rc != LDAP_SUCCESS ) {
		goto done;
	}

	rc = LDAP_OTHER;

	if ( retoid == NULL || strcmp( retoid, LDAP_SYNC_INFO ) != 0 ) {
		goto done;
	}

	/* init ber using the value in the response */
	ber = ber_init( retdata );
	if ( ber == NULL ) {
		goto done;
	}

	syncinfo_tag = ber_peek_tag( ber, &len );
	switch ( syncinfo_tag ) {
	case LDAP_TAG_SYNC_NEW_COOKIE:
		if ( ber_scanf( ber, "m", &cookie ) == LBER_ERROR ) {
			goto done;
		}
		if ( cookie.bv_val != NULL ) {
			ber_bvreplace( &ls->ls_cookie, &cookie );
		}
#ifdef LDAP_SYNC_TRACE
		fprintf( stderr, "\t\tgot cookie=%s\n",
			cookie.bv_val ? cookie.bv_val : "(null)" );
#endif /* LDAP_SYNC_TRACE */
		break;

	case LDAP_TAG_SYNC_REFRESH_DELETE:
	case LDAP_TAG_SYNC_REFRESH_PRESENT:
		if ( syncinfo_tag == LDAP_TAG_SYNC_REFRESH_DELETE ) {
#ifdef LDAP_SYNC_TRACE
			fprintf( stderr, "\t\tgot refreshDelete\n" );
#endif /* LDAP_SYNC_TRACE */
			switch ( ls->ls_refreshPhase ) {
			case LDAP_SYNC_CAPI_NONE:
			case LDAP_SYNC_CAPI_PRESENTS:
				ls->ls_refreshPhase = LDAP_SYNC_CAPI_DELETES;
				break;

			default:
				/* TODO: impossible; handle */
				goto done;
			}

		} else {
#ifdef LDAP_SYNC_TRACE
			fprintf( stderr, "\t\tgot refreshPresent\n" );
#endif /* LDAP_SYNC_TRACE */
			switch ( ls->ls_refreshPhase ) {
			case LDAP_SYNC_CAPI_NONE:
				ls->ls_refreshPhase = LDAP_SYNC_CAPI_PRESENTS;
				break;

			default:
				/* TODO: impossible; handle */
				goto done;
			}
		}

		if ( ber_scanf( ber, "{" /*"}"*/ ) == LBER_ERROR ) {
			goto done;
		}
		if ( ber_peek_tag( ber, &len ) == LDAP_TAG_SYNC_COOKIE ) {
			if ( ber_scanf( ber, "m", &cookie ) == LBER_ERROR ) {
				goto done;
			}
			if ( cookie.bv_val != NULL ) {
				ber_bvreplace( &ls->ls_cookie, &cookie );
			}
#ifdef LDAP_SYNC_TRACE
			fprintf( stderr, "\t\tgot cookie=%s\n",
				cookie.bv_val ? cookie.bv_val : "(null)" );
#endif /* LDAP_SYNC_TRACE */
		}

		*refreshDone = 1;
		if ( ber_peek_tag( ber, &len ) == LDAP_TAG_REFRESHDONE ) {
			if ( ber_scanf( ber, "b", refreshDone ) == LBER_ERROR ) {
				goto done;
			}
		}

#ifdef LDAP_SYNC_TRACE
		fprintf( stderr, "\t\tgot refreshDone=%s\n",
			*refreshDone ? "TRUE" : "FALSE" );
#endif /* LDAP_SYNC_TRACE */

		if ( ber_scanf( ber, /*"{"*/ "}" ) == LBER_ERROR ) {
			goto done;
		}

		if ( *refreshDone ) {
			ls->ls_refreshPhase = LDAP_SYNC_CAPI_DONE;
		}

		if ( ls->ls_intermediate ) {
			ls->ls_intermediate( ls, res, NULL, ls->ls_refreshPhase );
		}

		break;

	case LDAP_TAG_SYNC_ID_SET:
#ifdef LDAP_SYNC_TRACE
		fprintf( stderr, "\t\tgot syncIdSet\n" );
#endif /* LDAP_SYNC_TRACE */
		if ( ber_scanf( ber, "{" /*"}"*/ ) == LBER_ERROR ) {
			goto done;
		}
		if ( ber_peek_tag( ber, &len ) == LDAP_TAG_SYNC_COOKIE ) {
			if ( ber_scanf( ber, "m", &cookie ) == LBER_ERROR ) {
				goto done;
			}
			if ( cookie.bv_val != NULL ) {
				ber_bvreplace( &ls->ls_cookie, &cookie );
			}
#ifdef LDAP_SYNC_TRACE
			fprintf( stderr, "\t\tgot cookie=%s\n",
				cookie.bv_val ? cookie.bv_val : "(null)" );
#endif /* LDAP_SYNC_TRACE */
		}

		if ( ber_peek_tag( ber, &len ) == LDAP_TAG_REFRESHDELETES ) {
			if ( ber_scanf( ber, "b", &refreshDeletes ) == LBER_ERROR ) {
				goto done;
			}
		}

		if ( ber_scanf( ber, /*"{"*/ "[W]}", &syncUUIDs ) == LBER_ERROR
			|| syncUUIDs == NULL )
		{
			goto done;
		}

#ifdef LDAP_SYNC_TRACE
		{
			int	i;

			fprintf( stderr, "\t\tgot refreshDeletes=%s\n",
				refreshDeletes ? "TRUE" : "FALSE" );
			for ( i = 0; syncUUIDs[ i ].bv_val != NULL; i++ ) {
				char	buf[ BUFSIZ ];
				fprintf( stderr, "\t\t%s\n", 
					lutil_uuidstr_from_normalized(
						syncUUIDs[ i ].bv_val, syncUUIDs[ i ].bv_len,
						buf, sizeof( buf ) ) );
			}
		}
#endif /* LDAP_SYNC_TRACE */

		if ( refreshDeletes ) {
			phase = LDAP_SYNC_CAPI_DELETES_IDSET;

		} else {
			phase = LDAP_SYNC_CAPI_PRESENTS_IDSET;
		}

		/* FIXME: should touch ls->ls_refreshPhase? */
		if ( ls->ls_intermediate ) {
			ls->ls_intermediate( ls, res, syncUUIDs, phase );
		}

		ber_bvarray_free( syncUUIDs );
		break;

	default:
#ifdef LDAP_SYNC_TRACE
		fprintf( stderr, "\t\tunknown tag!\n" );
#endif /* LDAP_SYNC_TRACE */
		goto done;
	}

	rc = LDAP_SUCCESS;

done:;
	if ( ber != NULL ) {
		ber_free( ber, 1 );
	}

	if ( retoid != NULL ) {
		ldap_memfree( retoid );
	}

	if ( retdata != NULL ) {
		ber_bvfree( retdata );
	}

	return rc;
}

/*
 * initialize the sync
 */
int
ldap_sync_init( ldap_sync_t *ls, int mode )
{
	LDAPControl	ctrl = { 0 },
			*ctrls[ 2 ];
	BerElement	*ber = NULL;
	int		rc;
	struct timeval	tv = { 0 },
			*tvp = NULL;
	LDAPMessage	*res = NULL;

#ifdef LDAP_SYNC_TRACE
	fprintf( stderr, "ldap_sync_init(%s)...\n",
		mode == LDAP_SYNC_REFRESH_AND_PERSIST ?
			"LDAP_SYNC_REFRESH_AND_PERSIST" :
			( mode == LDAP_SYNC_REFRESH_ONLY ? 
				"LDAP_SYNC_REFRESH_ONLY" : "unknown" ) );
#endif /* LDAP_SYNC_TRACE */

	assert( ls != NULL );
	assert( ls->ls_ld != NULL );

	/* support both refreshOnly and refreshAndPersist */
	switch ( mode ) {
	case LDAP_SYNC_REFRESH_AND_PERSIST:
	case LDAP_SYNC_REFRESH_ONLY:
		break;

	default:
		fprintf( stderr, "ldap_sync_init: unknown mode=%d\n", mode );
		return LDAP_PARAM_ERROR;
	}

	/* check consistency of cookie and reloadHint at initial refresh */
	if ( ls->ls_cookie.bv_val == NULL && ls->ls_reloadHint != 0 ) {
		fprintf( stderr, "ldap_sync_init: inconsistent cookie/rhint\n" );
		return LDAP_PARAM_ERROR;
	}

	ctrls[ 0 ] = &ctrl;
	ctrls[ 1 ] = NULL;

	/* prepare the Sync Request control */
	ber = ber_alloc_t( LBER_USE_DER );
#ifdef LDAP_SYNC_TRACE
	fprintf( stderr, "%sber_alloc_t() %s= NULL\n",
		ber == NULL ? "!!! " : "",
		ber == NULL ? "=" : "!" );
#endif /* LDAP_SYNC_TRACE */
	if ( ber == NULL ) {
		rc = LDAP_NO_MEMORY;
		goto done;
	}

	ls->ls_refreshPhase = LDAP_SYNC_CAPI_NONE;

	if ( ls->ls_cookie.bv_val != NULL ) {
		ber_printf( ber, "{eOb}", mode,
			&ls->ls_cookie, ls->ls_reloadHint );

	} else {
		ber_printf( ber, "{eb}", mode, ls->ls_reloadHint );
	}

	rc = ber_flatten2( ber, &ctrl.ldctl_value, 0 );
#ifdef LDAP_SYNC_TRACE
	fprintf( stderr,
		"%sber_flatten2() == %d\n",
		rc ? "!!! " : "",
		rc );
#endif /* LDAP_SYNC_TRACE */
	if ( rc < 0 ) {
		rc = LDAP_OTHER;
                goto done;
        }

	/* make the control critical, as we cannot proceed without */
	ctrl.ldctl_oid = LDAP_CONTROL_SYNC;
	ctrl.ldctl_iscritical = 1;

	/* timelimit? */
	if ( ls->ls_timelimit ) {
		tv.tv_sec = ls->ls_timelimit;
		tvp = &tv;
	}

	/* actually run the search */
	rc = ldap_search_ext( ls->ls_ld,
		ls->ls_base, ls->ls_scope, ls->ls_filter,
		ls->ls_attrs, 0, ctrls, NULL,
		tvp, ls->ls_sizelimit, &ls->ls_msgid );
#ifdef LDAP_SYNC_TRACE
	fprintf( stderr,
		"%sldap_search_ext(\"%s\", %d, \"%s\") == %d\n",
		rc ? "!!! " : "",
		ls->ls_base, ls->ls_scope, ls->ls_filter, rc );
#endif /* LDAP_SYNC_TRACE */
	if ( rc != LDAP_SUCCESS ) {
		goto done;
	}

	/* initial content/content update phase */
	for ( ; ; ) {
		LDAPMessage	*msg = NULL;

		/* NOTE: this very short timeout is just to let
		 * ldap_result() yield long enough to get something */
		tv.tv_sec = 0;
		tv.tv_usec = 100000;

		rc = ldap_result( ls->ls_ld, ls->ls_msgid,
			LDAP_MSG_RECEIVED, &tv, &res );
#ifdef LDAP_SYNC_TRACE
		fprintf( stderr,
			"\t%sldap_result(%d) == %d\n",
			rc == -1 ? "!!! " : "",
			ls->ls_msgid, rc );
#endif /* LDAP_SYNC_TRACE */
		switch ( rc ) {
		case 0:
			/*
			 * timeout
			 *
			 * TODO: can do something else in the meanwhile)
			 */
			break;

		case -1:
			/* smtg bad! */
			goto done;

		default:
			for ( msg = ldap_first_message( ls->ls_ld, res );
				msg != NULL;
				msg = ldap_next_message( ls->ls_ld, msg ) )
			{
				int	refreshDone;

				switch ( ldap_msgtype( msg ) ) {
				case LDAP_RES_SEARCH_ENTRY:
					rc = ldap_sync_search_entry( ls, res );
					break;

				case LDAP_RES_SEARCH_REFERENCE:
					rc = ldap_sync_search_reference( ls, res );
					break;

				case LDAP_RES_SEARCH_RESULT:
					rc = ldap_sync_search_result( ls, res );
					goto done_search;

				case LDAP_RES_INTERMEDIATE:
					rc = ldap_sync_search_intermediate( ls, res, &refreshDone );
					if ( rc != LDAP_SUCCESS || refreshDone ) {
						goto done_search;
					}
					break;

				default:
#ifdef LDAP_SYNC_TRACE
					fprintf( stderr, "\tgot something unexpected...\n" );
#endif /* LDAP_SYNC_TRACE */

					ldap_msgfree( res );

					rc = LDAP_OTHER;
					goto done;
				}
			}
			ldap_msgfree( res );
			res = NULL;
			break;
		}
	}

done_search:;
	ldap_msgfree( res );

done:;
	if ( ber != NULL ) {
		ber_free( ber, 1 );
	}

	return rc;
}

/*
 * initialize the refreshOnly sync
 */
int
ldap_sync_init_refresh_only( ldap_sync_t *ls )
{
	return ldap_sync_init( ls, LDAP_SYNC_REFRESH_ONLY );
}

/*
 * initialize the refreshAndPersist sync
 */
int
ldap_sync_init_refresh_and_persist( ldap_sync_t *ls )
{
	return ldap_sync_init( ls, LDAP_SYNC_REFRESH_AND_PERSIST );
}

/*
 * poll for new responses
 */
int
ldap_sync_poll( ldap_sync_t *ls )
{
	struct	timeval		tv,
				*tvp = NULL;
	LDAPMessage		*res = NULL,
				*msg;
	int			rc = 0;

#ifdef LDAP_SYNC_TRACE
	fprintf( stderr, "ldap_sync_poll...\n" );
#endif /* LDAP_SYNC_TRACE */

	assert( ls != NULL );
	assert( ls->ls_ld != NULL );

	if ( ls->ls_timeout != -1 ) {
		tv.tv_sec = ls->ls_timeout;
		tv.tv_usec = 0;
		tvp = &tv;
	}

	rc = ldap_result( ls->ls_ld, ls->ls_msgid,
		LDAP_MSG_RECEIVED, tvp, &res );
	if ( rc <= 0 ) {
		return rc;
	}

	for ( msg = ldap_first_message( ls->ls_ld, res );
		msg;
		msg = ldap_next_message( ls->ls_ld, msg ) )
	{
		int	refreshDone;

		switch ( ldap_msgtype( msg ) ) {
		case LDAP_RES_SEARCH_ENTRY:
			rc = ldap_sync_search_entry( ls, res );
			break;

		case LDAP_RES_SEARCH_REFERENCE:
			rc = ldap_sync_search_reference( ls, res );
			break;

		case LDAP_RES_SEARCH_RESULT:
			rc = ldap_sync_search_result( ls, res );
			goto done_search;

		case LDAP_RES_INTERMEDIATE:
			rc = ldap_sync_search_intermediate( ls, res, &refreshDone );
			if ( rc != LDAP_SUCCESS || refreshDone ) {
				goto done_search;
			}
			break;

		default:
#ifdef LDAP_SYNC_TRACE
			fprintf( stderr, "\tgot something unexpected...\n" );
#endif /* LDAP_SYNC_TRACE */

			ldap_msgfree( res );

			rc = LDAP_OTHER;
			goto done;
		}
	}

done_search:;
	ldap_msgfree( res );

done:;
	return rc;
}
