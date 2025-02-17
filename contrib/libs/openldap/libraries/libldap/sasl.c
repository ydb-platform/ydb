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

/*
 *	BindRequest ::= SEQUENCE {
 *		version		INTEGER,
 *		name		DistinguishedName,	 -- who
 *		authentication	CHOICE {
 *			simple		[0] OCTET STRING -- passwd
 *			krbv42ldap	[1] OCTET STRING -- OBSOLETE
 *			krbv42dsa	[2] OCTET STRING -- OBSOLETE
 *			sasl		[3] SaslCredentials	-- LDAPv3
 *		}
 *	}
 *
 *	BindResponse ::= SEQUENCE {
 *		COMPONENTS OF LDAPResult,
 *		serverSaslCreds		OCTET STRING OPTIONAL -- LDAPv3
 *	}
 *
 */

#include "portable.h"

#include <stdio.h>

#include <ac/socket.h>
#include <ac/stdlib.h>
#include <ac/string.h>
#include <ac/time.h>
#include <ac/errno.h>

#include "ldap-int.h"

BerElement *
ldap_build_bind_req(
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAP_CONST char	*mechanism,
	struct berval	*cred,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	ber_int_t		*msgidp )
{
	BerElement	*ber;
	int rc;

	if( mechanism == LDAP_SASL_SIMPLE ) {
		if( dn == NULL && cred != NULL && cred->bv_len ) {
			/* use default binddn */
			dn = ld->ld_defbinddn;
		}

	} else if( ld->ld_version < LDAP_VERSION3 ) {
		ld->ld_errno = LDAP_NOT_SUPPORTED;
		return( NULL );
	}

	if ( dn == NULL ) {
		dn = "";
	}

	/* create a message to send */
	if ( (ber = ldap_alloc_ber_with_options( ld )) == NULL ) {
		return( NULL );
	}

	LDAP_NEXT_MSGID( ld, *msgidp );
	if( mechanism == LDAP_SASL_SIMPLE ) {
		/* simple bind */
		rc = ber_printf( ber, "{it{istON}" /*}*/,
			*msgidp, LDAP_REQ_BIND,
			ld->ld_version, dn, LDAP_AUTH_SIMPLE,
			cred );
		
	} else if ( cred == NULL || cred->bv_val == NULL ) {
		/* SASL bind w/o credentials */
		rc = ber_printf( ber, "{it{ist{sN}N}" /*}*/,
			*msgidp, LDAP_REQ_BIND,
			ld->ld_version, dn, LDAP_AUTH_SASL,
			mechanism );

	} else {
		/* SASL bind w/ credentials */
		rc = ber_printf( ber, "{it{ist{sON}N}" /*}*/,
			*msgidp, LDAP_REQ_BIND,
			ld->ld_version, dn, LDAP_AUTH_SASL,
			mechanism, cred );
	}

	if( rc == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( NULL );
	}

	/* Put Server Controls */
	if( ldap_int_put_controls( ld, sctrls, ber ) != LDAP_SUCCESS ) {
		ber_free( ber, 1 );
		return( NULL );
	}

	if ( ber_printf( ber, /*{*/ "N}" ) == -1 ) {
		ld->ld_errno = LDAP_ENCODING_ERROR;
		ber_free( ber, 1 );
		return( NULL );
	}

	return( ber );
}

/*
 * ldap_sasl_bind - bind to the ldap server (and X.500).
 * The dn (usually NULL), mechanism, and credentials are provided.
 * The message id of the request initiated is provided upon successful
 * (LDAP_SUCCESS) return.
 *
 * Example:
 *	ldap_sasl_bind( ld, NULL, "mechanism",
 *		cred, NULL, NULL, &msgid )
 */

int
ldap_sasl_bind(
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAP_CONST char	*mechanism,
	struct berval	*cred,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	int				*msgidp )
{
	BerElement	*ber;
	int rc;
	ber_int_t id;

	Debug0( LDAP_DEBUG_TRACE, "ldap_sasl_bind\n" );

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( msgidp != NULL );

	/* check client controls */
	rc = ldap_int_client_controls( ld, cctrls );
	if( rc != LDAP_SUCCESS ) return rc;

	ber = ldap_build_bind_req( ld, dn, mechanism, cred, sctrls, cctrls, &id );
	if( !ber )
		return ld->ld_errno;

	/* send the message */
	*msgidp = ldap_send_initial_request( ld, LDAP_REQ_BIND, dn, ber, id );

	if(*msgidp < 0)
		return ld->ld_errno;

	return LDAP_SUCCESS;
}


int
ldap_sasl_bind_s(
	LDAP			*ld,
	LDAP_CONST char	*dn,
	LDAP_CONST char	*mechanism,
	struct berval	*cred,
	LDAPControl		**sctrls,
	LDAPControl		**cctrls,
	struct berval	**servercredp )
{
	int	rc, msgid;
	LDAPMessage	*result;
	struct berval	*scredp = NULL;

	Debug0( LDAP_DEBUG_TRACE, "ldap_sasl_bind_s\n" );

	/* do a quick !LDAPv3 check... ldap_sasl_bind will do the rest. */
	if( servercredp != NULL ) {
		if (ld->ld_version < LDAP_VERSION3) {
			ld->ld_errno = LDAP_NOT_SUPPORTED;
			return ld->ld_errno;
		}
		*servercredp = NULL;
	}

	rc = ldap_sasl_bind( ld, dn, mechanism, cred, sctrls, cctrls, &msgid );

	if ( rc != LDAP_SUCCESS ) {
		return( rc );
	}

#ifdef LDAP_CONNECTIONLESS
	if (LDAP_IS_UDP(ld)) {
		return( rc );
	}
#endif

	if ( ldap_result( ld, msgid, LDAP_MSG_ALL, NULL, &result ) == -1 || !result ) {
		return( ld->ld_errno );	/* ldap_result sets ld_errno */
	}

	/* parse the results */
	scredp = NULL;
	if( servercredp != NULL ) {
		rc = ldap_parse_sasl_bind_result( ld, result, &scredp, 0 );
	}

	if ( rc != LDAP_SUCCESS ) {
		ldap_msgfree( result );
		return( rc );
	}

	rc = ldap_result2error( ld, result, 1 );

	if ( rc == LDAP_SUCCESS || rc == LDAP_SASL_BIND_IN_PROGRESS ) {
		if( servercredp != NULL ) {
			*servercredp = scredp;
			scredp = NULL;
		}
	}

	if ( scredp != NULL ) {
		ber_bvfree(scredp);
	}

	return rc;
}


/*
* Parse BindResponse:
*
*   BindResponse ::= [APPLICATION 1] SEQUENCE {
*     COMPONENTS OF LDAPResult,
*     serverSaslCreds  [7] OCTET STRING OPTIONAL }
*
*   LDAPResult ::= SEQUENCE {
*     resultCode      ENUMERATED,
*     matchedDN       LDAPDN,
*     errorMessage    LDAPString,
*     referral        [3] Referral OPTIONAL }
*/

int
ldap_parse_sasl_bind_result(
	LDAP			*ld,
	LDAPMessage		*res,
	struct berval	**servercredp,
	int				freeit )
{
	ber_int_t errcode;
	struct berval* scred;

	ber_tag_t tag;
	BerElement	*ber;

	Debug0( LDAP_DEBUG_TRACE, "ldap_parse_sasl_bind_result\n" );

	assert( ld != NULL );
	assert( LDAP_VALID( ld ) );
	assert( res != NULL );

	if( servercredp != NULL ) {
		if( ld->ld_version < LDAP_VERSION2 ) {
			return LDAP_NOT_SUPPORTED;
		}
		*servercredp = NULL;
	}

	if( res->lm_msgtype != LDAP_RES_BIND ) {
		ld->ld_errno = LDAP_PARAM_ERROR;
		return ld->ld_errno;
	}

	scred = NULL;

	if ( ld->ld_error ) {
		LDAP_FREE( ld->ld_error );
		ld->ld_error = NULL;
	}
	if ( ld->ld_matched ) {
		LDAP_FREE( ld->ld_matched );
		ld->ld_matched = NULL;
	}

	/* parse results */

	ber = ber_dup( res->lm_ber );

	if( ber == NULL ) {
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	}

	if ( ld->ld_version < LDAP_VERSION2 ) {
		tag = ber_scanf( ber, "{iA}",
			&errcode, &ld->ld_error );

		if( tag == LBER_ERROR ) {
			ber_free( ber, 0 );
			ld->ld_errno = LDAP_DECODING_ERROR;
			return ld->ld_errno;
		}

	} else {
		ber_len_t len;

		tag = ber_scanf( ber, "{eAA" /*}*/,
			&errcode, &ld->ld_matched, &ld->ld_error );

		if( tag == LBER_ERROR ) {
			ber_free( ber, 0 );
			ld->ld_errno = LDAP_DECODING_ERROR;
			return ld->ld_errno;
		}

		tag = ber_peek_tag(ber, &len);

		if( tag == LDAP_TAG_REFERRAL ) {
			/* skip 'em */
			if( ber_scanf( ber, "x" ) == LBER_ERROR ) {
				ber_free( ber, 0 );
				ld->ld_errno = LDAP_DECODING_ERROR;
				return ld->ld_errno;
			}

			tag = ber_peek_tag(ber, &len);
		}

		if( tag == LDAP_TAG_SASL_RES_CREDS ) {
			if( ber_scanf( ber, "O", &scred ) == LBER_ERROR ) {
				ber_free( ber, 0 );
				ld->ld_errno = LDAP_DECODING_ERROR;
				return ld->ld_errno;
			}
		}
	}

	ber_free( ber, 0 );

	if ( servercredp != NULL ) {
		*servercredp = scred;

	} else if ( scred != NULL ) {
		ber_bvfree( scred );
	}

	ld->ld_errno = errcode;

	if ( freeit ) {
		ldap_msgfree( res );
	}

	return( LDAP_SUCCESS );
}

int
ldap_pvt_sasl_getmechs ( LDAP *ld, char **pmechlist )
{
	/* we need to query the server for supported mechs anyway */
	LDAPMessage *res, *e;
	char *attrs[] = { "supportedSASLMechanisms", NULL };
	char **values, *mechlist;
	int rc;

	Debug0( LDAP_DEBUG_TRACE, "ldap_pvt_sasl_getmech\n" );

	rc = ldap_search_s( ld, "", LDAP_SCOPE_BASE,
		NULL, attrs, 0, &res );

	if ( rc != LDAP_SUCCESS ) {
		return ld->ld_errno;
	}
		
	e = ldap_first_entry( ld, res );
	if ( e == NULL ) {
		ldap_msgfree( res );
		if ( ld->ld_errno == LDAP_SUCCESS ) {
			ld->ld_errno = LDAP_NO_SUCH_OBJECT;
		}
		return ld->ld_errno;
	}

	values = ldap_get_values( ld, e, "supportedSASLMechanisms" );
	if ( values == NULL ) {
		ldap_msgfree( res );
		ld->ld_errno = LDAP_NO_SUCH_ATTRIBUTE;
		return ld->ld_errno;
	}

	mechlist = ldap_charray2str( values, " " );
	if ( mechlist == NULL ) {
		LDAP_VFREE( values );
		ldap_msgfree( res );
		ld->ld_errno = LDAP_NO_MEMORY;
		return ld->ld_errno;
	} 

	LDAP_VFREE( values );
	ldap_msgfree( res );

	*pmechlist = mechlist;

	return LDAP_SUCCESS;
}

/*
 * ldap_sasl_interactive_bind - interactive SASL authentication
 *
 * This routine uses interactive callbacks.
 *
 * LDAP_SUCCESS is returned upon success, the ldap error code
 * otherwise. LDAP_SASL_BIND_IN_PROGRESS is returned if further
 * calls are needed.
 */
int
ldap_sasl_interactive_bind(
	LDAP *ld,
	LDAP_CONST char *dn, /* usually NULL */
	LDAP_CONST char *mechs,
	LDAPControl **serverControls,
	LDAPControl **clientControls,
	unsigned flags,
	LDAP_SASL_INTERACT_PROC *interact,
	void *defaults,
	LDAPMessage *result,
	const char **rmech,
	int *msgid )
{
	char *smechs = NULL;
	int rc;

#ifdef LDAP_CONNECTIONLESS
	if( LDAP_IS_UDP(ld) ) {
		/* Just force it to simple bind, silly to make the user
		 * ask all the time. No, we don't ever actually bind, but I'll
		 * let the final bind handler take care of saving the cdn.
		 */
		rc = ldap_simple_bind( ld, dn, NULL );
		rc = rc < 0 ? rc : 0;
		goto done;
	} else
#endif

	/* First time */
	if ( !result ) {

#ifdef HAVE_CYRUS_SASL
	if( mechs == NULL || *mechs == '\0' ) {
		mechs = ld->ld_options.ldo_def_sasl_mech;
	}
#endif
		
	if( mechs == NULL || *mechs == '\0' ) {
		/* FIXME: this needs to be asynchronous too;
		 * perhaps NULL should be disallowed for async usage?
		 */
		rc = ldap_pvt_sasl_getmechs( ld, &smechs );
		if( rc != LDAP_SUCCESS ) {
			goto done;
		}

		Debug1( LDAP_DEBUG_TRACE,
			"ldap_sasl_interactive_bind: server supports: %s\n",
			smechs );

		mechs = smechs;

	} else {
		Debug1( LDAP_DEBUG_TRACE,
			"ldap_sasl_interactive_bind: user selected: %s\n",
			mechs );
	}
	}
	rc = ldap_int_sasl_bind( ld, dn, mechs,
		serverControls, clientControls,
		flags, interact, defaults, result, rmech, msgid );

done:
	if ( smechs ) LDAP_FREE( smechs );

	return rc;
}

/*
 * ldap_sasl_interactive_bind_s - interactive SASL authentication
 *
 * This routine uses interactive callbacks.
 *
 * LDAP_SUCCESS is returned upon success, the ldap error code
 * otherwise.
 */
int
ldap_sasl_interactive_bind_s(
	LDAP *ld,
	LDAP_CONST char *dn, /* usually NULL */
	LDAP_CONST char *mechs,
	LDAPControl **serverControls,
	LDAPControl **clientControls,
	unsigned flags,
	LDAP_SASL_INTERACT_PROC *interact,
	void *defaults )
{
	const char *rmech = NULL;
	LDAPMessage *result = NULL;
	int rc, msgid;

	do {
		rc = ldap_sasl_interactive_bind( ld, dn, mechs,
			serverControls, clientControls,
			flags, interact, defaults, result, &rmech, &msgid );

		ldap_msgfree( result );

		if ( rc != LDAP_SASL_BIND_IN_PROGRESS )
			break;

#ifdef LDAP_CONNECTIONLESS
		if (LDAP_IS_UDP(ld)) {
			break;
		}
#endif

		if ( ldap_result( ld, msgid, LDAP_MSG_ALL, NULL, &result ) == -1 || !result ) {
			return( ld->ld_errno );	/* ldap_result sets ld_errno */
		}
	} while ( rc == LDAP_SASL_BIND_IN_PROGRESS );

	return rc;
}

#ifdef HAVE_CYRUS_SASL

#ifdef HAVE_SASL_SASL_H
#include <sasl/sasl.h>
#else
#include <sasl.h>
#endif

#endif /* HAVE_CYRUS_SASL */

static int
sb_sasl_generic_remove( Sockbuf_IO_Desc *sbiod );

static int
sb_sasl_generic_setup( Sockbuf_IO_Desc *sbiod, void *arg )
{
	struct sb_sasl_generic_data	*p;
	struct sb_sasl_generic_install	*i;

	assert( sbiod != NULL );

	i = (struct sb_sasl_generic_install *)arg;

	p = LBER_MALLOC( sizeof( *p ) );
	if ( p == NULL )
		return -1;
	p->ops = i->ops;
	p->ops_private = i->ops_private;
	p->sbiod = sbiod;
	p->flags = 0;
	ber_pvt_sb_buf_init( &p->sec_buf_in );
	ber_pvt_sb_buf_init( &p->buf_in );
	ber_pvt_sb_buf_init( &p->buf_out );

	sbiod->sbiod_pvt = p;

	p->ops->init( p, &p->min_send, &p->max_send, &p->max_recv );

	if ( ber_pvt_sb_grow_buffer( &p->sec_buf_in, p->min_send ) < 0 ) {
		sb_sasl_generic_remove( sbiod );
		sock_errset(ENOMEM);
		return -1;
	}

	return 0;
}

static int
sb_sasl_generic_remove( Sockbuf_IO_Desc *sbiod )
{
	struct sb_sasl_generic_data	*p;

	assert( sbiod != NULL );

	p = (struct sb_sasl_generic_data *)sbiod->sbiod_pvt;

	p->ops->fini(p);

	ber_pvt_sb_buf_destroy( &p->sec_buf_in );
	ber_pvt_sb_buf_destroy( &p->buf_in );
	ber_pvt_sb_buf_destroy( &p->buf_out );
	LBER_FREE( p );
	sbiod->sbiod_pvt = NULL;
	return 0;
}

static ber_len_t
sb_sasl_generic_pkt_length(
	struct sb_sasl_generic_data *p,
	const unsigned char *buf,
	int debuglevel )
{
	ber_len_t		size;

	assert( buf != NULL );

	size = buf[0] << 24
		| buf[1] << 16
		| buf[2] << 8
		| buf[3];

	if ( size > p->max_recv ) {
		/* somebody is trying to mess me up. */
		ber_log_printf( LDAP_DEBUG_ANY, debuglevel,
			"sb_sasl_generic_pkt_length: "
			"received illegal packet length of %lu bytes\n",
			(unsigned long)size );
		size = 16; /* this should lead to an error. */
	}

	return size + 4; /* include the size !!! */
}

/* Drop a processed packet from the input buffer */
static void
sb_sasl_generic_drop_packet (
	struct sb_sasl_generic_data *p,
	int debuglevel )
{
	ber_slen_t			len;

	len = p->sec_buf_in.buf_ptr - p->sec_buf_in.buf_end;
	if ( len > 0 )
		AC_MEMCPY( p->sec_buf_in.buf_base, p->sec_buf_in.buf_base +
			p->sec_buf_in.buf_end, len );

	if ( len >= 4 ) {
		p->sec_buf_in.buf_end = sb_sasl_generic_pkt_length(p,
			(unsigned char *) p->sec_buf_in.buf_base, debuglevel);
	}
	else {
		p->sec_buf_in.buf_end = 0;
	}
	p->sec_buf_in.buf_ptr = len;
}

static ber_slen_t
sb_sasl_generic_read( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len)
{
	struct sb_sasl_generic_data	*p;
	ber_slen_t			ret, bufptr;

	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

	p = (struct sb_sasl_generic_data *)sbiod->sbiod_pvt;

	/* Are there anything left in the buffer? */
	ret = ber_pvt_sb_copy_out( &p->buf_in, buf, len );
	bufptr = ret;
	len -= ret;

	if ( len == 0 )
		return bufptr;

	p->ops->reset_buf( p, &p->buf_in );

	/* Read the length of the packet */
	while ( p->sec_buf_in.buf_ptr < 4 ) {
		ret = LBER_SBIOD_READ_NEXT( sbiod, p->sec_buf_in.buf_base +
			p->sec_buf_in.buf_ptr,
			4 - p->sec_buf_in.buf_ptr );
#ifdef EINTR
		if ( ( ret < 0 ) && ( errno == EINTR ) )
			continue;
#endif
		if ( ret <= 0 )
			return bufptr ? bufptr : ret;

		p->sec_buf_in.buf_ptr += ret;
	}

	/* The new packet always starts at p->sec_buf_in.buf_base */
	ret = sb_sasl_generic_pkt_length(p, (unsigned char *) p->sec_buf_in.buf_base,
		sbiod->sbiod_sb->sb_debug );

	/* Grow the packet buffer if necessary */
	if ( ( p->sec_buf_in.buf_size < (ber_len_t) ret ) && 
		ber_pvt_sb_grow_buffer( &p->sec_buf_in, ret ) < 0 )
	{
		sock_errset(ENOMEM);
		return -1;
	}
	p->sec_buf_in.buf_end = ret;

	/* Did we read the whole encrypted packet? */
	while ( p->sec_buf_in.buf_ptr < p->sec_buf_in.buf_end ) {
		/* No, we have got only a part of it */
		ret = p->sec_buf_in.buf_end - p->sec_buf_in.buf_ptr;

		ret = LBER_SBIOD_READ_NEXT( sbiod, p->sec_buf_in.buf_base +
			p->sec_buf_in.buf_ptr, ret );
#ifdef EINTR
		if ( ( ret < 0 ) && ( errno == EINTR ) )
			continue;
#endif
		if ( ret <= 0 )
			return bufptr ? bufptr : ret;

		p->sec_buf_in.buf_ptr += ret;
   	}

	/* Decode the packet */
	ret = p->ops->decode( p, &p->sec_buf_in, &p->buf_in );

	/* Drop the packet from the input buffer */
	sb_sasl_generic_drop_packet( p, sbiod->sbiod_sb->sb_debug );

	if ( ret != 0 ) {
		ber_log_printf( LDAP_DEBUG_ANY, sbiod->sbiod_sb->sb_debug,
			"sb_sasl_generic_read: failed to decode packet\n" );
		sock_errset(EIO);
		return -1;
	}

	bufptr += ber_pvt_sb_copy_out( &p->buf_in, (char*) buf + bufptr, len );

	return bufptr;
}

static ber_slen_t
sb_sasl_generic_write( Sockbuf_IO_Desc *sbiod, void *buf, ber_len_t len)
{
	struct sb_sasl_generic_data	*p;
	int				ret;
	ber_len_t			len2;

	assert( sbiod != NULL );
	assert( SOCKBUF_VALID( sbiod->sbiod_sb ) );

	p = (struct sb_sasl_generic_data *)sbiod->sbiod_pvt;

	/* Is there anything left in the buffer? */
	if ( p->buf_out.buf_ptr != p->buf_out.buf_end ) {
		ret = ber_pvt_sb_do_write( sbiod, &p->buf_out );
		if ( ret < 0 ) return ret;

		/* Still have something left?? */
		if ( p->buf_out.buf_ptr != p->buf_out.buf_end ) {
			sock_errset(EAGAIN);
			return -1;
		}
	}

	len2 = p->max_send - 100;	/* For safety margin */
	len2 = len > len2 ? len2 : len;

	/* If we're just retrying a partial write, tell the
	 * caller it's done. Let them call again if there's
	 * still more left to write.
	 */
	if ( p->flags & LDAP_PVT_SASL_PARTIAL_WRITE ) {
		p->flags ^= LDAP_PVT_SASL_PARTIAL_WRITE;
		return len2;
	}

	/* now encode the next packet. */
	p->ops->reset_buf( p, &p->buf_out );

	ret = p->ops->encode( p, buf, len2, &p->buf_out );

	if ( ret != 0 ) {
		ber_log_printf( LDAP_DEBUG_ANY, sbiod->sbiod_sb->sb_debug,
			"sb_sasl_generic_write: failed to encode packet\n" );
		sock_errset(EIO);
		return -1;
	}

	ret = ber_pvt_sb_do_write( sbiod, &p->buf_out );

	if ( ret < 0 ) {
		/* error? */
		int err = sock_errno();
		/* caller can retry this */
		if ( err == EAGAIN || err == EWOULDBLOCK || err == EINTR )
			p->flags |= LDAP_PVT_SASL_PARTIAL_WRITE;
		return ret;
	} else if ( p->buf_out.buf_ptr != p->buf_out.buf_end ) {
		/* partial write? pretend nothing got written */
		p->flags |= LDAP_PVT_SASL_PARTIAL_WRITE;
		sock_errset(EAGAIN);
		len2 = -1;
	}

	/* return number of bytes encoded, not written, to ensure
	 * no byte is encoded twice (even if only sent once).
	 */
	return len2;
}

static int
sb_sasl_generic_ctrl( Sockbuf_IO_Desc *sbiod, int opt, void *arg )
{
	struct sb_sasl_generic_data	*p;

	p = (struct sb_sasl_generic_data *)sbiod->sbiod_pvt;

	if ( opt == LBER_SB_OPT_DATA_READY ) {
		if ( p->buf_in.buf_ptr != p->buf_in.buf_end ) return 1;
	}

	return LBER_SBIOD_CTRL_NEXT( sbiod, opt, arg );
}

Sockbuf_IO ldap_pvt_sockbuf_io_sasl_generic = {
	sb_sasl_generic_setup,		/* sbi_setup */
	sb_sasl_generic_remove,		/* sbi_remove */
	sb_sasl_generic_ctrl,		/* sbi_ctrl */
	sb_sasl_generic_read,		/* sbi_read */
	sb_sasl_generic_write,		/* sbi_write */
	NULL			/* sbi_close */
};

int ldap_pvt_sasl_generic_install(
	Sockbuf *sb,
	struct sb_sasl_generic_install *install_arg )
{
	Debug0( LDAP_DEBUG_TRACE, "ldap_pvt_sasl_generic_install\n" );

	/* don't install the stuff unless security has been negotiated */

	if ( !ber_sockbuf_ctrl( sb, LBER_SB_OPT_HAS_IO,
			&ldap_pvt_sockbuf_io_sasl_generic ) )
	{
#ifdef LDAP_DEBUG
		ber_sockbuf_add_io( sb, &ber_sockbuf_io_debug,
			LBER_SBIOD_LEVEL_APPLICATION, (void *)"sasl_generic_" );
#endif
		ber_sockbuf_add_io( sb, &ldap_pvt_sockbuf_io_sasl_generic,
			LBER_SBIOD_LEVEL_APPLICATION, install_arg );
	}

	return LDAP_SUCCESS;
}

void ldap_pvt_sasl_generic_remove( Sockbuf *sb )
{
	ber_sockbuf_remove_io( sb, &ldap_pvt_sockbuf_io_sasl_generic,
		LBER_SBIOD_LEVEL_APPLICATION );
#ifdef LDAP_DEBUG
	ber_sockbuf_remove_io( sb, &ber_sockbuf_io_debug,
		LBER_SBIOD_LEVEL_APPLICATION );
#endif
}
