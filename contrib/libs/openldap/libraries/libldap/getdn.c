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
/* Portions Copyright (c) 1994 Regents of the University of Michigan.
 * All rights reserved.
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>
#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"
#include "ldap_schema.h"
#include "ldif.h"

/* extension to UFN that turns trailing "dc=value" rdns in DNS style,
 * e.g. "ou=People,dc=openldap,dc=org" => "People, openldap.org" */
#define DC_IN_UFN

/* parsing/printing routines */
static int str2strval( const char *str, ber_len_t stoplen, struct berval *val, 
		const char **next, unsigned flags, int *retFlags, void *ctx );
static int DCE2strval( const char *str, struct berval *val, 
		const char **next, unsigned flags, void *ctx );
static int IA52strval( const char *str, struct berval *val, 
		const char **next, unsigned flags, void *ctx );
static int quotedIA52strval( const char *str, struct berval *val, 
		const char **next, unsigned flags, void *ctx );
static int hexstr2binval( const char *str, struct berval *val, 
		const char **next, unsigned flags, void *ctx );
static int hexstr2bin( const char *str, char *c );
static int byte2hexpair( const char *val, char *pair );
static int binval2hexstr( struct berval *val, char *str );
static int strval2strlen( struct berval *val, unsigned flags, 
		ber_len_t *len );
static int strval2str( struct berval *val, char *str, unsigned flags, 
		ber_len_t *len );
static int strval2IA5strlen( struct berval *val, unsigned flags,
		ber_len_t *len );
static int strval2IA5str( struct berval *val, char *str, unsigned flags, 
		ber_len_t *len );
static int strval2DCEstrlen( struct berval *val, unsigned flags,
		ber_len_t *len );
static int strval2DCEstr( struct berval *val, char *str, unsigned flags, 
		ber_len_t *len );
static int strval2ADstrlen( struct berval *val, unsigned flags,
		ber_len_t *len );
static int strval2ADstr( struct berval *val, char *str, unsigned flags, 
		ber_len_t *len );
static int dn2domain( LDAPDN dn, struct berval *bv, int pos, int *iRDN );

/* AVA helpers */
static LDAPAVA * ldapava_new(
	const struct berval *attr, const struct berval *val, unsigned flags, void *ctx );

/* Higher level helpers */
static int rdn2strlen( LDAPRDN rdn, unsigned flags, ber_len_t *len,
		int ( *s2l )( struct berval *, unsigned, ber_len_t * ) );
static int rdn2str( LDAPRDN rdn, char *str, unsigned flags, ber_len_t *len,
		int ( *s2s )( struct berval *, char *, unsigned, ber_len_t * ));
static int rdn2UFNstrlen( LDAPRDN rdn, unsigned flags, ber_len_t *len  );
static int rdn2UFNstr( LDAPRDN rdn, char *str, unsigned flags, ber_len_t *len );
static int rdn2DCEstrlen( LDAPRDN rdn, unsigned flags, ber_len_t *len );
static int rdn2DCEstr( LDAPRDN rdn, char *str, unsigned flag, ber_len_t *len, int first );
static int rdn2ADstrlen( LDAPRDN rdn, unsigned flags, ber_len_t *len );
static int rdn2ADstr( LDAPRDN rdn, char *str, unsigned flags, ber_len_t *len, int first );

/*
 * RFC 1823 ldap_get_dn
 */
char *
ldap_get_dn( LDAP *ld, LDAPMessage *entry )
{
	char		*dn;
	BerElement	tmp;

	Debug0( LDAP_DEBUG_TRACE, "ldap_get_dn\n" );

	assert( ld != NULL );
	assert( LDAP_VALID(ld) );
	assert( entry != NULL );

	tmp = *entry->lm_ber;	/* struct copy */
	if ( ber_scanf( &tmp, "{a" /*}*/, &dn ) == LBER_ERROR ) {
		ld->ld_errno = LDAP_DECODING_ERROR;
		return( NULL );
	}

	return( dn );
}

int
ldap_get_dn_ber( LDAP *ld, LDAPMessage *entry, BerElement **berout,
	BerValue *dn )
{
	BerElement	tmp, *ber;
	ber_len_t	len = 0;
	int rc = LDAP_SUCCESS;

	Debug0( LDAP_DEBUG_TRACE, "ldap_get_dn_ber\n" );

	assert( ld != NULL );
	assert( LDAP_VALID(ld) );
	assert( entry != NULL );
	assert( dn != NULL );

	dn->bv_val = NULL;
	dn->bv_len = 0;

	if ( berout ) {
		*berout = NULL;
		ber = ldap_alloc_ber_with_options( ld );
		if( ber == NULL ) {
			return LDAP_NO_MEMORY;
		}
		*berout = ber;
	} else {
		ber = &tmp;
	}
		
	*ber = *entry->lm_ber;	/* struct copy */
	if ( ber_scanf( ber, "{ml{" /*}*/, dn, &len ) == LBER_ERROR ) {
		rc = ld->ld_errno = LDAP_DECODING_ERROR;
	}
	if ( rc == LDAP_SUCCESS ) {
		/* set the length to avoid overrun */
		rc = ber_set_option( ber, LBER_OPT_REMAINING_BYTES, &len );
		if( rc != LBER_OPT_SUCCESS ) {
			rc = ld->ld_errno = LDAP_LOCAL_ERROR;
		}
	}
	if ( rc != LDAP_SUCCESS && berout ) {
		ber_free( ber, 0 );
		*berout = NULL;
	}
	return rc;
}

/*
 * RFC 1823 ldap_dn2ufn
 */
char *
ldap_dn2ufn( LDAP_CONST char *dn )
{
	char	*out = NULL;

	Debug0( LDAP_DEBUG_TRACE, "ldap_dn2ufn\n" );

	( void )ldap_dn_normalize( dn, LDAP_DN_FORMAT_LDAP, 
		&out, LDAP_DN_FORMAT_UFN );
	
	return( out );
}

/*
 * RFC 1823 ldap_explode_dn
 */
char **
ldap_explode_dn( LDAP_CONST char *dn, int notypes )
{
	LDAPDN	tmpDN;
	char	**values = NULL;
	int	iRDN;
	unsigned flag = notypes ? LDAP_DN_FORMAT_UFN : LDAP_DN_FORMAT_LDAPV3;
	
	Debug0( LDAP_DEBUG_TRACE, "ldap_explode_dn\n" );

	if ( ldap_str2dn( dn, &tmpDN, LDAP_DN_FORMAT_LDAP ) 
			!= LDAP_SUCCESS ) {
		return NULL;
	}

	if( tmpDN == NULL ) {
		values = LDAP_MALLOC( sizeof( char * ) );
		if( values == NULL ) return NULL;

		values[0] = NULL;
		return values;
	}

	for ( iRDN = 0; tmpDN[ iRDN ]; iRDN++ );

	values = LDAP_MALLOC( sizeof( char * ) * ( 1 + iRDN ) );
	if ( values == NULL ) {
		ldap_dnfree( tmpDN );
		return NULL;
	}

	for ( iRDN = 0; tmpDN[ iRDN ]; iRDN++ ) {
		ldap_rdn2str( tmpDN[ iRDN ], &values[ iRDN ], flag );
	}
	ldap_dnfree( tmpDN );
	values[ iRDN ] = NULL;

	return values;
}

char **
ldap_explode_rdn( LDAP_CONST char *rdn, int notypes )
{
	LDAPRDN		tmpRDN;
	char		**values = NULL;
	const char 	*p;
	int		iAVA;
	
	Debug0( LDAP_DEBUG_TRACE, "ldap_explode_rdn\n" );

	/*
	 * we only parse the first rdn
	 * FIXME: we prefer efficiency over checking if the _ENTIRE_
	 * dn can be parsed
	 */
	if ( ldap_str2rdn( rdn, &tmpRDN, (char **) &p, LDAP_DN_FORMAT_LDAP ) 
			!= LDAP_SUCCESS ) {
		return( NULL );
	}

	for ( iAVA = 0; tmpRDN[ iAVA ]; iAVA++ ) ;
	values = LDAP_MALLOC( sizeof( char * ) * ( 1 + iAVA ) );
	if ( values == NULL ) {
		ldap_rdnfree( tmpRDN );
		return( NULL );
	}

	for ( iAVA = 0; tmpRDN[ iAVA ]; iAVA++ ) {
		ber_len_t	l = 0, vl, al = 0;
		char		*str;
		LDAPAVA		*ava = tmpRDN[ iAVA ];
		
		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			vl = 1 + 2 * ava->la_value.bv_len;

		} else {
			if ( strval2strlen( &ava->la_value, 
						ava->la_flags, &vl ) ) {
				goto error_return;
			}
		}
		
		if ( !notypes ) {
			al = ava->la_attr.bv_len;
			l = vl + ava->la_attr.bv_len + 1;

			str = LDAP_MALLOC( l + 1 );
			if ( str == NULL ) {
				goto error_return;
			}
			AC_MEMCPY( str, ava->la_attr.bv_val, 
					ava->la_attr.bv_len );
			str[ al++ ] = '=';

		} else {
			l = vl;
			str = LDAP_MALLOC( l + 1 );
			if ( str == NULL ) {
				goto error_return;
			}
		}
		
		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			str[ al++ ] = '#';
			if ( binval2hexstr( &ava->la_value, &str[ al ] ) ) {
				goto error_return;
			}

		} else {
			if ( strval2str( &ava->la_value, &str[ al ], 
					ava->la_flags, &vl ) ) {
				goto error_return;
			}
		}

		str[ l ] = '\0';
		values[ iAVA ] = str;
	}
	values[ iAVA ] = NULL;

	ldap_rdnfree( tmpRDN );

	return( values );

error_return:;
	LBER_VFREE( values );
	ldap_rdnfree( tmpRDN );
	return( NULL );
}

char *
ldap_dn2dcedn( LDAP_CONST char *dn )
{
	char	*out = NULL;

	Debug0( LDAP_DEBUG_TRACE, "ldap_dn2dcedn\n" );

	( void )ldap_dn_normalize( dn, LDAP_DN_FORMAT_LDAP, 
				   &out, LDAP_DN_FORMAT_DCE );

	return( out );
}

char *
ldap_dcedn2dn( LDAP_CONST char *dce )
{
	char	*out = NULL;

	Debug0( LDAP_DEBUG_TRACE, "ldap_dcedn2dn\n" );

	( void )ldap_dn_normalize( dce, LDAP_DN_FORMAT_DCE, &out, LDAP_DN_FORMAT_LDAPV3 );

	return( out );
}

char *
ldap_dn2ad_canonical( LDAP_CONST char *dn )
{
	char	*out = NULL;

	Debug0( LDAP_DEBUG_TRACE, "ldap_dn2ad_canonical\n" );

	( void )ldap_dn_normalize( dn, LDAP_DN_FORMAT_LDAP, 
		       &out, LDAP_DN_FORMAT_AD_CANONICAL );

	return( out );
}

/*
 * function that changes the string representation of dnin
 * from ( fin & LDAP_DN_FORMAT_MASK ) to ( fout & LDAP_DN_FORMAT_MASK )
 * 
 * fin can be one of:
 * 	LDAP_DN_FORMAT_LDAP		(RFC 4514 liberal, plus some RFC 1779)
 * 	LDAP_DN_FORMAT_LDAPV3	(RFC 4514)
 * 	LDAP_DN_FORMAT_LDAPV2	(RFC 1779)
 * 	LDAP_DN_FORMAT_DCE		(?)
 *
 * fout can be any of the above except
 * 	LDAP_DN_FORMAT_LDAP
 * plus:
 * 	LDAP_DN_FORMAT_UFN		(RFC 1781, partial and with extensions)
 * 	LDAP_DN_FORMAT_AD_CANONICAL	(?)
 */
int
ldap_dn_normalize( LDAP_CONST char *dnin,
	unsigned fin, char **dnout, unsigned fout )
{
	int	rc;
	LDAPDN	tmpDN = NULL;

	Debug0( LDAP_DEBUG_TRACE, "ldap_dn_normalize\n" );

	assert( dnout != NULL );

	*dnout = NULL;

	if ( dnin == NULL ) {
		return( LDAP_SUCCESS );
	}

	rc = ldap_str2dn( dnin , &tmpDN, fin );
	if ( rc != LDAP_SUCCESS ) {
		return( rc );
	}

	rc = ldap_dn2str( tmpDN, dnout, fout );

	ldap_dnfree( tmpDN );

	return( rc );
}

/* States */
#define B4AVA			0x0000

/* #define	B4ATTRTYPE		0x0001 */
#define B4OIDATTRTYPE		0x0002
#define B4STRINGATTRTYPE	0x0003

#define B4AVAEQUALS		0x0100
#define B4AVASEP		0x0200
#define B4RDNSEP		0x0300
#define GOTAVA			0x0400

#define B4ATTRVALUE		0x0010
#define B4STRINGVALUE		0x0020
#define B4IA5VALUEQUOTED	0x0030
#define B4IA5VALUE		0x0040
#define B4BINARYVALUE		0x0050

/*
 * Helpers (mostly from slap.h)
 * c is assumed to Unicode in an ASCII compatible format (UTF-8)
 * Macros assume "C" Locale (ASCII)
 */
#define LDAP_DN_ASCII_SPACE(c) \
	( (c) == ' ' || (c) == '\t' || (c) == '\n' || (c) == '\r' )
#define LDAP_DN_ASCII_LOWER(c)		LDAP_LOWER(c)
#define LDAP_DN_ASCII_UPPER(c)		LDAP_UPPER(c)
#define LDAP_DN_ASCII_ALPHA(c)		LDAP_ALPHA(c)

#define LDAP_DN_ASCII_DIGIT(c)		LDAP_DIGIT(c)
#define LDAP_DN_ASCII_LCASE_HEXALPHA(c)	LDAP_HEXLOWER(c)
#define LDAP_DN_ASCII_UCASE_HEXALPHA(c)	LDAP_HEXUPPER(c)
#define LDAP_DN_ASCII_HEXDIGIT(c)	LDAP_HEX(c)
#define LDAP_DN_ASCII_ALNUM(c)		LDAP_ALNUM(c)
#define LDAP_DN_ASCII_PRINTABLE(c)	( (c) >= ' ' && (c) <= '~' )

/* attribute type */
#define LDAP_DN_OID_LEADCHAR(c)		LDAP_DIGIT(c)
#define LDAP_DN_DESC_LEADCHAR(c)	LDAP_ALPHA(c)
#define LDAP_DN_DESC_CHAR(c)		LDAP_LDH(c)
#define LDAP_DN_LANG_SEP(c)		( (c) == ';' )
#define LDAP_DN_ATTRDESC_CHAR(c) \
	( LDAP_DN_DESC_CHAR(c) || LDAP_DN_LANG_SEP(c) )

/* special symbols */
#define LDAP_DN_AVA_EQUALS(c)		( (c) == '=' )
#define LDAP_DN_AVA_SEP(c)		( (c) == '+' )
#define LDAP_DN_RDN_SEP(c)		( (c) == ',' )
#define LDAP_DN_RDN_SEP_V2(c)		( LDAP_DN_RDN_SEP(c) || (c) == ';' )
#define LDAP_DN_OCTOTHORPE(c)		( (c) == '#' )
#define LDAP_DN_QUOTES(c)		( (c) == '\"' )
#define LDAP_DN_ESCAPE(c)		( (c) == '\\' )
#define LDAP_DN_VALUE_END(c) \
	( LDAP_DN_RDN_SEP(c) || LDAP_DN_AVA_SEP(c) )

/* NOTE: according to RFC 4514, '=' can be escaped and treated as special,
 * i.e. escaped both as "\<hexpair>" and * as "\=", but it is treated as
 * a regular char, i.e. it can also appear as '='.
 *
 * As such, in 2.2 we used to allow reading unescaped '=', but we always
 * produced escaped '\3D'; this changes since 2.3, if compatibility issues
 * do not arise
 */
#define LDAP_DN_NE(c) \
	( LDAP_DN_RDN_SEP_V2(c) || LDAP_DN_AVA_SEP(c) \
	  || LDAP_DN_QUOTES(c) \
	  || (c) == '<' || (c) == '>' )
#define LDAP_DN_MAYESCAPE(c) \
	( LDAP_DN_ESCAPE(c) || LDAP_DN_NE(c) \
	  || LDAP_DN_AVA_EQUALS(c) \
	  || LDAP_DN_ASCII_SPACE(c) || LDAP_DN_OCTOTHORPE(c) )
#define LDAP_DN_SHOULDESCAPE(c)		( LDAP_DN_AVA_EQUALS(c) )

#define LDAP_DN_NEEDESCAPE(c) \
	( LDAP_DN_ESCAPE(c) || LDAP_DN_NE(c) )
#define LDAP_DN_NEEDESCAPE_LEAD(c) 	LDAP_DN_MAYESCAPE(c)
#define LDAP_DN_NEEDESCAPE_TRAIL(c) \
	( LDAP_DN_ASCII_SPACE(c) || LDAP_DN_NEEDESCAPE(c) )
#define LDAP_DN_WILLESCAPE_CHAR(c) \
	( LDAP_DN_RDN_SEP(c) || LDAP_DN_AVA_SEP(c) || LDAP_DN_ESCAPE(c) )
#define LDAP_DN_IS_PRETTY(f)		( (f) & LDAP_DN_PRETTY )
#define LDAP_DN_WILLESCAPE_HEX(f, c) \
	( ( !LDAP_DN_IS_PRETTY( f ) ) && LDAP_DN_WILLESCAPE_CHAR(c) )

/* LDAPv2 */
#define	LDAP_DN_VALUE_END_V2(c) \
	( LDAP_DN_RDN_SEP_V2(c) || LDAP_DN_AVA_SEP(c) )
/* RFC 1779 */
#define	LDAP_DN_V2_SPECIAL(c) \
	  ( LDAP_DN_RDN_SEP_V2(c) || LDAP_DN_AVA_EQUALS(c) \
	    || LDAP_DN_AVA_SEP(c) || (c) == '<' || (c) == '>' \
	    || LDAP_DN_OCTOTHORPE(c) )
#define LDAP_DN_V2_PAIR(c) \
	  ( LDAP_DN_V2_SPECIAL(c) || LDAP_DN_ESCAPE(c) || LDAP_DN_QUOTES(c) )

/*
 * DCE (mostly from Luke Howard and IBM implementation for AIX)
 *
 * From: "Application Development Guide - Directory Services" (FIXME: add link?)
 * Here escapes and valid chars for GDS are considered; as soon as more
 * specific info is found, the macros will be updated.
 *
 * Chars:	'a'-'z', 'A'-'Z', '0'-'9', 
 *		'.', ':', ',', ''', '+', '-', '=', '(', ')', '?', '/', ' '.
 *
 * Metachars:	'/', ',', '=', '\'.
 *
 * the '\' is used to escape other metachars.
 *
 * Assertion:		'='
 * RDN separator:	'/'
 * AVA separator:	','
 * 
 * Attribute types must start with alphabetic chars and can contain 
 * alphabetic chars and digits (FIXME: no '-'?). OIDs are allowed.
 */
#define LDAP_DN_RDN_SEP_DCE(c)		( (c) == '/' )
#define LDAP_DN_AVA_SEP_DCE(c)		( (c) == ',' )
#define LDAP_DN_ESCAPE_DCE(c)		( LDAP_DN_ESCAPE(c) )
#define	LDAP_DN_VALUE_END_DCE(c) \
	( LDAP_DN_RDN_SEP_DCE(c) || LDAP_DN_AVA_SEP_DCE(c) )
#define LDAP_DN_NEEDESCAPE_DCE(c) \
	( LDAP_DN_VALUE_END_DCE(c) || LDAP_DN_AVA_EQUALS(c) )

/* AD Canonical */
#define LDAP_DN_RDN_SEP_AD(c)		( (c) == '/' )
#define LDAP_DN_ESCAPE_AD(c)		( LDAP_DN_ESCAPE(c) )
#define LDAP_DN_AVA_SEP_AD(c)		( (c) == ',' )	/* assume same as DCE */
#define	LDAP_DN_VALUE_END_AD(c) \
	( LDAP_DN_RDN_SEP_AD(c) || LDAP_DN_AVA_SEP_AD(c) )
#define LDAP_DN_NEEDESCAPE_AD(c) \
	( LDAP_DN_VALUE_END_AD(c) || LDAP_DN_AVA_EQUALS(c) )

/* generics */
#define LDAP_DN_HEXPAIR(s) \
	( LDAP_DN_ASCII_HEXDIGIT((s)[0]) && LDAP_DN_ASCII_HEXDIGIT((s)[1]) )
/* better look at the AttributeDescription? */

/* FIXME: no composite rdn or non-"dc" types, right?
 * (what about "dc" in OID form?) */
/* FIXME: we do not allow binary values in domain, right? */
/* NOTE: use this macro only when ABSOLUTELY SURE rdn IS VALID! */
/* NOTE: don't use strcasecmp() as it is locale specific! */
#define	LDAP_DC_ATTR	"dc"
#define	LDAP_DC_ATTRU	"DC"
#define LDAP_DN_IS_RDN_DC( r ) \
	( (r) && (r)[0] && !(r)[1] \
	  && ((r)[0]->la_flags & LDAP_AVA_STRING) \
	  && ((r)[0]->la_attr.bv_len == 2) \
	  && (((r)[0]->la_attr.bv_val[0] == LDAP_DC_ATTR[0]) \
		|| ((r)[0]->la_attr.bv_val[0] == LDAP_DC_ATTRU[0])) \
	  && (((r)[0]->la_attr.bv_val[1] == LDAP_DC_ATTR[1]) \
		|| ((r)[0]->la_attr.bv_val[1] == LDAP_DC_ATTRU[1])))

/* Composite rules */
#define LDAP_DN_ALLOW_ONE_SPACE(f) \
	( LDAP_DN_LDAPV2(f) \
	  || !( (f) & LDAP_DN_P_NOSPACEAFTERRDN ) )
#define LDAP_DN_ALLOW_SPACES(f) \
	( LDAP_DN_LDAPV2(f) \
	  || !( (f) & ( LDAP_DN_P_NOLEADTRAILSPACES | LDAP_DN_P_NOSPACEAFTERRDN ) ) )
#define LDAP_DN_LDAP(f) \
	( ( (f) & LDAP_DN_FORMAT_MASK ) == LDAP_DN_FORMAT_LDAP )
#define LDAP_DN_LDAPV3(f) \
	( ( (f) & LDAP_DN_FORMAT_MASK ) == LDAP_DN_FORMAT_LDAPV3 )
#define LDAP_DN_LDAPV2(f) \
	( ( (f) & LDAP_DN_FORMAT_MASK ) == LDAP_DN_FORMAT_LDAPV2 )
#define LDAP_DN_DCE(f) \
	( ( (f) & LDAP_DN_FORMAT_MASK ) == LDAP_DN_FORMAT_DCE )
#define LDAP_DN_UFN(f) \
	( ( (f) & LDAP_DN_FORMAT_MASK ) == LDAP_DN_FORMAT_UFN )
#define LDAP_DN_ADC(f) \
	( ( (f) & LDAP_DN_FORMAT_MASK ) == LDAP_DN_FORMAT_AD_CANONICAL )
#define LDAP_DN_FORMAT(f)		( (f) & LDAP_DN_FORMAT_MASK )

/*
 * LDAPAVA helpers (will become part of the API for operations 
 * on structural representations of DNs).
 */
static LDAPAVA *
ldapava_new( const struct berval *attr, const struct berval *val, 
		unsigned flags, void *ctx )
{
	LDAPAVA *ava;

	assert( attr != NULL );
	assert( val != NULL );

	ava = LDAP_MALLOCX( sizeof( LDAPAVA ) + attr->bv_len + 1, ctx );

	if ( ava ) {
		ava->la_attr.bv_len = attr->bv_len;
		ava->la_attr.bv_val = (char *)(ava+1);
		AC_MEMCPY( ava->la_attr.bv_val, attr->bv_val, attr->bv_len );
		ava->la_attr.bv_val[attr->bv_len] = '\0';

		ava->la_value = *val;
		ava->la_flags = flags | LDAP_AVA_FREE_VALUE;

		ava->la_private = NULL;
	}

	return( ava );
}

static void
ldapava_free( LDAPAVA *ava, void *ctx )
{
	assert( ava != NULL );

#if 0
	/* ava's private must be freed by caller
	 * (at present let's skip this check because la_private
	 * basically holds static data) */
	assert( ava->la_private == NULL );
#endif

	if (ava->la_flags & LDAP_AVA_FREE_VALUE)
		LDAP_FREEX( ava->la_value.bv_val, ctx );

	LDAP_FREEX( ava, ctx );
}

void
ldap_rdnfree( LDAPRDN rdn )
{
	ldap_rdnfree_x( rdn, NULL );
}

void
ldap_rdnfree_x( LDAPRDN rdn, void *ctx )
{
	int iAVA;
	
	if ( rdn == NULL ) {
		return;
	}

	for ( iAVA = 0; rdn[ iAVA ]; iAVA++ ) {
		ldapava_free( rdn[ iAVA ], ctx );
	}

	LDAP_FREEX( rdn, ctx );
}

void
ldap_dnfree( LDAPDN dn )
{
	ldap_dnfree_x( dn, NULL );
}

void
ldap_dnfree_x( LDAPDN dn, void *ctx )
{
	int iRDN;
	
	if ( dn == NULL ) {
		return;
	}

	for ( iRDN = 0; dn[ iRDN ]; iRDN++ ) {
		ldap_rdnfree_x( dn[ iRDN ], ctx );
	}

	LDAP_FREEX( dn, ctx );
}

/*
 * Converts a string representation of a DN (in LDAPv3, LDAPv2 or DCE)
 * into a structural representation of the DN, by separating attribute
 * types and values encoded in the more appropriate form, which is
 * string or OID for attribute types and binary form of the BER encoded
 * value or Unicode string. Formats different from LDAPv3 are parsed
 * according to their own rules and turned into the more appropriate
 * form according to LDAPv3.
 *
 * NOTE: I realize the code is getting spaghettish; it is rather
 * experimental and will hopefully turn into something more simple
 * and readable as soon as it works as expected.
 */

/*
 * Default sizes of AVA and RDN static working arrays; if required
 * the are dynamically resized.  The values can be tuned in case
 * of special requirements (e.g. very deep DN trees or high number 
 * of AVAs per RDN).
 */
#define	TMP_AVA_SLOTS	8
#define	TMP_RDN_SLOTS	32

int
ldap_str2dn( LDAP_CONST char *str, LDAPDN *dn, unsigned flags )
{
	struct berval	bv;

	assert( str != NULL );

	bv.bv_len = strlen( str );
	bv.bv_val = (char *) str;
	
	return ldap_bv2dn_x( &bv, dn, flags, NULL );
}

int
ldap_bv2dn( struct berval *bv, LDAPDN *dn, unsigned flags )
{
	return ldap_bv2dn_x( bv, dn, flags, NULL );
}

int
ldap_bv2dn_x( struct berval *bvin, LDAPDN *dn, unsigned flags, void *ctx )
{
	const char 	*p;
	int		rc = LDAP_DECODING_ERROR;
	int		nrdns = 0;

	LDAPDN		newDN = NULL;
	LDAPRDN		newRDN = NULL, tmpDN_[TMP_RDN_SLOTS], *tmpDN = tmpDN_;
	int		num_slots = TMP_RDN_SLOTS;
	char		*str, *end;
	struct berval	bvtmp, *bv = &bvtmp;
	
	assert( bvin != NULL );
	assert( bvin->bv_val != NULL );
	assert( dn != NULL );

	*bv = *bvin;
	str = bv->bv_val;
	end = str + bv->bv_len;

	Debug2( LDAP_DEBUG_ARGS, "=> ldap_bv2dn(%s,%u)\n", str, flags );

	*dn = NULL;

	switch ( LDAP_DN_FORMAT( flags ) ) {
	case LDAP_DN_FORMAT_LDAP:
	case LDAP_DN_FORMAT_LDAPV3:
	case LDAP_DN_FORMAT_DCE:
		break;

		/* allow DN enclosed in brackets */
	case LDAP_DN_FORMAT_LDAPV2:
		if ( str[0] == '<' ) {
			if ( bv->bv_len < 2 || end[ -1 ] != '>' ) {
				rc = LDAP_DECODING_ERROR;
				goto parsing_error;
			}
			bv->bv_val++;
			bv->bv_len -= 2;
			str++;
			end--;
		}
		break;

	/* unsupported in str2dn */
	case LDAP_DN_FORMAT_UFN:
	case LDAP_DN_FORMAT_AD_CANONICAL:
		return LDAP_PARAM_ERROR;

	case LDAP_DN_FORMAT_LBER:
	default:
		return LDAP_PARAM_ERROR;
	}

	if ( bv->bv_len == 0 ) {
		return LDAP_SUCCESS;
	}

	if( memchr( bv->bv_val, '\0', bv->bv_len ) != NULL ) {
		/* value must have embedded NULs */
		return LDAP_DECODING_ERROR;
	}

	p = str;
	if ( LDAP_DN_DCE( flags ) ) {
		
		/* 
		 * (from Luke Howard: thnx) A RDN separator is required
		 * at the beginning of an (absolute) DN.
		 */
		if ( !LDAP_DN_RDN_SEP_DCE( p[ 0 ] ) ) {
			goto parsing_error;
		}
		p++;

	/*
	 * actually we do not want to accept by default the DCE form,
	 * we do not want to auto-detect it
	 */
#if 0
	} else if ( LDAP_DN_LDAP( flags ) ) {
		/*
		 * if dn starts with '/' let's make it a DCE dn
		 */
		if ( LDAP_DN_RDN_SEP_DCE( p[ 0 ] ) ) {
			flags |= LDAP_DN_FORMAT_DCE;
			p++;
		}
#endif
	}

	for ( ; p < end; p++ ) {
		int		err;
		struct berval 	tmpbv;
		tmpbv.bv_len = bv->bv_len - ( p - str );
		tmpbv.bv_val = (char *)p;
		
		err = ldap_bv2rdn_x( &tmpbv, &newRDN, (char **) &p, flags,ctx);
		if ( err != LDAP_SUCCESS ) {
			goto parsing_error;
		}

		/* 
		 * We expect a rdn separator
		 */
		if ( p < end && p[ 0 ] ) {
			switch ( LDAP_DN_FORMAT( flags ) ) {
			case LDAP_DN_FORMAT_LDAPV3:
				if ( !LDAP_DN_RDN_SEP( p[ 0 ] ) ) {
					rc = LDAP_DECODING_ERROR;
					goto parsing_error;
				}
				break;
	
			case LDAP_DN_FORMAT_LDAP:
			case LDAP_DN_FORMAT_LDAPV2:
				if ( !LDAP_DN_RDN_SEP_V2( p[ 0 ] ) ) {
					rc = LDAP_DECODING_ERROR;
					goto parsing_error;
				}
				break;
	
			case LDAP_DN_FORMAT_DCE:
				if ( !LDAP_DN_RDN_SEP_DCE( p[ 0 ] ) ) {
					rc = LDAP_DECODING_ERROR;
					goto parsing_error;
				}
				break;
			}
		}


		tmpDN[nrdns++] = newRDN;
		newRDN = NULL;

		/*
		 * make the static RDN array dynamically rescalable
		 */
		if ( nrdns == num_slots ) {
			LDAPRDN	*tmp;

			if ( tmpDN == tmpDN_ ) {
				tmp = LDAP_MALLOCX( num_slots * 2 * sizeof( LDAPRDN * ), ctx );
				if ( tmp == NULL ) {
					rc = LDAP_NO_MEMORY;
					goto parsing_error;
				}
				AC_MEMCPY( tmp, tmpDN, num_slots * sizeof( LDAPRDN * ) );

			} else {
				tmp = LDAP_REALLOCX( tmpDN, num_slots * 2 * sizeof( LDAPRDN * ), ctx );
				if ( tmp == NULL ) {
					rc = LDAP_NO_MEMORY;
					goto parsing_error;
				}
			}

			tmpDN = tmp;
			num_slots *= 2;
		}
				
		if ( p >= end || p[ 0 ] == '\0' ) {
			/* 
			 * the DN is over, phew
			 */
			newDN = (LDAPDN)LDAP_MALLOCX( sizeof(LDAPRDN *) * (nrdns+1), ctx );
			if ( newDN == NULL ) {
				rc = LDAP_NO_MEMORY;
				goto parsing_error;
			} else {
				int i;

				if ( LDAP_DN_DCE( flags ) ) {
					/* add in reversed order */
					for ( i=0; i<nrdns; i++ )
						newDN[i] = tmpDN[nrdns-1-i];
				} else {
					for ( i=0; i<nrdns; i++ )
						newDN[i] = tmpDN[i];
				}
				newDN[nrdns] = NULL;
				rc = LDAP_SUCCESS;
			}
			goto return_result;
		}
	}
	
parsing_error:;
	if ( newRDN ) {
		ldap_rdnfree_x( newRDN, ctx );
	}

	for ( nrdns-- ;nrdns >= 0; nrdns-- ) {
		ldap_rdnfree_x( tmpDN[nrdns], ctx );
	}

return_result:;

	if ( tmpDN != tmpDN_ ) {
		LDAP_FREEX( tmpDN, ctx );
	}

	Debug3( LDAP_DEBUG_ARGS, "<= ldap_bv2dn(%s)=%d %s\n", str, rc,
			rc ? ldap_err2string( rc ) : "" );
	*dn = newDN;
	
	return( rc );
}

/*
 * ldap_str2rdn
 *
 * Parses a relative DN according to flags up to a rdn separator 
 * or to the end of str.
 * Returns the rdn and a pointer to the string continuation, which
 * corresponds to the rdn separator or to '\0' in case the string is over.
 */
int
ldap_str2rdn( LDAP_CONST char *str, LDAPRDN *rdn,
	char **n_in, unsigned flags )
{
	struct berval	bv;

	assert( str != NULL );
	assert( str[ 0 ] != '\0' );	/* FIXME: is this required? */

	bv.bv_len = strlen( str );
	bv.bv_val = (char *) str;

	return ldap_bv2rdn_x( &bv, rdn, n_in, flags, NULL );
}

int
ldap_bv2rdn( struct berval *bv, LDAPRDN *rdn,
	char **n_in, unsigned flags )
{
	return ldap_bv2rdn_x( bv, rdn, n_in, flags, NULL );
}

int
ldap_bv2rdn_x( struct berval *bv, LDAPRDN *rdn,
	char **n_in, unsigned flags, void *ctx )
{
	const char  	**n = (const char **) n_in;
	const char 	*p;
	int		navas = 0;
	int 		state = B4AVA;
	int		rc = LDAP_DECODING_ERROR;
	int		attrTypeEncoding = LDAP_AVA_STRING, 
			attrValueEncoding = LDAP_AVA_STRING;

	struct berval	attrType = BER_BVNULL;
	struct berval 	attrValue = BER_BVNULL;

	LDAPRDN		newRDN = NULL;
	LDAPAVA		*tmpRDN_[TMP_AVA_SLOTS], **tmpRDN = tmpRDN_;
	int		num_slots = TMP_AVA_SLOTS;

	char		*str;
	ber_len_t	stoplen;
	
	assert( bv != NULL );
	assert( bv->bv_len != 0 );
	assert( bv->bv_val != NULL );
	assert( rdn || flags & LDAP_DN_SKIP );
	assert( n != NULL );

	str = bv->bv_val;
	stoplen = bv->bv_len;

	if ( rdn ) {
		*rdn = NULL;
	}
	*n = NULL;

	switch ( LDAP_DN_FORMAT( flags ) ) {
	case LDAP_DN_FORMAT_LDAP:
	case LDAP_DN_FORMAT_LDAPV3:
	case LDAP_DN_FORMAT_LDAPV2:
	case LDAP_DN_FORMAT_DCE:
		break;

	/* unsupported in str2dn */
	case LDAP_DN_FORMAT_UFN:
	case LDAP_DN_FORMAT_AD_CANONICAL:
		return LDAP_PARAM_ERROR;

	case LDAP_DN_FORMAT_LBER:
	default:
		return LDAP_PARAM_ERROR;
	}

	if ( bv->bv_len == 0 ) {
		return LDAP_SUCCESS;

	}

	if( memchr( bv->bv_val, '\0', bv->bv_len ) != NULL ) {
		/* value must have embedded NULs */
		return LDAP_DECODING_ERROR;
	}

	p = str;
	for ( ; p[ 0 ] || state == GOTAVA; ) {
		
		/*
		 * The parser in principle advances one token a time,
		 * or toggles state if preferable.
		 */
		switch (state) {

		/*
		 * an AttributeType can be encoded as:
		 * - its string representation; in detail, implementations
		 *   MUST recognize AttributeType string type names listed 
		 *   in Section 3 of RFC 4514, and MAY recognize other names.
		 * - its numeric OID (a dotted decimal string)
		 */
		case B4AVA:
			if ( LDAP_DN_ASCII_SPACE( p[ 0 ] ) ) {
				if ( !LDAP_DN_ALLOW_ONE_SPACE( flags ) ) {
					/* error */
					goto parsing_error;
				}
				p++;
			}

			if ( LDAP_DN_ASCII_SPACE( p[ 0 ] ) ) {
				if ( !LDAP_DN_ALLOW_SPACES( flags ) ) {
					/* error */
					goto parsing_error;
				}

				/* whitespace is allowed (and trimmed) */
				p++;
				while ( p[ 0 ] && LDAP_DN_ASCII_SPACE( p[ 0 ] ) ) {
					p++;
				}

				if ( !p[ 0 ] ) {
					/* error: we expected an AVA */
					goto parsing_error;
				}
			}

			/* oid */
			if ( LDAP_DN_OID_LEADCHAR( p[ 0 ] ) ) {
				state = B4OIDATTRTYPE;
				break;
			}
			
			/* else must be alpha */
			if ( !LDAP_DN_DESC_LEADCHAR( p[ 0 ] ) ) {
				goto parsing_error;
			}
			
			/* LDAPv2 "oid." prefix */
			if ( LDAP_DN_LDAPV2( flags ) ) {
				/*
				 * to be overly pedantic, we only accept
				 * "OID." or "oid."
				 */
				if ( flags & LDAP_DN_PEDANTIC ) {
					if ( !strncmp( p, "OID.", 4 )
						|| !strncmp( p, "oid.", 4 ) ) {
						p += 4;
						state = B4OIDATTRTYPE;
						break;
					}
				} else {
				       if ( !strncasecmp( p, "oid.", 4 ) ) {
					       p += 4;
					       state = B4OIDATTRTYPE;
					       break;
				       }
				}
			}

			state = B4STRINGATTRTYPE;
			break;
		
		case B4OIDATTRTYPE: {
			int 		err = LDAP_SUCCESS;
			
			attrType.bv_val = ldap_int_parse_numericoid( &p, &err,
				LDAP_SCHEMA_SKIP);

			if ( err != LDAP_SUCCESS ) {
				goto parsing_error;
			}
			attrType.bv_len = p - attrType.bv_val;

			attrTypeEncoding = LDAP_AVA_BINARY;

			state = B4AVAEQUALS;
			break;
		}

		case B4STRINGATTRTYPE: {
			const char 	*startPos, *endPos = NULL;
			ber_len_t 	len;
			
			/* 
			 * the starting char has been found to be
			 * a LDAP_DN_DESC_LEADCHAR so we don't re-check it
			 * FIXME: DCE attr types seem to have a more
			 * restrictive syntax (no '-' ...) 
			 */
			for ( startPos = p++; p[ 0 ]; p++ ) {
				if ( LDAP_DN_DESC_CHAR( p[ 0 ] ) ) {
					continue;
				}

				if ( LDAP_DN_LANG_SEP( p[ 0 ] ) ) {
					
					/*
					 * RFC 4514 explicitly does not allow attribute
					 * description options, such as language tags.
					 */
					if ( flags & LDAP_DN_PEDANTIC ) {
						goto parsing_error;
					}

					/*
					 * we trim ';' and following lang 
					 * and so from attribute types
					 */
					endPos = p;
					for ( ; LDAP_DN_ATTRDESC_CHAR( p[ 0 ] )
							|| LDAP_DN_LANG_SEP( p[ 0 ] ); p++ ) {
						/* no op */ ;
					}
					break;
				}
				break;
			}

			len = ( endPos ? endPos : p ) - startPos;
			if ( len == 0 ) {
				goto parsing_error;
			}
			
			attrTypeEncoding = LDAP_AVA_STRING;

			/*
			 * here we need to decide whether to use it as is 
			 * or turn it in OID form; as a consequence, we
			 * need to decide whether to binary encode the value
			 */
			
			state = B4AVAEQUALS;

			if ( flags & LDAP_DN_SKIP ) {
				break;
			}

			attrType.bv_val = (char *)startPos;
			attrType.bv_len = len;

			break;
		}
				
		case B4AVAEQUALS:
			/* spaces may not be allowed */
			if ( LDAP_DN_ASCII_SPACE( p[ 0 ] ) ) {
				if ( !LDAP_DN_ALLOW_SPACES( flags ) ) {
					goto parsing_error;
				}
			
				/* trim spaces */
				for ( p++; LDAP_DN_ASCII_SPACE( p[ 0 ] ); p++ ) {
					/* no op */
				}
			}

			/* need equal sign */
			if ( !LDAP_DN_AVA_EQUALS( p[ 0 ] ) ) {
				goto parsing_error;
			}
			p++;

			/* spaces may not be allowed */
			if ( LDAP_DN_ASCII_SPACE( p[ 0 ] ) ) {
				if ( !LDAP_DN_ALLOW_SPACES( flags ) ) {
					goto parsing_error;
				}

				/* trim spaces */
				for ( p++; LDAP_DN_ASCII_SPACE( p[ 0 ] ); p++ ) {
					/* no op */
				}
			}

			/*
			 * octothorpe means a BER encoded value will follow
			 * FIXME: I don't think DCE will allow it
			 */
			if ( LDAP_DN_OCTOTHORPE( p[ 0 ] ) ) {
				p++;
				attrValueEncoding = LDAP_AVA_BINARY;
				state = B4BINARYVALUE;
				break;
			}

			/* STRING value expected */

			/* 
			 * if we're pedantic, an attribute type in OID form
			 * SHOULD imply a BER encoded attribute value; we
			 * should at least issue a warning
			 */
			if ( ( flags & LDAP_DN_PEDANTIC )
				&& ( attrTypeEncoding == LDAP_AVA_BINARY ) ) {
				/* OID attrType SHOULD use binary encoding */
				goto parsing_error;
			}

			attrValueEncoding = LDAP_AVA_STRING;

			/* 
			 * LDAPv2 allows the attribute value to be quoted;
			 * also, IA5 values are expected, in principle
			 */
			if ( LDAP_DN_LDAPV2( flags ) || LDAP_DN_LDAP( flags ) ) {
				if ( LDAP_DN_QUOTES( p[ 0 ] ) ) {
					p++;
					state = B4IA5VALUEQUOTED;
					break;
				}

				if ( LDAP_DN_LDAPV2( flags ) ) {
					state = B4IA5VALUE;
					break;
				}
			}

			/*
			 * here STRING means RFC 4514 string
			 * FIXME: what about DCE strings? 
			 */
			if ( !p[ 0 ] ) {
				/* empty value */
				state = GOTAVA;
			} else {
				state = B4STRINGVALUE;
			}
			break;

		case B4BINARYVALUE:
			if ( hexstr2binval( p, &attrValue, &p, flags, ctx ) ) {
				goto parsing_error;
			}

			state = GOTAVA;
			break;

		case B4STRINGVALUE:
			switch ( LDAP_DN_FORMAT( flags ) ) {
			case LDAP_DN_FORMAT_LDAP:
			case LDAP_DN_FORMAT_LDAPV3:
				if ( str2strval( p, stoplen - ( p - str ),
							&attrValue, &p, flags, 
							&attrValueEncoding, ctx ) ) {
					goto parsing_error;
				}
				break;

			case LDAP_DN_FORMAT_DCE:
				if ( DCE2strval( p, &attrValue, &p, flags, ctx ) ) {
					goto parsing_error;
				}
				break;

			default:
				assert( 0 );
			}

			state = GOTAVA;
			break;

		case B4IA5VALUE:
			if ( IA52strval( p, &attrValue, &p, flags, ctx ) ) {
				goto parsing_error;
			}

			state = GOTAVA;
			break;
		
		case B4IA5VALUEQUOTED:

			/* lead quote already stripped */
			if ( quotedIA52strval( p, &attrValue, 
						&p, flags, ctx ) ) {
				goto parsing_error;
			}

			state = GOTAVA;
			break;

		case GOTAVA: {
			int	rdnsep = 0;

			if ( !( flags & LDAP_DN_SKIP ) ) {
				LDAPAVA *ava;

				/*
				 * we accept empty values
				 */
				ava = ldapava_new( &attrType, &attrValue, 
						attrValueEncoding, ctx );
				if ( ava == NULL ) {
					rc = LDAP_NO_MEMORY;
					goto parsing_error;
				}
				tmpRDN[navas++] = ava;

				attrValue.bv_val = NULL;
				attrValue.bv_len = 0;

				/*
				 * prepare room for new AVAs if needed
				 */
				if (navas == num_slots) {
					LDAPAVA **tmp;
					
					if ( tmpRDN == tmpRDN_ ) {
						tmp = LDAP_MALLOCX( num_slots * 2 * sizeof( LDAPAVA * ), ctx );
						if ( tmp == NULL ) {
							rc = LDAP_NO_MEMORY;
							goto parsing_error;
						}
						AC_MEMCPY( tmp, tmpRDN, num_slots * sizeof( LDAPAVA * ) );

					} else {
						tmp = LDAP_REALLOCX( tmpRDN, num_slots * 2 * sizeof( LDAPAVA * ), ctx );
						if ( tmp == NULL ) {
							rc = LDAP_NO_MEMORY;
							goto parsing_error;
						}
					}

					tmpRDN = tmp;
					num_slots *= 2;
				}
			}
			
			/* 
			 * if we got an AVA separator ('+', or ',' for DCE ) 
			 * we expect a new AVA for this RDN; otherwise 
			 * we add the RDN to the DN
			 */
			switch ( LDAP_DN_FORMAT( flags ) ) {
			case LDAP_DN_FORMAT_LDAP:
			case LDAP_DN_FORMAT_LDAPV3:
			case LDAP_DN_FORMAT_LDAPV2:
				if ( !LDAP_DN_AVA_SEP( p[ 0 ] ) ) {
					rdnsep = 1;
				}
				break;

			case LDAP_DN_FORMAT_DCE:
				if ( !LDAP_DN_AVA_SEP_DCE( p[ 0 ] ) ) {
					rdnsep = 1;
				}
				break;
			}

			if ( rdnsep ) {
				/* 
				 * the RDN is over, phew
				 */
				*n = p;
				if ( !( flags & LDAP_DN_SKIP ) ) {
					newRDN = (LDAPRDN)LDAP_MALLOCX( 
						sizeof(LDAPAVA) * (navas+1), ctx );
					if ( newRDN == NULL ) {
						rc = LDAP_NO_MEMORY;
						goto parsing_error;
					} else {
						AC_MEMCPY( newRDN, tmpRDN, sizeof(LDAPAVA *) * navas);
						newRDN[navas] = NULL;
					}

				}
				rc = LDAP_SUCCESS;
				goto return_result;
			}

			/* they should have been used in an AVA */
			attrType.bv_val = NULL;
			attrValue.bv_val = NULL;
			
			p++;
			state = B4AVA;
			break;
		}

		default:
			assert( 0 );
			goto parsing_error;
		}
	}
	*n = p;
	
parsing_error:;
	/* They are set to NULL after they're used in an AVA */

	if ( attrValue.bv_val ) {
		LDAP_FREEX( attrValue.bv_val, ctx );
	}

	for ( navas-- ; navas >= 0; navas-- ) {
		ldapava_free( tmpRDN[navas], ctx );
	}

return_result:;

	if ( tmpRDN != tmpRDN_ ) {
		LDAP_FREEX( tmpRDN, ctx );
	}

	if ( rdn ) {
		*rdn = newRDN;
	}
	
	return( rc );
}

/*
 * reads in a UTF-8 string value, unescaping stuff:
 * '\' + LDAP_DN_NEEDESCAPE(c) -> 'c'
 * '\' + HEXPAIR(p) -> unhex(p)
 */
static int
str2strval( const char *str, ber_len_t stoplen, struct berval *val, const char **next, unsigned flags, int *retFlags, void *ctx )
{
	const char 	*p, *end, *startPos, *endPos = NULL;
	ber_len_t	len, escapes;

	assert( str != NULL );
	assert( val != NULL );
	assert( next != NULL );

	*next = NULL;
	end = str + stoplen;
	for ( startPos = p = str, escapes = 0; p < end; p++ ) {
		if ( LDAP_DN_ESCAPE( p[ 0 ] ) ) {
			p++;
			if ( p[ 0 ] == '\0' ) {
				return( 1 );
			}
			if ( LDAP_DN_MAYESCAPE( p[ 0 ] ) ) {
				escapes++;
				continue;
			}

			if ( LDAP_DN_HEXPAIR( p ) ) {
				char c;

				hexstr2bin( p, &c );
				escapes += 2;

				if ( !LDAP_DN_ASCII_PRINTABLE( c ) ) {

					/*
					 * we assume the string is UTF-8
					 */
					*retFlags = LDAP_AVA_NONPRINTABLE;
				}
				p++;

				continue;
			}

			if ( LDAP_DN_PEDANTIC & flags ) {
				return( 1 );
			}
			/* 
			 * we do not allow escaping 
			 * of chars that don't need 
			 * to and do not belong to 
			 * HEXDIGITS
			 */
			return( 1 );

		} else if ( !LDAP_DN_ASCII_PRINTABLE( p[ 0 ] ) ) {
			if ( p[ 0 ] == '\0' ) {
				return( 1 );
			}
			*retFlags = LDAP_AVA_NONPRINTABLE;

		} else if ( ( LDAP_DN_LDAP( flags ) && LDAP_DN_VALUE_END_V2( p[ 0 ] ) ) 
				|| ( LDAP_DN_LDAPV3( flags ) && LDAP_DN_VALUE_END( p[ 0 ] ) ) ) {
			break;

		} else if ( LDAP_DN_NEEDESCAPE( p[ 0 ] ) ) {
			/* 
			 * FIXME: maybe we can add 
			 * escapes if not pedantic?
			 */
			return( 1 );
		}
	}

	/*
	 * we do allow unescaped spaces at the end
	 * of the value only in non-pedantic mode
	 */
	if ( p > startPos + 1 && LDAP_DN_ASCII_SPACE( p[ -1 ] ) &&
			!LDAP_DN_ESCAPE( p[ -2 ] ) ) {
		if ( flags & LDAP_DN_PEDANTIC ) {
			return( 1 );
		}

		/* strip trailing (unescaped) spaces */
		for ( endPos = p - 1; 
				endPos > startPos + 1 && 
				LDAP_DN_ASCII_SPACE( endPos[ -1 ] ) &&
				!LDAP_DN_ESCAPE( endPos[ -2 ] );
				endPos-- ) {
			/* no op */
		}
	}

	*next = p;
	if ( flags & LDAP_DN_SKIP ) {
		return( 0 );
	}

	/*
	 * FIXME: test memory?
	 */
	len = ( endPos ? endPos : p ) - startPos - escapes;
	val->bv_len = len;

	if ( escapes == 0 ) {
		if ( *retFlags & LDAP_AVA_NONPRINTABLE ) {
			val->bv_val = LDAP_MALLOCX( len + 1, ctx );
			if ( val->bv_val == NULL ) {
				return( 1 );
			}

			AC_MEMCPY( val->bv_val, startPos, len );
			val->bv_val[ len ] = '\0';
		} else {
			val->bv_val = LDAP_STRNDUPX( startPos, len, ctx );
		}

	} else {
		ber_len_t	s, d;

		val->bv_val = LDAP_MALLOCX( len + 1, ctx );
		if ( val->bv_val == NULL ) {
			return( 1 );
		}

		for ( s = 0, d = 0; d < len; ) {
			if ( LDAP_DN_ESCAPE( startPos[ s ] ) ) {
				s++;
				if ( LDAP_DN_MAYESCAPE( startPos[ s ] ) ) {
					val->bv_val[ d++ ] = 
						startPos[ s++ ];
					
				} else if ( LDAP_DN_HEXPAIR( &startPos[ s ] ) ) {
					char 	c;

					hexstr2bin( &startPos[ s ], &c );
					val->bv_val[ d++ ] = c;
					s += 2;
					
				} else {
					/* we should never get here */
					assert( 0 );
				}

			} else {
				val->bv_val[ d++ ] = startPos[ s++ ];
			}
		}

		val->bv_val[ d ] = '\0';
		assert( d == len );
	}

	return( 0 );
}

static int
DCE2strval( const char *str, struct berval *val, const char **next, unsigned flags, void *ctx )
{
	const char 	*p, *startPos, *endPos = NULL;
	ber_len_t	len, escapes;

	assert( str != NULL );
	assert( val != NULL );
	assert( next != NULL );

	*next = NULL;
	
	for ( startPos = p = str, escapes = 0; p[ 0 ]; p++ ) {
		if ( LDAP_DN_ESCAPE_DCE( p[ 0 ] ) ) {
			p++;
			if ( LDAP_DN_NEEDESCAPE_DCE( p[ 0 ] ) ) {
				escapes++;

			} else {
				return( 1 );
			}

		} else if ( LDAP_DN_VALUE_END_DCE( p[ 0 ] ) ) {
			break;
		}

		/*
		 * FIXME: can we accept anything else? I guess we need
		 * to stop if a value is not legal
		 */
	}

	/* 
	 * (unescaped) trailing spaces are trimmed must be silently ignored;
	 * so we eat them
	 */
	if ( p > startPos + 1 && LDAP_DN_ASCII_SPACE( p[ -1 ] ) &&
			!LDAP_DN_ESCAPE( p[ -2 ] ) ) {
		if ( flags & LDAP_DN_PEDANTIC ) {
			return( 1 );
		}

		/* strip trailing (unescaped) spaces */
		for ( endPos = p - 1; 
				endPos > startPos + 1 && 
				LDAP_DN_ASCII_SPACE( endPos[ -1 ] ) &&
				!LDAP_DN_ESCAPE( endPos[ -2 ] );
				endPos-- ) {
			/* no op */
		}
	}

	*next = p;
	if ( flags & LDAP_DN_SKIP ) {
		return( 0 );
	}
	
	len = ( endPos ? endPos : p ) - startPos - escapes;
	val->bv_len = len;
	if ( escapes == 0 ){
		val->bv_val = LDAP_STRNDUPX( startPos, len, ctx );

	} else {
		ber_len_t	s, d;

		val->bv_val = LDAP_MALLOCX( len + 1, ctx );
		if ( val->bv_val == NULL ) {
			return( 1 );
		}

		for ( s = 0, d = 0; d < len; ) {
			/*
			 * This point is reached only if escapes 
			 * are properly used, so all we need to
			 * do is eat them
			 */
			if (  LDAP_DN_ESCAPE_DCE( startPos[ s ] ) ) {
				s++;

			}
			val->bv_val[ d++ ] = startPos[ s++ ];
		}
		val->bv_val[ d ] = '\0';
		assert( strlen( val->bv_val ) == len );
	}
	
	return( 0 );
}

static int
IA52strval( const char *str, struct berval *val, const char **next, unsigned flags, void *ctx )
{
	const char 	*p, *startPos, *endPos = NULL;
	ber_len_t	len, escapes;

	assert( str != NULL );
	assert( val != NULL );
	assert( next != NULL );

	*next = NULL;

	/*
	 * LDAPv2 (RFC 1779)
	 */
	
	for ( startPos = p = str, escapes = 0; p[ 0 ]; p++ ) {
		if ( LDAP_DN_ESCAPE( p[ 0 ] ) ) {
			p++;
			if ( p[ 0 ] == '\0' ) {
				return( 1 );
			}

			if ( !LDAP_DN_NEEDESCAPE( p[ 0 ] )
					&& ( LDAP_DN_PEDANTIC & flags ) ) {
				return( 1 );
			}
			escapes++;

		} else if ( LDAP_DN_VALUE_END_V2( p[ 0 ] ) ) {
			break;
		}

		/*
		 * FIXME: can we accept anything else? I guess we need
		 * to stop if a value is not legal
		 */
	}

	/* strip trailing (unescaped) spaces */
	for ( endPos = p; 
			endPos > startPos + 1 && 
			LDAP_DN_ASCII_SPACE( endPos[ -1 ] ) &&
			!LDAP_DN_ESCAPE( endPos[ -2 ] );
			endPos-- ) {
		/* no op */
	}

	*next = p;
	if ( flags & LDAP_DN_SKIP ) {
		return( 0 );
	}

	len = ( endPos ? endPos : p ) - startPos - escapes;
	val->bv_len = len;
	if ( escapes == 0 ) {
		val->bv_val = LDAP_STRNDUPX( startPos, len, ctx );

	} else {
		ber_len_t	s, d;
		
		val->bv_val = LDAP_MALLOCX( len + 1, ctx );
		if ( val->bv_val == NULL ) {
			return( 1 );
		}

		for ( s = 0, d = 0; d < len; ) {
			if ( LDAP_DN_ESCAPE( startPos[ s ] ) ) {
				s++;
			}
			val->bv_val[ d++ ] = startPos[ s++ ];
		}
		val->bv_val[ d ] = '\0';
		assert( strlen( val->bv_val ) == len );
	}

	return( 0 );
}

static int
quotedIA52strval( const char *str, struct berval *val, const char **next, unsigned flags, void *ctx )
{
	const char 	*p, *startPos, *endPos = NULL;
	ber_len_t	len;
	unsigned	escapes = 0;

	assert( str != NULL );
	assert( val != NULL );
	assert( next != NULL );

	*next = NULL;

	/* initial quote already eaten */
	for ( startPos = p = str; p[ 0 ]; p++ ) {
		/* 
		 * According to RFC 1779, the quoted value can
		 * contain escaped as well as unescaped special values;
		 * as a consequence we tolerate escaped values 
		 * (e.g. '"\,"' -> '\,') and escape unescaped specials
		 * (e.g. '","' -> '\,').
		 */
		if ( LDAP_DN_ESCAPE( p[ 0 ] ) ) {
			if ( p[ 1 ] == '\0' ) {
				return( 1 );
			}
			p++;

			if ( !LDAP_DN_V2_PAIR( p[ 0 ] )
					&& ( LDAP_DN_PEDANTIC & flags ) ) {
				/*
				 * do we allow to escape normal chars?
				 * LDAPv2 does not allow any mechanism 
				 * for escaping chars with '\' and hex 
				 * pair
				 */
				return( 1 );
			}
			escapes++;

		} else if ( LDAP_DN_QUOTES( p[ 0 ] ) ) {
			endPos = p;
			/* eat closing quotes */
			p++;
			break;
		}

		/*
		 * FIXME: can we accept anything else? I guess we need
		 * to stop if a value is not legal
		 */
	}

	if ( endPos == NULL ) {
		return( 1 );
	}

	/* Strip trailing (unescaped) spaces */
	for ( ; p[ 0 ] && LDAP_DN_ASCII_SPACE( p[ 0 ] ); p++ ) {
		/* no op */
	}

	*next = p;
	if ( flags & LDAP_DN_SKIP ) {
		return( 0 );
	}

	len = endPos - startPos - escapes;
	assert( endPos >= startPos + escapes );
	val->bv_len = len;
	if ( escapes == 0 ) {
		val->bv_val = LDAP_STRNDUPX( startPos, len, ctx );

	} else {
		ber_len_t	s, d;
		
		val->bv_val = LDAP_MALLOCX( len + 1, ctx );
		if ( val->bv_val == NULL ) {
			return( 1 );
		}

		val->bv_len = len;

		for ( s = d = 0; d < len; ) {
			if ( LDAP_DN_ESCAPE( str[ s ] ) ) {
				s++;
			}
			val->bv_val[ d++ ] = str[ s++ ];
		}
		val->bv_val[ d ] = '\0';
		assert( strlen( val->bv_val ) == len );
	}

	return( 0 );
}

static int
hexstr2bin( const char *str, char *c )
{
	char	c1, c2;

	assert( str != NULL );
	assert( c != NULL );

	c1 = str[ 0 ];
	c2 = str[ 1 ];

	if ( LDAP_DN_ASCII_DIGIT( c1 ) ) {
		*c = c1 - '0';

	} else {
		if ( LDAP_DN_ASCII_UCASE_HEXALPHA( c1 ) ) {
			*c = c1 - 'A' + 10;
		} else {
			assert( LDAP_DN_ASCII_LCASE_HEXALPHA( c1 ) );
			*c = c1 - 'a' + 10;
		}
	}

	*c <<= 4;

	if ( LDAP_DN_ASCII_DIGIT( c2 ) ) {
		*c += c2 - '0';
		
	} else {
		if ( LDAP_DN_ASCII_UCASE_HEXALPHA( c2 ) ) {
			*c += c2 - 'A' + 10;
		} else {
			assert( LDAP_DN_ASCII_LCASE_HEXALPHA( c2 ) );
			*c += c2 - 'a' + 10;
		}
	}

	return( 0 );
}

static int
hexstr2binval( const char *str, struct berval *val, const char **next, unsigned flags, void *ctx )
{
	const char 	*p, *startPos, *endPos = NULL;
	ber_len_t	len;
	ber_len_t	s, d;

	assert( str != NULL );
	assert( val != NULL );
	assert( next != NULL );

	*next = NULL;

	for ( startPos = p = str; p[ 0 ]; p += 2 ) {
		switch ( LDAP_DN_FORMAT( flags ) ) {
		case LDAP_DN_FORMAT_LDAPV3:
			if ( LDAP_DN_VALUE_END( p[ 0 ] ) ) {
				goto end_of_value;
			}
			break;

		case LDAP_DN_FORMAT_LDAP:
		case LDAP_DN_FORMAT_LDAPV2:
			if ( LDAP_DN_VALUE_END_V2( p[ 0 ] ) ) {
				goto end_of_value;
			}
			break;

		case LDAP_DN_FORMAT_DCE:
			if ( LDAP_DN_VALUE_END_DCE( p[ 0 ] ) ) {
				goto end_of_value;
			}
			break;
		}

		if ( LDAP_DN_ASCII_SPACE( p[ 0 ] ) ) {
			if ( flags & LDAP_DN_PEDANTIC ) {
				return( 1 );
			}
			endPos = p;

			for ( ; p[ 0 ]; p++ ) {
				switch ( LDAP_DN_FORMAT( flags ) ) {
				case LDAP_DN_FORMAT_LDAPV3:
					if ( LDAP_DN_VALUE_END( p[ 0 ] ) ) {
						goto end_of_value;
					}
					break;

				case LDAP_DN_FORMAT_LDAP:
				case LDAP_DN_FORMAT_LDAPV2:
					if ( LDAP_DN_VALUE_END_V2( p[ 0 ] ) ) {
						goto end_of_value;
					}
					break;

				case LDAP_DN_FORMAT_DCE:
					if ( LDAP_DN_VALUE_END_DCE( p[ 0 ] ) ) {
						goto end_of_value;
					}
					break;
				}
			}
			break;
		}
		
		if ( !LDAP_DN_HEXPAIR( p ) ) {
			return( 1 );
		}
	}

end_of_value:;

	*next = p;
	if ( flags & LDAP_DN_SKIP ) {
		return( 0 );
	}

	len = ( ( endPos ? endPos : p ) - startPos ) / 2;
	/* must be even! */
	assert( 2 * len == (ber_len_t) (( endPos ? endPos : p ) - startPos ));

	val->bv_len = len;
	val->bv_val = LDAP_MALLOCX( len + 1, ctx );
	if ( val->bv_val == NULL ) {
		return( LDAP_NO_MEMORY );
	}

	for ( s = 0, d = 0; d < len; s += 2, d++ ) {
		char 	c;

		hexstr2bin( &startPos[ s ], &c );

		val->bv_val[ d ] = c;
	}

	val->bv_val[ d ] = '\0';

	return( 0 );
}

/*
 * convert a byte in a hexadecimal pair
 */
static int
byte2hexpair( const char *val, char *pair )
{
	static const char	hexdig[] = "0123456789ABCDEF";

	assert( val != NULL );
	assert( pair != NULL );

	/* 
	 * we assume the string has enough room for the hex encoding
	 * of the value
	 */

	pair[ 0 ] = hexdig[ 0x0f & ( val[ 0 ] >> 4 ) ];
	pair[ 1 ] = hexdig[ 0x0f & val[ 0 ] ];
	
	return( 0 );
}

/*
 * convert a binary value in hexadecimal pairs
 */
static int
binval2hexstr( struct berval *val, char *str )
{
	ber_len_t	s, d;

	assert( val != NULL );
	assert( str != NULL );

	if ( val->bv_len == 0 ) {
		return( 0 );
	}

	/* 
	 * we assume the string has enough room for the hex encoding
	 * of the value
	 */

	for ( s = 0, d = 0; s < val->bv_len; s++, d += 2 ) {
		byte2hexpair( &val->bv_val[ s ], &str[ d ] );
	}
	
	return( 0 );
}

/*
 * Length of the string representation, accounting for escaped hex
 * of UTF-8 chars
 */
static int
strval2strlen( struct berval *val, unsigned flags, ber_len_t *len )
{
	ber_len_t	l, cl = 1;
	char		*p, *end;
	int		escaped_byte_len = LDAP_DN_IS_PRETTY( flags ) ? 1 : 3;
#ifdef PRETTY_ESCAPE
	int		escaped_ascii_len = LDAP_DN_IS_PRETTY( flags ) ? 2 : 3;
#endif /* PRETTY_ESCAPE */
	
	assert( val != NULL );
	assert( len != NULL );

	*len = 0;
	if ( val->bv_len == 0 ) {
		return( 0 );
	}

	end = val->bv_val + val->bv_len - 1;
	for ( l = 0, p = val->bv_val; p <= end; p += cl ) {

		/* 
		 * escape '%x00' 
		 */
		if ( p[ 0 ] == '\0' ) {
			cl = 1;
			l += 3;
			continue;
		}

		cl = LDAP_UTF8_CHARLEN2( p, cl );
		if ( cl == 0 ) {
			/* illegal utf-8 char! */
			return( -1 );

		} else if ( cl > 1 ) {
			ber_len_t cnt;

			for ( cnt = 1; cnt < cl; cnt++ ) {
				if ( ( p[ cnt ] & 0xc0 ) != 0x80 ) {
					return( -1 );
				}
			}
			l += escaped_byte_len * cl;

		} else if ( LDAP_DN_NEEDESCAPE( p[ 0 ] )
				|| LDAP_DN_SHOULDESCAPE( p[ 0 ] )
				|| ( p == val->bv_val && LDAP_DN_NEEDESCAPE_LEAD( p[ 0 ] ) )
				|| ( p == end && LDAP_DN_NEEDESCAPE_TRAIL( p[ 0 ] ) ) ) {
#ifdef PRETTY_ESCAPE
#if 0
			if ( LDAP_DN_WILLESCAPE_HEX( flags, p[ 0 ] ) ) {
#else
			if ( LDAP_DN_WILLESCAPE_CHAR( p[ 0 ] ) ) {
#endif

				/* 
				 * there might be some chars we want 
				 * to escape in form of a couple 
				 * of hexdigits for optimization purposes
				 */
				l += 3;

			} else {
				l += escaped_ascii_len;
			}
#else /* ! PRETTY_ESCAPE */
			l += 3;
#endif /* ! PRETTY_ESCAPE */

		} else {
			l++;
		}
	}

	*len = l;

	return( 0 );
}

/*
 * convert to string representation, escaping with hex the UTF-8 stuff;
 * assume the destination has enough room for escaping
 */
static int
strval2str( struct berval *val, char *str, unsigned flags, ber_len_t *len )
{
	ber_len_t	s, d, end;

	assert( val != NULL );
	assert( str != NULL );
	assert( len != NULL );

	if ( val->bv_len == 0 ) {
		*len = 0;
		return( 0 );
	}

	/* 
	 * we assume the string has enough room for the hex encoding
	 * of the value
	 */
	for ( s = 0, d = 0, end = val->bv_len - 1; s < val->bv_len; ) {
		ber_len_t	cl;

		/* 
		 * escape '%x00' 
		 */
		if ( val->bv_val[ s ] == '\0' ) {
			cl = 1;
			str[ d++ ] = '\\';
			str[ d++ ] = '0';
			str[ d++ ] = '0';
			s++;
			continue;
		}
		
		/*
		 * The length was checked in strval2strlen();
		 */
		cl = LDAP_UTF8_CHARLEN( &val->bv_val[ s ] );
		
		/* 
		 * there might be some chars we want to escape in form
		 * of a couple of hexdigits for optimization purposes
		 */
		if ( ( cl > 1 && !LDAP_DN_IS_PRETTY( flags ) ) 
#ifdef PRETTY_ESCAPE
#if 0
				|| LDAP_DN_WILLESCAPE_HEX( flags, val->bv_val[ s ] ) 
#else
				|| LDAP_DN_WILLESCAPE_CHAR( val->bv_val[ s ] ) 
#endif
#else /* ! PRETTY_ESCAPE */
				|| LDAP_DN_NEEDESCAPE( val->bv_val[ s ] )
				|| LDAP_DN_SHOULDESCAPE( val->bv_val[ s ] )
				|| ( d == 0 && LDAP_DN_NEEDESCAPE_LEAD( val->bv_val[ s ] ) )
				|| ( s == end && LDAP_DN_NEEDESCAPE_TRAIL( val->bv_val[ s ] ) )

#endif /* ! PRETTY_ESCAPE */
				) {
			for ( ; cl--; ) {
				str[ d++ ] = '\\';
				byte2hexpair( &val->bv_val[ s ], &str[ d ] );
				s++;
				d += 2;
			}

		} else if ( cl > 1 ) {
			for ( ; cl--; ) {
				str[ d++ ] = val->bv_val[ s++ ];
			}

		} else {
#ifdef PRETTY_ESCAPE
			if ( LDAP_DN_NEEDESCAPE( val->bv_val[ s ] )
					|| LDAP_DN_SHOULDESCAPE( val->bv_val[ s ] )
					|| ( d == 0 && LDAP_DN_NEEDESCAPE_LEAD( val->bv_val[ s ] ) )
					|| ( s == end && LDAP_DN_NEEDESCAPE_TRAIL( val->bv_val[ s ] ) ) ) {
				str[ d++ ] = '\\';
				if ( !LDAP_DN_IS_PRETTY( flags ) ) {
					byte2hexpair( &val->bv_val[ s ], &str[ d ] );
					s++;
					d += 2;
					continue;
				}
			}
#endif /* PRETTY_ESCAPE */
			str[ d++ ] = val->bv_val[ s++ ];
		}
	}

	*len = d;
	
	return( 0 );
}

/*
 * Length of the IA5 string representation (no UTF-8 allowed)
 */
static int
strval2IA5strlen( struct berval *val, unsigned flags, ber_len_t *len )
{
	ber_len_t	l;
	char		*p;

	assert( val != NULL );
	assert( len != NULL );

	*len = 0;
	if ( val->bv_len == 0 ) {
		return( 0 );
	}

	if ( flags & LDAP_AVA_NONPRINTABLE ) {
		/*
		 * Turn value into a binary encoded BER
		 */
		return( -1 );

	} else {
		for ( l = 0, p = val->bv_val; p[ 0 ]; p++ ) {
			if ( LDAP_DN_NEEDESCAPE( p[ 0 ] )
					|| LDAP_DN_SHOULDESCAPE( p[ 0 ] )
					|| ( p == val->bv_val && LDAP_DN_NEEDESCAPE_LEAD( p[ 0 ] ) )
					|| ( !p[ 1 ] && LDAP_DN_NEEDESCAPE_TRAIL( p[ 0 ] ) ) ) {
				l += 2;

			} else {
				l++;
			}
		}
	}

	*len = l;
	
	return( 0 );
}

/*
 * convert to string representation (np UTF-8)
 * assume the destination has enough room for escaping
 */
static int
strval2IA5str( struct berval *val, char *str, unsigned flags, ber_len_t *len )
{
	ber_len_t	s, d, end;

	assert( val != NULL );
	assert( str != NULL );
	assert( len != NULL );

	if ( val->bv_len == 0 ) {
		*len = 0;
		return( 0 );
	}

	if ( flags & LDAP_AVA_NONPRINTABLE ) {
		/*
		 * Turn value into a binary encoded BER
		 */
		*len = 0;
		return( -1 );

	} else {
		/* 
		 * we assume the string has enough room for the hex encoding
		 * of the value
		 */

		for ( s = 0, d = 0, end = val->bv_len - 1; s < val->bv_len; ) {
			if ( LDAP_DN_NEEDESCAPE( val->bv_val[ s ] )
					|| LDAP_DN_SHOULDESCAPE( val->bv_val[ s ] )
					|| ( s == 0 && LDAP_DN_NEEDESCAPE_LEAD( val->bv_val[ s ] ) )
					|| ( s == end && LDAP_DN_NEEDESCAPE_TRAIL( val->bv_val[ s ] ) ) ) {
				str[ d++ ] = '\\';
			}
			str[ d++ ] = val->bv_val[ s++ ];
		}
	}

	*len = d;
	
	return( 0 );
}

/*
 * Length of the (supposedly) DCE string representation, 
 * accounting for escaped hex of UTF-8 chars
 */
static int
strval2DCEstrlen( struct berval *val, unsigned flags, ber_len_t *len )
{
	ber_len_t	l;
	char		*p;

	assert( val != NULL );
	assert( len != NULL );

	*len = 0;
	if ( val->bv_len == 0 ) {
		return( 0 );
	}

	if ( flags & LDAP_AVA_NONPRINTABLE ) {
		/* 
		 * FIXME: Turn the value into a binary encoded BER?
		 */
		return( -1 );
		
	} else {
		for ( l = 0, p = val->bv_val; p[ 0 ]; p++ ) {
			if ( LDAP_DN_NEEDESCAPE_DCE( p[ 0 ] ) ) {
				l += 2;

			} else {
				l++;
			}
		}
	}

	*len = l;

	return( 0 );
}

/*
 * convert to (supposedly) DCE string representation, 
 * escaping with hex the UTF-8 stuff;
 * assume the destination has enough room for escaping
 */
static int
strval2DCEstr( struct berval *val, char *str, unsigned flags, ber_len_t *len )
{
	ber_len_t	s, d;

	assert( val != NULL );
	assert( str != NULL );
	assert( len != NULL );

	if ( val->bv_len == 0 ) {
		*len = 0;
		return( 0 );
	}

	if ( flags & LDAP_AVA_NONPRINTABLE ) {
		/*
		 * FIXME: Turn the value into a binary encoded BER?
		 */
		*len = 0;
		return( -1 );
		
	} else {

		/* 
		 * we assume the string has enough room for the hex encoding
		 * of the value
		 */

		for ( s = 0, d = 0; s < val->bv_len; ) {
			if ( LDAP_DN_NEEDESCAPE_DCE( val->bv_val[ s ] ) ) {
				str[ d++ ] = '\\';
			}
			str[ d++ ] = val->bv_val[ s++ ];
		}
	}

	*len = d;
	
	return( 0 );
}

/*
 * Length of the (supposedly) AD canonical string representation, 
 * accounting for chars that need to be escaped 
 */
static int
strval2ADstrlen( struct berval *val, unsigned flags, ber_len_t *len )
{
	ber_len_t	l, cl;
	char		*p;

	assert( val != NULL );
	assert( len != NULL );

	*len = 0;
	if ( val->bv_len == 0 ) {
		return( 0 );
	}

	for ( l = 0, p = val->bv_val; p[ 0 ]; p += cl ) {
		cl = LDAP_UTF8_CHARLEN2( p, cl );
		if ( cl == 0 ) {
			/* illegal utf-8 char */
			return -1;
		} else if ( (cl == 1) && LDAP_DN_NEEDESCAPE_AD( p[ 0 ] ) ) {
			l += 2;
		} else {
			l += cl;
		}
	}

	*len = l;

	return( 0 );
}

/*
 * convert to (supposedly) AD string representation,
 * assume the destination has enough room for escaping
 */
static int
strval2ADstr( struct berval *val, char *str, unsigned flags, ber_len_t *len )
{
	ber_len_t	s, d, cl;

	assert( val != NULL );
	assert( str != NULL );
	assert( len != NULL );

	if ( val->bv_len == 0 ) {
		*len = 0;
		return( 0 );
	}

	/* 
	 * we assume the string has enough room for the escaping
	 * of the value
	 */

	for ( s = 0, d = 0; s < val->bv_len; ) {
		cl = LDAP_UTF8_CHARLEN2( val->bv_val+s, cl );
		if ( cl == 0 ) {
			/* illegal utf-8 char */
			return -1;
		} else if ( (cl == 1) && LDAP_DN_NEEDESCAPE_AD(val->bv_val[ s ]) ) {
			str[ d++ ] = '\\';
		}
		for (; cl--;) {
			str[ d++ ] = val->bv_val[ s++ ];
		}
	}

	*len = d;
	
	return( 0 );
}

/*
 * If the DN is terminated by single-AVA RDNs with attribute type of "dc",
 * the first part of the AD representation of the DN is written in DNS
 * form, i.e. dot separated domain name components (as suggested 
 * by Luke Howard, http://www.padl.com/~lukeh)
 */
static int
dn2domain( LDAPDN dn, struct berval *bv, int pos, int *iRDN )
{
	int 		i;
	int		domain = 0, first = 1;
	ber_len_t	l = 1; /* we move the null also */
	char		*str;

	/* we are guaranteed there's enough memory in str */

	/* sanity */
	assert( dn != NULL );
	assert( bv != NULL );
	assert( iRDN != NULL );
	assert( *iRDN >= 0 );

	str = bv->bv_val + pos;

	for ( i = *iRDN; i >= 0; i-- ) {
		LDAPRDN		rdn;
		LDAPAVA		*ava;

		assert( dn[ i ] != NULL );
		rdn = dn[ i ];

		assert( rdn[ 0 ] != NULL );
		ava = rdn[ 0 ];

		if ( !LDAP_DN_IS_RDN_DC( rdn ) ) {
			break;
		}

		if ( ldif_is_not_printable( ava->la_value.bv_val, ava->la_value.bv_len ) ) {
			domain = 0;
			break;
		}

		domain = 1;
		
		if ( first ) {
			first = 0;
			AC_MEMCPY( str, ava->la_value.bv_val, 
					ava->la_value.bv_len + 1);
			l += ava->la_value.bv_len;

		} else {
			AC_MEMCPY( str + ava->la_value.bv_len + 1, bv->bv_val + pos, l);
			AC_MEMCPY( str, ava->la_value.bv_val, 
					ava->la_value.bv_len );
			str[ ava->la_value.bv_len ] = '.';
			l += ava->la_value.bv_len + 1;
		}
	}

	*iRDN = i;
	bv->bv_len = pos + l - 1;

	return( domain );
}

static int
rdn2strlen( LDAPRDN rdn, unsigned flags, ber_len_t *len,
	 int ( *s2l )( struct berval *v, unsigned f, ber_len_t *l ) )
{
	int		iAVA;
	ber_len_t	l = 0;

	*len = 0;

	for ( iAVA = 0; rdn[ iAVA ]; iAVA++ ) {
		LDAPAVA		*ava = rdn[ iAVA ];

		/* len(type) + '=' + '+' | ',' */
		l += ava->la_attr.bv_len + 2;

		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			/* octothorpe + twice the length */
			l += 1 + 2 * ava->la_value.bv_len;

		} else {
			ber_len_t	vl;
			unsigned	f = flags | ava->la_flags;
			
			if ( ( *s2l )( &ava->la_value, f, &vl ) ) {
				return( -1 );
			}
			l += vl;
		}
	}
	if ( !iAVA )
		return( -1 );	/* RDN ::= SET SIZE (1..MAX) OF AVA */
	
	*len = l;
	
	return( 0 );
}

static int
rdn2str( LDAPRDN rdn, char *str, unsigned flags, ber_len_t *len,
	int ( *s2s ) ( struct berval *v, char * s, unsigned f, ber_len_t *l ) )
{
	int		iAVA;
	ber_len_t	l = 0;

	for ( iAVA = 0; rdn[ iAVA ]; iAVA++ ) {
		LDAPAVA		*ava = rdn[ iAVA ];

		AC_MEMCPY( &str[ l ], ava->la_attr.bv_val, 
				ava->la_attr.bv_len );
		l += ava->la_attr.bv_len;

		str[ l++ ] = '=';

		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			str[ l++ ] = '#';
			if ( binval2hexstr( &ava->la_value, &str[ l ] ) ) {
				return( -1 );
			}
			l += 2 * ava->la_value.bv_len;

		} else {
			ber_len_t	vl;
			unsigned	f = flags | ava->la_flags;

			if ( ( *s2s )( &ava->la_value, &str[ l ], f, &vl ) ) {
				return( -1 );
			}
			l += vl;
		}
		str[ l++ ] = ( rdn[ iAVA + 1] ? '+' : ',' );
	}

	*len = l;

	return( 0 );
}

static int
rdn2DCEstrlen( LDAPRDN rdn, unsigned flags, ber_len_t *len )
{
	int		iAVA;
	ber_len_t	l = 0;

	*len = 0;

	for ( iAVA = 0; rdn[ iAVA ]; iAVA++ ) {
		LDAPAVA		*ava = rdn[ iAVA ];

		/* len(type) + '=' + ',' | '/' */
		l += ava->la_attr.bv_len + 2;

		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			/* octothorpe + twice the length */
			l += 1 + 2 * ava->la_value.bv_len;
		} else {
			ber_len_t	vl;
			unsigned	f = flags | ava->la_flags;
			
			if ( strval2DCEstrlen( &ava->la_value, f, &vl ) ) {
				return( -1 );
			}
			l += vl;
		}
	}
	if ( !iAVA )
		return( -1 );	/* RDN ::= SET SIZE (1..MAX) OF AVA */
	
	*len = l;
	
	return( 0 );
}

static int
rdn2DCEstr( LDAPRDN rdn, char *str, unsigned flags, ber_len_t *len, int first )
{
	int		iAVA;
	ber_len_t	l = 0;

	for ( iAVA = 0; rdn[ iAVA ]; iAVA++ ) {
		LDAPAVA		*ava = rdn[ iAVA ];

		if ( first ) {
			first = 0;
		} else {
			str[ l++ ] = ( iAVA ? ',' : '/' );
		}

		AC_MEMCPY( &str[ l ], ava->la_attr.bv_val, 
				ava->la_attr.bv_len );
		l += ava->la_attr.bv_len;

		str[ l++ ] = '=';

		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			str[ l++ ] = '#';
			if ( binval2hexstr( &ava->la_value, &str[ l ] ) ) {
				return( -1 );
			}
			l += 2 * ava->la_value.bv_len;
		} else {
			ber_len_t	vl;
			unsigned	f = flags | ava->la_flags;

			if ( strval2DCEstr( &ava->la_value, &str[ l ], f, &vl ) ) {
				return( -1 );
			}
			l += vl;
		}
	}

	*len = l;

	return( 0 );
}

static int
rdn2UFNstrlen( LDAPRDN rdn, unsigned flags, ber_len_t *len )
{
	int		iAVA;
	ber_len_t	l = 0;

	assert( rdn != NULL );
	assert( len != NULL );

	*len = 0;

	for ( iAVA = 0; rdn[ iAVA ]; iAVA++ ) {
		LDAPAVA		*ava = rdn[ iAVA ];

		/* ' + ' | ', ' */
		l += ( rdn[ iAVA + 1 ] ? 3 : 2 );

		/* FIXME: are binary values allowed in UFN? */
		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			/* octothorpe + twice the value */
			l += 1 + 2 * ava->la_value.bv_len;

		} else {
			ber_len_t	vl;
			unsigned	f = flags | ava->la_flags;

			if ( strval2strlen( &ava->la_value, f, &vl ) ) {
				return( -1 );
			}
			l += vl;
		}
	}
	if ( !iAVA )
		return( -1 );	/* RDN ::= SET SIZE (1..MAX) OF AVA */
	
	*len = l;
	
	return( 0 );
}

static int
rdn2UFNstr( LDAPRDN rdn, char *str, unsigned flags, ber_len_t *len )
{
	int		iAVA;
	ber_len_t	l = 0;

	for ( iAVA = 0; rdn[ iAVA ]; iAVA++ ) {
		LDAPAVA		*ava = rdn[ iAVA ];

		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			str[ l++ ] = '#';
			if ( binval2hexstr( &ava->la_value, &str[ l ] ) ) {
				return( -1 );
			}
			l += 2 * ava->la_value.bv_len;
			
		} else {
			ber_len_t	vl;
			unsigned	f = flags | ava->la_flags;
			
			if ( strval2str( &ava->la_value, &str[ l ], f, &vl ) ) {
				return( -1 );
			}
			l += vl;
		}

		if ( rdn[ iAVA + 1 ] ) {
			AC_MEMCPY( &str[ l ], " + ", 3 );
			l += 3;

		} else {
			AC_MEMCPY( &str[ l ], ", ", 2 );
			l += 2;
		}
	}

	*len = l;

	return( 0 );
}

static int
rdn2ADstrlen( LDAPRDN rdn, unsigned flags, ber_len_t *len )
{
	int		iAVA;
	ber_len_t	l = 0;

	assert( rdn != NULL );
	assert( len != NULL );

	*len = 0;

	for ( iAVA = 0; rdn[ iAVA ]; iAVA++ ) {
		LDAPAVA		*ava = rdn[ iAVA ];

		/* ',' | '/' */
		l++;

		/* FIXME: are binary values allowed in UFN? */
		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			/* octothorpe + twice the value */
			l += 1 + 2 * ava->la_value.bv_len;
		} else {
			ber_len_t	vl;
			unsigned	f = flags | ava->la_flags;

			if ( strval2ADstrlen( &ava->la_value, f, &vl ) ) {
				return( -1 );
			}
			l += vl;
		}
	}
	if ( !iAVA )
		return( -1 );	/* RDN ::= SET SIZE (1..MAX) OF AVA */
	
	*len = l;
	
	return( 0 );
}

static int
rdn2ADstr( LDAPRDN rdn, char *str, unsigned flags, ber_len_t *len, int first )
{
	int		iAVA;
	ber_len_t	l = 0;

	for ( iAVA = 0; rdn[ iAVA ]; iAVA++ ) {
		LDAPAVA		*ava = rdn[ iAVA ];

		if ( first ) {
			first = 0;
		} else {
			str[ l++ ] = ( iAVA ? ',' : '/' );
		}

		if ( ava->la_flags & LDAP_AVA_BINARY ) {
			str[ l++ ] = '#';
			if ( binval2hexstr( &ava->la_value, &str[ l ] ) ) {
				return( -1 );
			}
			l += 2 * ava->la_value.bv_len;
		} else {
			ber_len_t	vl;
			unsigned	f = flags | ava->la_flags;
			
			if ( strval2ADstr( &ava->la_value, &str[ l ], f, &vl ) ) {
				return( -1 );
			}
			l += vl;
		}
	}

	*len = l;

	return( 0 );
}

/*
 * ldap_rdn2str
 *
 * Returns in str a string representation of rdn based on flags.
 * There is some duplication of code between this and ldap_dn2str;
 * this is wanted to reduce the allocation of temporary buffers.
 */
int
ldap_rdn2str( LDAPRDN rdn, char **str, unsigned flags )
{
	struct berval bv;
	int rc;

	assert( str != NULL );

	if((flags & LDAP_DN_FORMAT_MASK) == LDAP_DN_FORMAT_LBER) {
		return LDAP_PARAM_ERROR;
	}

	rc = ldap_rdn2bv_x( rdn, &bv, flags, NULL );
	*str = bv.bv_val;
	return rc;
}

int
ldap_rdn2bv( LDAPRDN rdn, struct berval *bv, unsigned flags )
{
	return ldap_rdn2bv_x( rdn, bv, flags, NULL );
}

int
ldap_rdn2bv_x( LDAPRDN rdn, struct berval *bv, unsigned flags, void *ctx )
{
	int		rc, back;
	ber_len_t	l;
	
	assert( bv != NULL );

	bv->bv_len = 0;
	bv->bv_val = NULL;

	if ( rdn == NULL ) {
		bv->bv_val = LDAP_STRDUPX( "", ctx );
		return( LDAP_SUCCESS );
	}

	/*
	 * This routine wastes "back" bytes at the end of the string
	 */

	switch ( LDAP_DN_FORMAT( flags ) ) {
	case LDAP_DN_FORMAT_LDAPV3:
		if ( rdn2strlen( rdn, flags, &l, strval2strlen ) ) {
			return LDAP_DECODING_ERROR;
		}
		break;

	case LDAP_DN_FORMAT_LDAPV2:
		if ( rdn2strlen( rdn, flags, &l, strval2IA5strlen ) ) {
			return LDAP_DECODING_ERROR;
		}
		break;

	case LDAP_DN_FORMAT_UFN:
		if ( rdn2UFNstrlen( rdn, flags, &l ) ) {
			return LDAP_DECODING_ERROR;
		}
		break;

	case LDAP_DN_FORMAT_DCE:
		if ( rdn2DCEstrlen( rdn, flags, &l ) ) {
			return LDAP_DECODING_ERROR;
		}
		break;

	case LDAP_DN_FORMAT_AD_CANONICAL:
		if ( rdn2ADstrlen( rdn, flags, &l ) ) {
			return LDAP_DECODING_ERROR;
		}
		break;

	default:
		return LDAP_PARAM_ERROR;
	}

	bv->bv_val = LDAP_MALLOCX( l + 1, ctx );
	if ( bv->bv_val == NULL ) {
		return LDAP_NO_MEMORY;
	}

	switch ( LDAP_DN_FORMAT( flags ) ) {
	case LDAP_DN_FORMAT_LDAPV3:
		rc = rdn2str( rdn, bv->bv_val, flags, &l, strval2str );
		back = 1;
		break;

	case LDAP_DN_FORMAT_LDAPV2:
		rc = rdn2str( rdn, bv->bv_val, flags, &l, strval2IA5str );
		back = 1;
		break;

	case LDAP_DN_FORMAT_UFN:
		rc = rdn2UFNstr( rdn, bv->bv_val, flags, &l );
		back = 2;
		break;

	case LDAP_DN_FORMAT_DCE:
		rc = rdn2DCEstr( rdn, bv->bv_val, flags, &l, 1 );
		back = 0;
		break;

	case LDAP_DN_FORMAT_AD_CANONICAL:
		rc = rdn2ADstr( rdn, bv->bv_val, flags, &l, 1 );
		back = 0;
		break;

	default:
		/* need at least one of the previous */
		return LDAP_PARAM_ERROR;
	}

	if ( rc ) {
		LDAP_FREEX( bv->bv_val, ctx );
		return rc;
	}

	bv->bv_len = l - back;
	bv->bv_val[ bv->bv_len ] = '\0';

	return LDAP_SUCCESS;
}

/*
 * Very bulk implementation; many optimizations can be performed
 *   - a NULL dn results in an empty string ""
 * 
 * FIXME: doubts
 *   a) what do we do if a UTF-8 string must be converted in LDAPv2?
 *      we must encode it in binary form ('#' + HEXPAIRs)
 *   b) does DCE/AD support UTF-8?
 *      no clue; don't think so.
 *   c) what do we do when binary values must be converted in UTF/DCE/AD?
 *      use binary encoded BER
 */ 
int ldap_dn2str( LDAPDN dn, char **str, unsigned flags )
{
	struct berval bv;
	int rc;

	assert( str != NULL );

	if((flags & LDAP_DN_FORMAT_MASK) == LDAP_DN_FORMAT_LBER) {
		return LDAP_PARAM_ERROR;
	}
	
	rc = ldap_dn2bv_x( dn, &bv, flags, NULL );
	*str = bv.bv_val;
	return rc;
}

int ldap_dn2bv( LDAPDN dn, struct berval *bv, unsigned flags )
{
	return ldap_dn2bv_x( dn, bv, flags, NULL );
}

int ldap_dn2bv_x( LDAPDN dn, struct berval *bv, unsigned flags, void *ctx )
{
	int		iRDN;
	int		rc = LDAP_ENCODING_ERROR;
	ber_len_t	len, l;

	/* stringifying helpers for LDAPv3/LDAPv2 */
	int ( *sv2l ) ( struct berval *v, unsigned f, ber_len_t *l );
	int ( *sv2s ) ( struct berval *v, char *s, unsigned f, ber_len_t *l );

	assert( bv != NULL );
	bv->bv_len = 0;
	bv->bv_val = NULL;

	Debug1( LDAP_DEBUG_ARGS, "=> ldap_dn2bv(%u)\n", flags );

	/* 
	 * a null dn means an empty dn string 
	 * FIXME: better raise an error?
	 */
	if ( dn == NULL || dn[0] == NULL ) {
		bv->bv_val = LDAP_STRDUPX( "", ctx );
		return( LDAP_SUCCESS );
	}

	switch ( LDAP_DN_FORMAT( flags ) ) {
	case LDAP_DN_FORMAT_LDAPV3:
		sv2l = strval2strlen;
		sv2s = strval2str;

		if( 0 ) {
	case LDAP_DN_FORMAT_LDAPV2:
			sv2l = strval2IA5strlen;
			sv2s = strval2IA5str;
		}

		for ( iRDN = 0, len = 0; dn[ iRDN ]; iRDN++ ) {
			ber_len_t	rdnl;
			if ( rdn2strlen( dn[ iRDN ], flags, &rdnl, sv2l ) ) {
				goto return_results;
			}

			len += rdnl;
		}

		if ( ( bv->bv_val = LDAP_MALLOCX( len + 1, ctx ) ) == NULL ) {
			rc = LDAP_NO_MEMORY;
			break;
		}

		for ( l = 0, iRDN = 0; dn[ iRDN ]; iRDN++ ) {
			ber_len_t	rdnl;
			
			if ( rdn2str( dn[ iRDN ], &bv->bv_val[ l ], flags, 
					&rdnl, sv2s ) ) {
				LDAP_FREEX( bv->bv_val, ctx );
				bv->bv_val = NULL;
				goto return_results;
			}
			l += rdnl;
		}

		assert( l == len );

		/* 
		 * trim the last ',' (the allocated memory 
		 * is one byte longer than required)
		 */
		bv->bv_len = len - 1;
		bv->bv_val[ bv->bv_len ] = '\0';

		rc = LDAP_SUCCESS;
		break;

	case LDAP_DN_FORMAT_UFN: {
		/*
		 * FIXME: quoting from RFC 1781:
		 *
   To take a distinguished name, and generate a name of this format with
   attribute types omitted, the following steps are followed.

    1.  If the first attribute is of type CommonName, the type may be
	omitted.

    2.  If the last attribute is of type Country, the type may be
        omitted.

    3.  If the last attribute is of type Country, the last
        Organisation attribute may have the type omitted.

    4.  All attributes of type OrganisationalUnit may have the type
        omitted, unless they are after an Organisation attribute or
        the first attribute is of type OrganisationalUnit.

         * this should be the pedantic implementation.
		 *
		 * Here the standard implementation reflects
		 * the one historically provided by OpenLDAP
		 * (and UMIch, I presume), with the variant
		 * of spaces and plusses (' + ') separating 
		 * rdn components.
		 * 
		 * A non-standard but nice implementation could
		 * be to turn the  final "dc" attributes into a 
		 * dot-separated domain.
		 *
		 * Other improvements could involve the use of
		 * friendly country names and so.
		 */
#ifdef DC_IN_UFN
		int	leftmost_dc = -1;
		int	last_iRDN = -1;
#endif /* DC_IN_UFN */

		for ( iRDN = 0, len = 0; dn[ iRDN ]; iRDN++ ) {
			ber_len_t	rdnl;
			
			if ( rdn2UFNstrlen( dn[ iRDN ], flags, &rdnl ) ) {
				goto return_results;
			}
			len += rdnl;

#ifdef DC_IN_UFN
			if ( LDAP_DN_IS_RDN_DC( dn[ iRDN ] ) ) {
				if ( leftmost_dc == -1 ) {
					leftmost_dc = iRDN;
				}
			} else {
				leftmost_dc = -1;
			}
#endif /* DC_IN_UFN */
		}

		if ( ( bv->bv_val = LDAP_MALLOCX( len + 1, ctx ) ) == NULL ) {
			rc = LDAP_NO_MEMORY;
			break;
		}

#ifdef DC_IN_UFN
		if ( leftmost_dc == -1 ) {
#endif /* DC_IN_UFN */
			for ( l = 0, iRDN = 0; dn[ iRDN ]; iRDN++ ) {
				ber_len_t	vl;
			
				if ( rdn2UFNstr( dn[ iRDN ], &bv->bv_val[ l ], 
						flags, &vl ) ) {
					LDAP_FREEX( bv->bv_val, ctx );
					bv->bv_val = NULL;
					goto return_results;
				}
				l += vl;
			}

			/* 
			 * trim the last ', ' (the allocated memory 
			 * is two bytes longer than required)
			 */
			bv->bv_len = len - 2;
			bv->bv_val[ bv->bv_len ] = '\0';
#ifdef DC_IN_UFN
		} else {
			last_iRDN = iRDN - 1;

			for ( l = 0, iRDN = 0; iRDN < leftmost_dc; iRDN++ ) {
				ber_len_t	vl;
			
				if ( rdn2UFNstr( dn[ iRDN ], &bv->bv_val[ l ], 
						flags, &vl ) ) {
					LDAP_FREEX( bv->bv_val, ctx );
					bv->bv_val = NULL;
					goto return_results;
				}
				l += vl;
			}

			if ( !dn2domain( dn, bv, l, &last_iRDN ) ) {
				LDAP_FREEX( bv->bv_val, ctx );
				bv->bv_val = NULL;
				goto return_results;
			}

			/* the string is correctly terminated by dn2domain */
		}
#endif /* DC_IN_UFN */
		
		rc = LDAP_SUCCESS;

	} break;

	case LDAP_DN_FORMAT_DCE:
		for ( iRDN = 0, len = 0; dn[ iRDN ]; iRDN++ ) {
			ber_len_t	rdnl;
			if ( rdn2DCEstrlen( dn[ iRDN ], flags, &rdnl ) ) {
				goto return_results;
			}

			len += rdnl;
		}

		if ( ( bv->bv_val = LDAP_MALLOCX( len + 1, ctx ) ) == NULL ) {
			rc = LDAP_NO_MEMORY;
			break;
		}

		for ( l = 0; iRDN--; ) {
			ber_len_t	rdnl;
			
			if ( rdn2DCEstr( dn[ iRDN ], &bv->bv_val[ l ], flags, 
					&rdnl, 0 ) ) {
				LDAP_FREEX( bv->bv_val, ctx );
				bv->bv_val = NULL;
				goto return_results;
			}
			l += rdnl;
		}

		assert( l == len );

		bv->bv_len = len;
		bv->bv_val[ bv->bv_len ] = '\0';

		rc = LDAP_SUCCESS;
		break;

	case LDAP_DN_FORMAT_AD_CANONICAL: {
		int	trailing_slash = 1;

		/*
		 * Sort of UFN for DCE DNs: a slash ('/') separated
		 * global->local DN with no types; strictly speaking,
		 * the naming context should be a domain, which is
		 * written in DNS-style, e.g. dot-separated.
		 * 
		 * Example:
		 * 
		 * 	"givenName=Bill+sn=Gates,ou=People,dc=microsoft,dc=com"
		 *
		 * will read
		 * 
		 * 	"microsoft.com/People/Bill,Gates"
		 */ 
		for ( iRDN = 0, len = -1; dn[ iRDN ]; iRDN++ ) {
			ber_len_t	rdnl;
			
			if ( rdn2ADstrlen( dn[ iRDN ], flags, &rdnl ) ) {
				goto return_results;
			}

			len += rdnl;
		}

		/* reserve room for trailing '/' in case the DN 
		 * is exactly a domain */
		if ( ( bv->bv_val = LDAP_MALLOCX( len + 1 + 1, ctx ) ) == NULL )
		{
			rc = LDAP_NO_MEMORY;
			break;
		}

		iRDN--;
		if ( iRDN && dn2domain( dn, bv, 0, &iRDN ) != 0 ) {
			for ( l = bv->bv_len; iRDN >= 0 ; iRDN-- ) {
				ber_len_t	rdnl;

				trailing_slash = 0;
			
				if ( rdn2ADstr( dn[ iRDN ], &bv->bv_val[ l ], 
						flags, &rdnl, 0 ) ) {
					LDAP_FREEX( bv->bv_val, ctx );
					bv->bv_val = NULL;
					goto return_results;
				}
				l += rdnl;
			}

		} else {
			int		first = 1;

			/*
			 * Strictly speaking, AD canonical requires
			 * a DN to be in the form "..., dc=smtg",
			 * i.e. terminated by a domain component
			 */
			if ( flags & LDAP_DN_PEDANTIC ) {
				LDAP_FREEX( bv->bv_val, ctx );
				bv->bv_val = NULL;
				rc = LDAP_ENCODING_ERROR;
				break;
			}

			for ( l = 0; iRDN >= 0 ; iRDN-- ) {
				ber_len_t	rdnl;
			
				if ( rdn2ADstr( dn[ iRDN ], &bv->bv_val[ l ], 
						flags, &rdnl, first ) ) {
					LDAP_FREEX( bv->bv_val, ctx );
					bv->bv_val = NULL;
					goto return_results;
				}
				if ( first ) {
					first = 0;
				}
				l += rdnl;
			}
		}

		if ( trailing_slash ) {
			/* the DN is exactly a domain -- need a trailing
			 * slash; room was reserved in advance */
			bv->bv_val[ len ] = '/';
			len++;
		}

		bv->bv_len = len;
		bv->bv_val[ bv->bv_len ] = '\0';

		rc = LDAP_SUCCESS;
	} break;

	default:
		return LDAP_PARAM_ERROR;
	}

	Debug3( LDAP_DEBUG_ARGS, "<= ldap_dn2bv(%s)=%d %s\n",
		bv->bv_val, rc, rc ? ldap_err2string( rc ) : "" );

return_results:;
	return( rc );
}

