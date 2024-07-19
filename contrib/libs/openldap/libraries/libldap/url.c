/* LIBLDAP url.c -- LDAP URL (RFC 4516) related routines */
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
/* Portions Copyright (c) 1996 Regents of the University of Michigan.
 * All rights reserved.
 */


/*
 *  LDAP URLs look like this:
 *    [p]ldap[is]://host[:port][/[dn[?[attributes][?[scope][?[filter][?exts]]]]]]
 *
 *  where:
 *   attributes is a comma separated list
 *   scope is one of these three strings:  base one sub (default=base)
 *   filter is an string-represented filter as in RFC 4515
 *
 *  e.g.,  ldap://host:port/dc=com?o,cn?base?(o=openldap)?extension
 *
 *  We also tolerate URLs that look like: <ldapurl> and <URL:ldapurl>
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>
#include <ac/ctype.h>

#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

/* local functions */
static const char* skip_url_prefix LDAP_P((
	const char *url,
	int *enclosedp,
	const char **scheme ));

int ldap_pvt_url_scheme2proto( const char *scheme )
{
	assert( scheme != NULL );

	if( scheme == NULL ) {
		return -1;
	}

	if( strcmp("ldap", scheme) == 0 || strcmp("pldap", scheme) == 0 ) {
		return LDAP_PROTO_TCP;
	}

	if( strcmp("ldapi", scheme) == 0 ) {
		return LDAP_PROTO_IPC;
	}

	if( strcmp("ldaps", scheme) == 0 || strcmp("pldaps", scheme) == 0 ) {
		return LDAP_PROTO_TCP;
	}
#ifdef LDAP_CONNECTIONLESS
	if( strcmp("cldap", scheme) == 0 ) {
		return LDAP_PROTO_UDP;
	}
#endif

	return -1;
}

int ldap_pvt_url_scheme_port( const char *scheme, int port )
{
	assert( scheme != NULL );

	if( port ) return port;
	if( scheme == NULL ) return port;

	if( strcmp("ldap", scheme) == 0 || strcmp("pldap", scheme) == 0 ) {
		return LDAP_PORT;
	}

	if( strcmp("ldapi", scheme) == 0 ) {
		return -1;
	}

	if( strcmp("ldaps", scheme) == 0 || strcmp("pldaps", scheme) == 0 ) {
		return LDAPS_PORT;
	}

#ifdef LDAP_CONNECTIONLESS
	if( strcmp("cldap", scheme) == 0 ) {
		return LDAP_PORT;
	}
#endif

	return -1;
}

int
ldap_pvt_url_scheme2tls( const char *scheme )
{
	assert( scheme != NULL );

	if( scheme == NULL ) {
		return -1;
	}

	return strcmp("ldaps", scheme) == 0 || strcmp("pldaps", scheme) == 0;
}

int
ldap_pvt_url_scheme2proxied( const char *scheme )
{
	assert( scheme != NULL );

	if( scheme == NULL ) {
		return -1;
	}

	return strcmp("pldap", scheme) == 0 || strcmp("pldaps", scheme) == 0;
}

int
ldap_is_ldap_url( LDAP_CONST char *url )
{
	int	enclosed;
	const char * scheme;

	if( url == NULL ) {
		return 0;
	}

	if( skip_url_prefix( url, &enclosed, &scheme ) == NULL ) {
		return 0;
	}

	return 1;
}

int
ldap_is_ldaps_url( LDAP_CONST char *url )
{
	int	enclosed;
	const char * scheme;

	if( url == NULL ) {
		return 0;
	}

	if( skip_url_prefix( url, &enclosed, &scheme ) == NULL ) {
		return 0;
	}

	return strcmp(scheme, "ldaps") == 0 || strcmp(scheme, "pldaps") == 0;
}

int
ldap_is_ldapi_url( LDAP_CONST char *url )
{
	int	enclosed;
	const char * scheme;

	if( url == NULL ) {
		return 0;
	}

	if( skip_url_prefix( url, &enclosed, &scheme ) == NULL ) {
		return 0;
	}

	return strcmp(scheme, "ldapi") == 0;
}

#ifdef LDAP_CONNECTIONLESS
int
ldap_is_ldapc_url( LDAP_CONST char *url )
{
	int	enclosed;
	const char * scheme;

	if( url == NULL ) {
		return 0;
	}

	if( skip_url_prefix( url, &enclosed, &scheme ) == NULL ) {
		return 0;
	}

	return strcmp(scheme, "cldap") == 0;
}
#endif

static const char*
skip_url_prefix(
	const char *url,
	int *enclosedp,
	const char **scheme )
{
	/*
 	 * return non-zero if this looks like a LDAP URL; zero if not
 	 * if non-zero returned, *urlp will be moved past "ldap://" part of URL
 	 */
	const char *p;

	if ( url == NULL ) {
		return( NULL );
	}

	p = url;

	/* skip leading '<' (if any) */
	if ( *p == '<' ) {
		*enclosedp = 1;
		++p;
	} else {
		*enclosedp = 0;
	}

	/* skip leading "URL:" (if any) */
	if ( strncasecmp( p, LDAP_URL_URLCOLON, LDAP_URL_URLCOLON_LEN ) == 0 ) {
		p += LDAP_URL_URLCOLON_LEN;
	}

	/* check for "ldap://" prefix */
	if ( strncasecmp( p, LDAP_URL_PREFIX, LDAP_URL_PREFIX_LEN ) == 0 ) {
		/* skip over "ldap://" prefix and return success */
		p += LDAP_URL_PREFIX_LEN;
		*scheme = "ldap";
		return( p );
	}

	/* check for "pldap://" prefix */
	if ( strncasecmp( p, PLDAP_URL_PREFIX, PLDAP_URL_PREFIX_LEN ) == 0 ) {
		/* skip over "pldap://" prefix and return success */
		p += PLDAP_URL_PREFIX_LEN;
		*scheme = "pldap";
		return( p );
	}

	/* check for "ldaps://" prefix */
	if ( strncasecmp( p, LDAPS_URL_PREFIX, LDAPS_URL_PREFIX_LEN ) == 0 ) {
		/* skip over "ldaps://" prefix and return success */
		p += LDAPS_URL_PREFIX_LEN;
		*scheme = "ldaps";
		return( p );
	}

	/* check for "pldaps://" prefix */
	if ( strncasecmp( p, PLDAPS_URL_PREFIX, PLDAPS_URL_PREFIX_LEN ) == 0 ) {
		/* skip over "pldaps://" prefix and return success */
		p += PLDAPS_URL_PREFIX_LEN;
		*scheme = "pldaps";
		return( p );
	}

	/* check for "ldapi://" prefix */
	if ( strncasecmp( p, LDAPI_URL_PREFIX, LDAPI_URL_PREFIX_LEN ) == 0 ) {
		/* skip over "ldapi://" prefix and return success */
		p += LDAPI_URL_PREFIX_LEN;
		*scheme = "ldapi";
		return( p );
	}

#ifdef LDAP_CONNECTIONLESS
	/* check for "cldap://" prefix */
	if ( strncasecmp( p, LDAPC_URL_PREFIX, LDAPC_URL_PREFIX_LEN ) == 0 ) {
		/* skip over "cldap://" prefix and return success */
		p += LDAPC_URL_PREFIX_LEN;
		*scheme = "cldap";
		return( p );
	}
#endif

	return( NULL );
}

int
ldap_pvt_scope2bv( int scope, struct berval *bv )
{
	switch ( scope ) {
	case LDAP_SCOPE_BASE:
		BER_BVSTR( bv, "base" );
		break;

	case LDAP_SCOPE_ONELEVEL:
		BER_BVSTR( bv, "one" );
		break;

	case LDAP_SCOPE_SUBTREE:
		BER_BVSTR( bv, "sub" );
		break;

	case LDAP_SCOPE_SUBORDINATE:
		BER_BVSTR( bv, "subordinate" );
		break;

	default:
		return LDAP_OTHER;
	}

	return LDAP_SUCCESS;
}

const char *
ldap_pvt_scope2str( int scope )
{
	struct berval	bv;

	if ( ldap_pvt_scope2bv( scope, &bv ) == LDAP_SUCCESS ) {
		return bv.bv_val;
	}

	return NULL;
}

int
ldap_pvt_bv2scope( struct berval *bv )
{
	static struct {
		struct berval	bv;
		int		scope;
	}	v[] = {
		{ BER_BVC( "one" ),		LDAP_SCOPE_ONELEVEL },
		{ BER_BVC( "onelevel" ),	LDAP_SCOPE_ONELEVEL },
		{ BER_BVC( "base" ),		LDAP_SCOPE_BASE },
		{ BER_BVC( "sub" ),		LDAP_SCOPE_SUBTREE },
		{ BER_BVC( "subtree" ),		LDAP_SCOPE_SUBTREE },
		{ BER_BVC( "subord" ),		LDAP_SCOPE_SUBORDINATE },
		{ BER_BVC( "subordinate" ),	LDAP_SCOPE_SUBORDINATE },
		{ BER_BVC( "children" ),	LDAP_SCOPE_SUBORDINATE },
		{ BER_BVNULL,			-1 }
	};
	int	i;

	for ( i = 0; v[ i ].scope != -1; i++ ) {
		if ( ber_bvstrcasecmp( bv, &v[ i ].bv ) == 0 ) {
			return v[ i ].scope;
		}
	}

	return( -1 );
}

int
ldap_pvt_str2scope( const char *p )
{
	struct berval	bv;

	ber_str2bv( p, 0, 0, &bv );

	return ldap_pvt_bv2scope( &bv );
}

static const char	hex[] = "0123456789ABCDEF";

#define URLESC_NONE	0x0000U
#define URLESC_COMMA	0x0001U
#define URLESC_SLASH	0x0002U

static int
hex_escape_len( const char *s, unsigned list )
{
	int	len;

	if ( s == NULL ) {
		return 0;
	}

	for ( len = 0; s[0]; s++ ) {
		switch ( s[0] ) {
		/* RFC 2396: reserved */
		case '?':
			len += 3;
			break;

		case ',':
			if ( list & URLESC_COMMA ) {
				len += 3;
			} else {
				len++;
			}
			break;

		case '/':
			if ( list & URLESC_SLASH ) {
				len += 3;
			} else {
				len++;
			}
			break;

		case ';':
		case ':':
		case '@':
		case '&':
		case '=':
		case '+':
		case '$':

		/* RFC 2396: unreserved mark */
		case '-':
		case '_':
		case '.':
		case '!':
		case '~':
		case '*':
		case '\'':
		case '(':
		case ')':
			len++;
			break;
			
		/* RFC 2396: unreserved alphanum */
		default:
			if ( !isalnum( (unsigned char) s[0] ) ) {
				len += 3;
			} else {
				len++;
			}
			break;
		}
	}

	return len;
}

static int
hex_escape( char *buf, int len, const char *s, unsigned list )
{
	int	i;
	int	pos;

	if ( s == NULL ) {
		return 0;
	}

	for ( pos = 0, i = 0; s[i] && pos < len; i++ ) {
		int	escape = 0;

		switch ( s[i] ) {
		/* RFC 2396: reserved */
		case '?':
			escape = 1;
			break;

		case ',':
			if ( list & URLESC_COMMA ) {
				escape = 1;
			}
			break;

		case '/':
			if ( list & URLESC_SLASH ) {
				escape = 1;
			}
			break;

		case ';':
		case ':':
		case '@':
		case '&':
		case '=':
		case '+':
		case '$':

		/* RFC 2396: unreserved mark */
		case '-':
		case '_':
		case '.':
		case '!':
		case '~':
		case '*':
		case '\'':
		case '(':
		case ')':
			break;
			
		/* RFC 2396: unreserved alphanum */
		default:
			if ( !isalnum( (unsigned char) s[i] ) ) {
				escape = 1;
			}
			break;
		}

		if ( escape ) {
			buf[pos++] = '%';
			buf[pos++] = hex[ (s[i] >> 4) & 0x0f ];
			buf[pos++] = hex[ s[i] & 0x0f ];

		} else {
			buf[pos++] = s[i];
		}
	}

	buf[pos] = '\0';

	return pos;
}

static int
hex_escape_len_list( char **s, unsigned flags )
{
	int	len;
	int	i;

	if ( s == NULL ) {
		return 0;
	}

	len = 0;
	for ( i = 0; s[i] != NULL; i++ ) {
		if ( len ) {
			len++;
		}
		len += hex_escape_len( s[i], flags );
	}

	return len;
}

static int
hex_escape_list( char *buf, int len, char **s, unsigned flags )
{
	int	pos;
	int	i;

	if ( s == NULL ) {
		return 0;
	}

	pos = 0;
	for ( i = 0; s[i] != NULL; i++ ) {
		int	curlen;

		if ( pos ) {
			buf[pos++] = ',';
			len--;
		}
		curlen = hex_escape( &buf[pos], len, s[i], flags );
		len -= curlen;
		pos += curlen;
	}

	return pos;
}

static int
desc2str_len( LDAPURLDesc *u )
{
	int		sep = 0;
	int		len = 0;
	int		is_ipc = 0;
	struct berval	scope;

	if ( u == NULL || u->lud_scheme == NULL ) {
		return -1;
	}

	if ( !strcmp( "ldapi", u->lud_scheme )) {
		is_ipc = 1;
	}

	if ( u->lud_exts ) {
		len += hex_escape_len_list( u->lud_exts, URLESC_COMMA );
		if ( !sep ) {
			sep = 5;
		}
	}

	if ( u->lud_filter ) {
		len += hex_escape_len( u->lud_filter, URLESC_NONE );
		if ( !sep ) {
			sep = 4;
		}
	}

	if ( ldap_pvt_scope2bv( u->lud_scope, &scope ) == LDAP_SUCCESS ) {
		len += scope.bv_len;
		if ( !sep ) {
			sep = 3;
		}
	}

	if ( u->lud_attrs ) {
		len += hex_escape_len_list( u->lud_attrs, URLESC_NONE );
		if ( !sep ) {
			sep = 2;
		}
	}

	if ( u->lud_dn && u->lud_dn[0] ) {
		len += hex_escape_len( u->lud_dn, URLESC_NONE );
		if ( !sep ) {
			sep = 1;
		}
	};

	len += sep;

	if ( u->lud_port ) {
		unsigned p = u->lud_port;
		if ( p > 65535 )
			return -1;

		len += (p > 999 ? 5 + (p > 9999) : p > 99 ? 4 : 2 + (p > 9));
	}

	if ( u->lud_host && u->lud_host[0] ) {
		char *ptr;
		len += hex_escape_len( u->lud_host, URLESC_SLASH );
		if ( !is_ipc && ( ptr = strchr( u->lud_host, ':' ))) {
			if ( strchr( ptr+1, ':' ))
				len += 2;	/* IPv6, [] */
		}
	}

	len += strlen( u->lud_scheme ) + STRLENOF( "://" );

	return len;
}

static int
desc2str( LDAPURLDesc *u, char *s, int len )
{
	int		i;
	int		sep = 0;
	int		sofar = 0;
	int		is_v6 = 0;
	int		is_ipc = 0;
	struct berval	scope = BER_BVNULL;
	char		*ptr;

	if ( u == NULL ) {
		return -1;
	}

	if ( s == NULL ) {
		return -1;
	}

	if ( u->lud_scheme && !strcmp( "ldapi", u->lud_scheme )) {
		is_ipc = 1;
	}

	ldap_pvt_scope2bv( u->lud_scope, &scope );

	if ( u->lud_exts ) {
		sep = 5;
	} else if ( u->lud_filter ) {
		sep = 4;
	} else if ( !BER_BVISEMPTY( &scope ) ) {
		sep = 3;
	} else if ( u->lud_attrs ) {
		sep = 2;
	} else if ( u->lud_dn && u->lud_dn[0] ) {
		sep = 1;
	}

	if ( !is_ipc && u->lud_host && ( ptr = strchr( u->lud_host, ':' ))) {
		if ( strchr( ptr+1, ':' ))
			is_v6 = 1;
	}

	if ( u->lud_port ) {
		sofar = sprintf( s, "%s://%s%s%s:%d", u->lud_scheme,
				is_v6 ? "[" : "",
				u->lud_host ? u->lud_host : "",
				is_v6 ? "]" : "",
				u->lud_port );
		len -= sofar;

	} else {
		sofar = sprintf( s, "%s://", u->lud_scheme );
		len -= sofar;
		if ( u->lud_host && u->lud_host[0] ) {
			if ( is_v6 ) {
				s[sofar++] = '[';
				len--;
			}
			i = hex_escape( &s[sofar], len, u->lud_host, URLESC_SLASH );
			sofar += i;
			len -= i;
			if ( is_v6 ) {
				s[sofar++] = ']';
				len--;
			}
		}
	}

	assert( len >= 0 );

	if ( sep < 1 ) {
		goto done;
	}

	s[sofar++] = '/';
	len--;

	assert( len >= 0 );

	if ( u->lud_dn && u->lud_dn[0] ) {
		i = hex_escape( &s[sofar], len, u->lud_dn, URLESC_NONE );
		sofar += i;
		len -= i;

		assert( len >= 0 );
	}

	if ( sep < 2 ) {
		goto done;
	}
	s[sofar++] = '?';
	len--;

	assert( len >= 0 );

	i = hex_escape_list( &s[sofar], len, u->lud_attrs, URLESC_NONE );
	sofar += i;
	len -= i;

	assert( len >= 0 );

	if ( sep < 3 ) {
		goto done;
	}
	s[sofar++] = '?';
	len--;

	assert( len >= 0 );

	if ( !BER_BVISNULL( &scope ) ) {
		strcpy( &s[sofar], scope.bv_val );
		sofar += scope.bv_len;
		len -= scope.bv_len;
	}

	assert( len >= 0 );

	if ( sep < 4 ) {
		goto done;
	}
	s[sofar++] = '?';
	len--;

	assert( len >= 0 );

	i = hex_escape( &s[sofar], len, u->lud_filter, URLESC_NONE );
	sofar += i;
	len -= i;

	assert( len >= 0 );

	if ( sep < 5 ) {
		goto done;
	}
	s[sofar++] = '?';
	len--;

	assert( len >= 0 );

	i = hex_escape_list( &s[sofar], len, u->lud_exts, URLESC_COMMA );
	sofar += i;
	len -= i;

	assert( len >= 0 );

done:
	if ( len < 0 ) {
		return -1;
	}

	return sofar;
}

char *
ldap_url_desc2str( LDAPURLDesc *u )
{
	int	len;
	char	*s;

	if ( u == NULL ) {
		return NULL;
	}

	len = desc2str_len( u );
	if ( len < 0 ) {
		return NULL;
	}
	
	/* allocate enough to hex escape everything -- overkill */
	s = LDAP_MALLOC( len + 1 );

	if ( s == NULL ) {
		return NULL;
	}

	if ( desc2str( u, s, len ) != len ) {
		LDAP_FREE( s );
		return NULL;
	}

	s[len] = '\0';

	return s;
}

int
ldap_url_parse_ext( LDAP_CONST char *url_in, LDAPURLDesc **ludpp, unsigned flags )
{
/*
 *  Pick apart the pieces of an LDAP URL.
 */

	LDAPURLDesc	*ludp;
	char	*p, *q, *r;
	int		i, enclosed, proto, is_v6 = 0;
	const char *scheme = NULL;
	const char *url_tmp;
	char *url;

	int	check_dn = 1;

	if( url_in == NULL || ludpp == NULL ) {
		return LDAP_URL_ERR_PARAM;
	}

#ifndef LDAP_INT_IN_KERNEL
	/* Global options may not be created yet
	 * We can't test if the global options are initialized
	 * because a call to LDAP_INT_GLOBAL_OPT() will try to allocate
	 * the options and cause infinite recursion
	 */
	Debug1( LDAP_DEBUG_TRACE, "ldap_url_parse_ext(%s)\n", url_in );
#endif

	*ludpp = NULL;	/* pessimistic */

	url_tmp = skip_url_prefix( url_in, &enclosed, &scheme );

	if ( url_tmp == NULL ) {
		return LDAP_URL_ERR_BADSCHEME;
	}

	assert( scheme != NULL );

	proto = ldap_pvt_url_scheme2proto( scheme );
	if ( proto == -1 ) {
		return LDAP_URL_ERR_BADSCHEME;
	}

	/* make working copy of the remainder of the URL */
	url = LDAP_STRDUP( url_tmp );
	if ( url == NULL ) {
		return LDAP_URL_ERR_MEM;
	}

	if ( enclosed ) {
		if ( ! *url ) {
			LDAP_FREE( url );
			return LDAP_URL_ERR_BADENCLOSURE;
		}
		p = &url[strlen(url)-1];

		if( *p != '>' ) {
			LDAP_FREE( url );
			return LDAP_URL_ERR_BADENCLOSURE;
		}

		*p = '\0';
	}

	/* allocate return struct */
	ludp = (LDAPURLDesc *)LDAP_CALLOC( 1, sizeof( LDAPURLDesc ));

	if ( ludp == NULL ) {
		LDAP_FREE( url );
		return LDAP_URL_ERR_MEM;
	}

	ludp->lud_next = NULL;
	ludp->lud_host = NULL;
	ludp->lud_port = 0;
	ludp->lud_dn = NULL;
	ludp->lud_attrs = NULL;
	ludp->lud_scope = ( flags & LDAP_PVT_URL_PARSE_NODEF_SCOPE ) ? LDAP_SCOPE_BASE : LDAP_SCOPE_DEFAULT;
	ludp->lud_filter = NULL;
	ludp->lud_exts = NULL;

	ludp->lud_scheme = LDAP_STRDUP( scheme );

	if ( ludp->lud_scheme == NULL ) {
		LDAP_FREE( url );
		ldap_free_urldesc( ludp );
		return LDAP_URL_ERR_MEM;
	}

	/* scan forward for '/' that marks end of hostport and begin. of dn */
	p = strchr( url, '/' );
	q = NULL;

	if( p != NULL ) {
		/* terminate hostport; point to start of dn */
		*p++ = '\0';
	} else {
		/* check for Novell kludge, see below */
		p = strchr( url, '?' );
		if ( p ) {
			*p++ = '\0';
			q = p;
			p = NULL;
		}
	}

	if ( proto != LDAP_PROTO_IPC ) {
		/* IPv6 syntax with [ip address]:port */
		if ( *url == '[' ) {
			r = strchr( url, ']' );
			if ( r == NULL ) {
				LDAP_FREE( url );
				ldap_free_urldesc( ludp );
				return LDAP_URL_ERR_BADURL;
			}
			*r++ = '\0';
			q = strchr( r, ':' );
			if ( q && q != r ) {
				LDAP_FREE( url );
				ldap_free_urldesc( ludp );
				return LDAP_URL_ERR_BADURL;
			}
			is_v6 = 1;
		} else {
			q = strchr( url, ':' );
		}

		if ( q != NULL ) {
			char	*next;

			*q++ = '\0';
			ldap_pvt_hex_unescape( q );

			if( *q == '\0' ) {
				LDAP_FREE( url );
				ldap_free_urldesc( ludp );
				return LDAP_URL_ERR_BADURL;
			}

			ludp->lud_port = strtol( q, &next, 10 );
			if ( next == q || next[0] != '\0' ) {
				LDAP_FREE( url );
				ldap_free_urldesc( ludp );
				return LDAP_URL_ERR_BADURL;
			}
			/* check for Novell kludge */
			if ( !p ) {
				if ( *next != '\0' ) {
					q = &next[1];
				} else {
					q = NULL;
				}
			}
		}

		if ( ( flags & LDAP_PVT_URL_PARSE_DEF_PORT ) && ludp->lud_port == 0 ) {
			if ( strcmp( ludp->lud_scheme, "ldaps" ) == 0 ) {
				ludp->lud_port = LDAPS_PORT;
			} else {
				ludp->lud_port = LDAP_PORT;
			}
		}
	}

	ldap_pvt_hex_unescape( url );

	/* If [ip address]:port syntax, url is [ip and we skip the [ */
	ludp->lud_host = LDAP_STRDUP( url + is_v6 );

	if( ludp->lud_host == NULL ) {
		LDAP_FREE( url );
		ldap_free_urldesc( ludp );
		return LDAP_URL_ERR_MEM;
	}

	if ( ( flags & LDAP_PVT_URL_PARSE_NOEMPTY_HOST )
		&& ludp->lud_host != NULL
		&& *ludp->lud_host == '\0' )
	{
		LDAP_FREE( ludp->lud_host );
		ludp->lud_host = NULL;
	}

	/*
	 * Kludge.  ldap://111.222.333.444:389??cn=abc,o=company
	 *
	 * On early Novell releases, search references/referrals were returned
	 * in this format, i.e., the dn was kind of in the scope position,
	 * but the required slash is missing. The whole thing is illegal syntax,
	 * but we need to account for it. Fortunately it can't be confused with
	 * anything real.
	 */
	if( (p == NULL) && (q != NULL) && (*q == '?') ) {
		/* ? immediately followed by question */
		q++;
		if( *q != '\0' ) {
			/* parse dn part */
			ldap_pvt_hex_unescape( q );
			ludp->lud_dn = LDAP_STRDUP( q );

		} else if ( !( flags & LDAP_PVT_URL_PARSE_NOEMPTY_DN ) ) {
			ludp->lud_dn = LDAP_STRDUP( "" );

		} else {
			check_dn = 0;
		}

		if ( check_dn && ludp->lud_dn == NULL ) {
			LDAP_FREE( url );
			ldap_free_urldesc( ludp );
			return LDAP_URL_ERR_MEM;
		}
	}

	if( p == NULL ) {
		LDAP_FREE( url );
		*ludpp = ludp;
		return LDAP_URL_SUCCESS;
	}

	/* scan forward for '?' that may marks end of dn */
	q = strchr( p, '?' );

	if( q != NULL ) {
		/* terminate dn part */
		*q++ = '\0';
	}

	if( *p != '\0' ) {
		/* parse dn part */
		ldap_pvt_hex_unescape( p );
		ludp->lud_dn = LDAP_STRDUP( p );

	} else if ( !( flags & LDAP_PVT_URL_PARSE_NOEMPTY_DN ) ) {
		ludp->lud_dn = LDAP_STRDUP( "" );

	} else {
		check_dn = 0;
	}

	if( check_dn && ludp->lud_dn == NULL ) {
		LDAP_FREE( url );
		ldap_free_urldesc( ludp );
		return LDAP_URL_ERR_MEM;
	}

	if( q == NULL ) {
		/* no more */
		LDAP_FREE( url );
		*ludpp = ludp;
		return LDAP_URL_SUCCESS;
	}

	/* scan forward for '?' that may marks end of attributes */
	p = q;
	q = strchr( p, '?' );

	if( q != NULL ) {
		/* terminate attributes part */
		*q++ = '\0';
	}

	if( *p != '\0' ) {
		/* parse attributes */
		ldap_pvt_hex_unescape( p );
		ludp->lud_attrs = ldap_str2charray( p, "," );

		if( ludp->lud_attrs == NULL ) {
			LDAP_FREE( url );
			ldap_free_urldesc( ludp );
			return LDAP_URL_ERR_BADATTRS;
		}
	}

	if ( q == NULL ) {
		/* no more */
		LDAP_FREE( url );
		*ludpp = ludp;
		return LDAP_URL_SUCCESS;
	}

	/* scan forward for '?' that may marks end of scope */
	p = q;
	q = strchr( p, '?' );

	if( q != NULL ) {
		/* terminate the scope part */
		*q++ = '\0';
	}

	if( *p != '\0' ) {
		/* parse the scope */
		ldap_pvt_hex_unescape( p );
		ludp->lud_scope = ldap_pvt_str2scope( p );

		if( ludp->lud_scope == -1 ) {
			LDAP_FREE( url );
			ldap_free_urldesc( ludp );
			return LDAP_URL_ERR_BADSCOPE;
		}
	}

	if ( q == NULL ) {
		/* no more */
		LDAP_FREE( url );
		*ludpp = ludp;
		return LDAP_URL_SUCCESS;
	}

	/* scan forward for '?' that may marks end of filter */
	p = q;
	q = strchr( p, '?' );

	if( q != NULL ) {
		/* terminate the filter part */
		*q++ = '\0';
	}

	if( *p != '\0' ) {
		/* parse the filter */
		ldap_pvt_hex_unescape( p );

		if( ! *p ) {
			/* missing filter */
			LDAP_FREE( url );
			ldap_free_urldesc( ludp );
			return LDAP_URL_ERR_BADFILTER;
		}

		ludp->lud_filter = LDAP_STRDUP( p );

		if( ludp->lud_filter == NULL ) {
			LDAP_FREE( url );
			ldap_free_urldesc( ludp );
			return LDAP_URL_ERR_MEM;
		}
	}

	if ( q == NULL ) {
		/* no more */
		LDAP_FREE( url );
		*ludpp = ludp;
		return LDAP_URL_SUCCESS;
	}

	/* scan forward for '?' that may marks end of extensions */
	p = q;
	q = strchr( p, '?' );

	if( q != NULL ) {
		/* extra '?' */
		LDAP_FREE( url );
		ldap_free_urldesc( ludp );
		return LDAP_URL_ERR_BADURL;
	}

	/* parse the extensions */
	ludp->lud_exts = ldap_str2charray( p, "," );

	if( ludp->lud_exts == NULL ) {
		LDAP_FREE( url );
		ldap_free_urldesc( ludp );
		return LDAP_URL_ERR_BADEXTS;
	}

	for( i=0; ludp->lud_exts[i] != NULL; i++ ) {
		ldap_pvt_hex_unescape( ludp->lud_exts[i] );

		if( *ludp->lud_exts[i] == '!' ) {
			/* count the number of critical extensions */
			ludp->lud_crit_exts++;
		}
	}

	if( i == 0 ) {
		/* must have 1 or more */
		LDAP_FREE( url );
		ldap_free_urldesc( ludp );
		return LDAP_URL_ERR_BADEXTS;
	}

	/* no more */
	*ludpp = ludp;
	LDAP_FREE( url );
	return LDAP_URL_SUCCESS;
}

int
ldap_url_parse( LDAP_CONST char *url_in, LDAPURLDesc **ludpp )
{
	return ldap_url_parse_ext( url_in, ludpp, LDAP_PVT_URL_PARSE_HISTORIC );
}

LDAPURLDesc *
ldap_url_dup ( LDAPURLDesc *ludp )
{
	LDAPURLDesc *dest;

	if ( ludp == NULL ) {
		return NULL;
	}

	dest = LDAP_MALLOC( sizeof(LDAPURLDesc) );
	if (dest == NULL)
		return NULL;
	
	*dest = *ludp;
	dest->lud_scheme = NULL;
	dest->lud_host = NULL;
	dest->lud_dn = NULL;
	dest->lud_filter = NULL;
	dest->lud_attrs = NULL;
	dest->lud_exts = NULL;
	dest->lud_next = NULL;

	if ( ludp->lud_scheme != NULL ) {
		dest->lud_scheme = LDAP_STRDUP( ludp->lud_scheme );
		if (dest->lud_scheme == NULL) {
			ldap_free_urldesc(dest);
			return NULL;
		}
	}

	if ( ludp->lud_host != NULL ) {
		dest->lud_host = LDAP_STRDUP( ludp->lud_host );
		if (dest->lud_host == NULL) {
			ldap_free_urldesc(dest);
			return NULL;
		}
	}

	if ( ludp->lud_dn != NULL ) {
		dest->lud_dn = LDAP_STRDUP( ludp->lud_dn );
		if (dest->lud_dn == NULL) {
			ldap_free_urldesc(dest);
			return NULL;
		}
	}

	if ( ludp->lud_filter != NULL ) {
		dest->lud_filter = LDAP_STRDUP( ludp->lud_filter );
		if (dest->lud_filter == NULL) {
			ldap_free_urldesc(dest);
			return NULL;
		}
	}

	if ( ludp->lud_attrs != NULL ) {
		dest->lud_attrs = ldap_charray_dup( ludp->lud_attrs );
		if (dest->lud_attrs == NULL) {
			ldap_free_urldesc(dest);
			return NULL;
		}
	}

	if ( ludp->lud_exts != NULL ) {
		dest->lud_exts = ldap_charray_dup( ludp->lud_exts );
		if (dest->lud_exts == NULL) {
			ldap_free_urldesc(dest);
			return NULL;
		}
	}

	return dest;
}

LDAPURLDesc *
ldap_url_duplist (LDAPURLDesc *ludlist)
{
	LDAPURLDesc *dest, *tail, *ludp, *newludp;

	dest = NULL;
	tail = NULL;
	for (ludp = ludlist; ludp != NULL; ludp = ludp->lud_next) {
		newludp = ldap_url_dup(ludp);
		if (newludp == NULL) {
			ldap_free_urllist(dest);
			return NULL;
		}
		if (tail == NULL)
			dest = newludp;
		else
			tail->lud_next = newludp;
		tail = newludp;
	}
	return dest;
}

static int
ldap_url_parselist_int (LDAPURLDesc **ludlist, const char *url, const char *sep, unsigned flags )
	
{
	int i, rc;
	LDAPURLDesc *ludp;
	char **urls;

	assert( ludlist != NULL );
	assert( url != NULL );

	*ludlist = NULL;

	if ( sep == NULL ) {
		sep = ", ";
	}

	urls = ldap_str2charray( url, sep );
	if (urls == NULL)
		return LDAP_URL_ERR_MEM;

	/* count the URLs... */
	for (i = 0; urls[i] != NULL; i++) ;
	/* ...and put them in the "stack" backward */
	while (--i >= 0) {
		rc = ldap_url_parse_ext( urls[i], &ludp, flags );
		if ( rc != 0 ) {
			ldap_charray_free( urls );
			ldap_free_urllist( *ludlist );
			*ludlist = NULL;
			return rc;
		}
		ludp->lud_next = *ludlist;
		*ludlist = ludp;
	}
	ldap_charray_free( urls );
	return LDAP_URL_SUCCESS;
}

int
ldap_url_parselist (LDAPURLDesc **ludlist, const char *url )
{
	return ldap_url_parselist_int( ludlist, url, ", ", LDAP_PVT_URL_PARSE_HISTORIC );
}

int
ldap_url_parselist_ext (LDAPURLDesc **ludlist, const char *url, const char *sep, unsigned flags )
{
	return ldap_url_parselist_int( ludlist, url, sep, flags );
}

int
ldap_url_parsehosts(
	LDAPURLDesc **ludlist,
	const char *hosts,
	int port )
{
	int i;
	LDAPURLDesc *ludp;
	char **specs, *p;

	assert( ludlist != NULL );
	assert( hosts != NULL );

	*ludlist = NULL;

	specs = ldap_str2charray(hosts, ", ");
	if (specs == NULL)
		return LDAP_NO_MEMORY;

	/* count the URLs... */
	for (i = 0; specs[i] != NULL; i++) /* EMPTY */;

	/* ...and put them in the "stack" backward */
	while (--i >= 0) {
		ludp = LDAP_CALLOC( 1, sizeof(LDAPURLDesc) );
		if (ludp == NULL) {
			ldap_charray_free(specs);
			ldap_free_urllist(*ludlist);
			*ludlist = NULL;
			return LDAP_NO_MEMORY;
		}
		ludp->lud_port = port;
		ludp->lud_host = specs[i];
		p = strchr(ludp->lud_host, ':');
		if (p != NULL) {
			/* more than one :, IPv6 address */
			if ( strchr(p+1, ':') != NULL ) {
				/* allow [address] and [address]:port */
				if ( *ludp->lud_host == '[' ) {
					p = strchr( ludp->lud_host+1, ']' );
					if ( p == NULL ) {
						LDAP_FREE(ludp);
						ldap_charray_free(specs);
						return LDAP_PARAM_ERROR;
					}
					/* Truncate trailing ']' and shift hostname down 1 char */
					*p = '\0';
					AC_MEMCPY( ludp->lud_host, ludp->lud_host+1, p - ludp->lud_host );
					p++;
					if ( *p != ':' ) {
						if ( *p != '\0' ) {
							LDAP_FREE(ludp);
							ldap_charray_free(specs);
							return LDAP_PARAM_ERROR;
						}
						p = NULL;
					}
				} else {
					p = NULL;
				}
			}
			if (p != NULL) {
				char	*next;

				*p++ = 0;
				ldap_pvt_hex_unescape(p);
				ludp->lud_port = strtol( p, &next, 10 );
				if ( next == p || next[0] != '\0' ) {
					LDAP_FREE(ludp);
					ldap_charray_free(specs);
					return LDAP_PARAM_ERROR;
				}
			}
		}
		ludp->lud_scheme = LDAP_STRDUP("ldap");
		if ( ludp->lud_scheme == NULL ) {
			LDAP_FREE(ludp);
			ldap_charray_free(specs);
			return LDAP_NO_MEMORY;
		}
		specs[i] = NULL;
		ldap_pvt_hex_unescape(ludp->lud_host);
		ludp->lud_next = *ludlist;
		*ludlist = ludp;
	}

	/* this should be an array of NULLs now */
	ldap_charray_free(specs);
	return LDAP_SUCCESS;
}

char *
ldap_url_list2hosts (LDAPURLDesc *ludlist)
{
	LDAPURLDesc *ludp;
	int size;
	char *s, *p, buf[32];	/* big enough to hold a long decimal # (overkill) */

	if (ludlist == NULL)
		return NULL;

	/* figure out how big the string is */
	size = 1;	/* nul-term */
	for (ludp = ludlist; ludp != NULL; ludp = ludp->lud_next) {
		if ( ludp->lud_host == NULL ) continue;
		size += strlen(ludp->lud_host) + 1;		/* host and space */
		if (strchr(ludp->lud_host, ':'))        /* will add [ ] below */
			size += 2;
		if (ludp->lud_port != 0)
			size += sprintf(buf, ":%d", ludp->lud_port);
	}
	s = LDAP_MALLOC(size);
	if (s == NULL)
		return NULL;

	p = s;
	for (ludp = ludlist; ludp != NULL; ludp = ludp->lud_next) {
		if ( ludp->lud_host == NULL ) continue;
		if (strchr(ludp->lud_host, ':')) {
			p += sprintf(p, "[%s]", ludp->lud_host);
		} else {
			strcpy(p, ludp->lud_host);
			p += strlen(ludp->lud_host);
		}
		if (ludp->lud_port != 0)
			p += sprintf(p, ":%d", ludp->lud_port);
		*p++ = ' ';
	}
	if (p != s)
		p--;	/* nuke that extra space */
	*p = '\0';
	return s;
}

char *
ldap_url_list2urls(
	LDAPURLDesc *ludlist )
{
	LDAPURLDesc	*ludp;
	int		size, sofar;
	char		*s;

	if ( ludlist == NULL ) {
		return NULL;
	}

	/* figure out how big the string is */
	for ( size = 0, ludp = ludlist; ludp != NULL; ludp = ludp->lud_next ) {
		int	len = desc2str_len( ludp );
		if ( len < 0 ) {
			return NULL;
		}
		size += len + 1;
	}
	
	s = LDAP_MALLOC( size );

	if ( s == NULL ) {
		return NULL;
	}

	for ( sofar = 0, ludp = ludlist; ludp != NULL; ludp = ludp->lud_next ) {
		int	len;

		len = desc2str( ludp, &s[sofar], size );
		
		if ( len < 0 ) {
			LDAP_FREE( s );
			return NULL;
		}

		sofar += len;
		size -= len;

		s[sofar++] = ' ';
		size--;

		assert( size >= 0 );
	}

	s[sofar - 1] = '\0';

	return s;
}

void
ldap_free_urllist( LDAPURLDesc *ludlist )
{
	LDAPURLDesc *ludp, *next;

	for (ludp = ludlist; ludp != NULL; ludp = next) {
		next = ludp->lud_next;
		ldap_free_urldesc(ludp);
	}
}

void
ldap_free_urldesc( LDAPURLDesc *ludp )
{
	if ( ludp == NULL ) {
		return;
	}
	
	if ( ludp->lud_scheme != NULL ) {
		LDAP_FREE( ludp->lud_scheme );
	}

	if ( ludp->lud_host != NULL ) {
		LDAP_FREE( ludp->lud_host );
	}

	if ( ludp->lud_dn != NULL ) {
		LDAP_FREE( ludp->lud_dn );
	}

	if ( ludp->lud_filter != NULL ) {
		LDAP_FREE( ludp->lud_filter);
	}

	if ( ludp->lud_attrs != NULL ) {
		LDAP_VFREE( ludp->lud_attrs );
	}

	if ( ludp->lud_exts != NULL ) {
		LDAP_VFREE( ludp->lud_exts );
	}

	LDAP_FREE( ludp );
}

static int
ldap_int_is_hexpair( char *s )
{
	int	i;

	for ( i = 0; i < 2; i++ ) {
		if ( s[i] >= '0' && s[i] <= '9' ) {
			continue;
		}

		if ( s[i] >= 'A' && s[i] <= 'F' ) {
			continue;
		}

		if ( s[i] >= 'a' && s[i] <= 'f' ) {
			continue;
		}

		return 0;
	}
	
	return 1;	
}
	
static int
ldap_int_unhex( int c )
{
	return( c >= '0' && c <= '9' ? c - '0'
	    : c >= 'A' && c <= 'F' ? c - 'A' + 10
	    : c - 'a' + 10 );
}

void
ldap_pvt_hex_unescape( char *s )
{
	/*
	 * Remove URL hex escapes from s... done in place.  The basic concept for
	 * this routine is borrowed from the WWW library HTUnEscape() routine.
	 */
	char	*p,
		*save_s = s;

	for ( p = s; *s != '\0'; ++s ) {
		if ( *s == '%' ) {
			/*
			 * FIXME: what if '%' is followed
			 * by non-hexpair chars?
			 */
			if ( !ldap_int_is_hexpair( s + 1 ) ) {
				p = save_s;
				break;
			}

			if ( *++s == '\0' ) {
				break;
			}
			*p = ldap_int_unhex( *s ) << 4;
			if ( *++s == '\0' ) {
				break;
			}
			*p++ += ldap_int_unhex( *s );
		} else {
			*p++ = *s;
		}
	}

	*p = '\0';
}

