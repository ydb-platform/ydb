/* fetch.c - routines for fetching data at URLs */
/* $OpenLDAP$ */
/* This work is part of OpenLDAP Software <http://www.openldap.org/>.
 *
 * Copyright 1999-2022 The OpenLDAP Foundation.
 * Portions Copyright 1999-2003 Kurt D. Zeilenga.
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
/* This work was initially developed by Kurt D. Zeilenga for
 * inclusion in OpenLDAP Software.
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/string.h>
#include <ac/socket.h>
#include <ac/time.h>

#ifdef HAVE_FETCH
#include <fetch.h>
#endif

#include "lber_pvt.h"
#include "ldap_pvt.h"
#include "ldap_config.h"
#include "ldif.h"

FILE *
ldif_open_url(
	LDAP_CONST char *urlstr )
{
	FILE *url;

	if( strncasecmp( "file:", urlstr, sizeof("file:")-1 ) == 0 ) {
		char *p;
		urlstr += sizeof("file:")-1;

		/* we don't check for LDAP_DIRSEP since URLs should contain '/' */
		if ( urlstr[0] == '/' && urlstr[1] == '/' ) {
			urlstr += 2;
			/* path must be absolute if authority is present
			 * technically, file://hostname/path is also legal but we don't
			 * accept a non-empty hostname
			 */
			if ( urlstr[0] != '/' ) {
#ifdef _WIN32
				/* An absolute path in improper file://C:/foo/bar format */
				if ( urlstr[1] != ':' )
#endif
				return NULL;
			}
#ifdef _WIN32
			/* An absolute path in proper file:///C:/foo/bar format */
			if ( urlstr[2] == ':' )
				urlstr++;
#endif
		}

		p = ber_strdup( urlstr );
		if ( p == NULL )
			return NULL;

		/* But we should convert to LDAP_DIRSEP before use */
		if ( LDAP_DIRSEP[0] != '/' ) {
			char *s = p;
			while (( s = strchr( s, '/' )))
				*s++ = LDAP_DIRSEP[0];
		}

		ldap_pvt_hex_unescape( p );

		url = fopen( p, "rb" );

		ber_memfree( p );
	} else {
#ifdef HAVE_FETCH
		url = fetchGetURL( (char*) urlstr, "" );
#else
		url = NULL;
#endif
	}
	return url;
}

int
ldif_fetch_url(
    LDAP_CONST char	*urlstr,
    char	**valuep,
    ber_len_t *vlenp )
{
	FILE *url;
	char buffer[1024];
	char *p = NULL;
	size_t total;
	size_t bytes;

	*valuep = NULL;
	*vlenp = 0;

	url = ldif_open_url( urlstr );

	if( url == NULL ) {
		return -1;
	}

	total = 0;

	while( (bytes = fread( buffer, 1, sizeof(buffer), url )) != 0 ) {
		char *newp = ber_memrealloc( p, total + bytes + 1 );
		if( newp == NULL ) {
			ber_memfree( p );
			fclose( url );
			return -1;
		}
		p = newp;
		AC_MEMCPY( &p[total], buffer, bytes );
		total += bytes;
	}

	fclose( url );

	if( total == 0 ) {
		char *newp = ber_memrealloc( p, 1 );
		if( newp == NULL ) {
			ber_memfree( p );
			return -1;
		}
		p = newp;
	}

	p[total] = '\0';
	*valuep = p;
	*vlenp = total;

	return 0;
}
