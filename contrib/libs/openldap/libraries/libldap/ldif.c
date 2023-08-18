/* ldif.c - routines for dealing with LDIF files */
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
/* Portions Copyright (c) 1992-1996 Regents of the University of Michigan.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms are permitted
 * provided that this notice is preserved and that due credit is given
 * to the University of Michigan at Ann Arbor.  The name of the
 * University may not be used to endorse or promote products derived
 * from this software without specific prior written permission.  This
 * software is provided ``as is'' without express or implied warranty.
 */
/* This work was originally developed by the University of Michigan
 * and distributed as part of U-MICH LDAP.
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>
#include <ac/ctype.h>

#include <ac/string.h>
#include <ac/socket.h>
#include <ac/time.h>

int ldif_debug = 0;

#include "ldap-int.h"
#include "ldif.h"

#define CONTINUED_LINE_MARKER	'\r'

#ifdef CSRIMALLOC
#define ber_memalloc malloc
#define ber_memcalloc calloc
#define ber_memrealloc realloc
#define ber_strdup strdup
#endif

static const char nib2b64[0x40] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/*
 * ldif_parse_line - takes a line of the form "type:[:] value" and splits it
 * into components "type" and "value".  if a double colon separates type from
 * value, then value is encoded in base 64, and parse_line un-decodes it
 * (in place) before returning. The type and value are stored in malloc'd
 * memory which must be freed by the caller.
 *
 * ldif_parse_line2 - operates in-place on input buffer, returning type
 * in-place. Will return value in-place if possible, (must malloc for
 * fetched URLs). If freeval is NULL, all return data will be malloc'd
 * and the input line will be unmodified. Otherwise freeval is set to
 * True if the value was malloc'd.
 */

int
ldif_parse_line(
    LDAP_CONST char	*line,
    char	**typep,
    char	**valuep,
    ber_len_t *vlenp
)
{
	struct berval type, value;
	int rc = ldif_parse_line2( (char *)line, &type, &value, NULL );

	*typep = type.bv_val;
	*valuep = value.bv_val;
	*vlenp = value.bv_len;
	return rc;
}

int
ldif_parse_line2(
    char	*line,
	struct berval *type,
	struct berval *value,
	int		*freeval
)
{
	char	*s, *p, *d; 
	int	b64, url;

	BER_BVZERO( type );
	BER_BVZERO( value );

	/* skip any leading space */
	while ( isspace( (unsigned char) *line ) ) {
		line++;
	}

	if ( freeval ) {
		*freeval = 0;
	} else {
		line = ber_strdup( line );

		if( line == NULL ) {
			ber_pvt_log_printf( LDAP_DEBUG_ANY, ldif_debug,
				_("ldif_parse_line: line malloc failed\n"));
			return( -1 );
		}
	}

	type->bv_val = line;

	s = strchr( type->bv_val, ':' );

	if ( s == NULL ) {
		ber_pvt_log_printf( LDAP_DEBUG_PARSE, ldif_debug,
			_("ldif_parse_line: missing ':' after %s\n"),
			type->bv_val );
		if ( !freeval ) ber_memfree( line );
		return( -1 );
	}

	/* trim any space between type and : */
	for ( p = &s[-1]; p > type->bv_val && isspace( * (unsigned char *) p ); p-- ) {
		*p = '\0';
	}
	*s++ = '\0';
	type->bv_len = s - type->bv_val - 1;

	url = 0;
	b64 = 0;

	if ( *s == '<' ) {
		s++;
		url = 1;

	} else if ( *s == ':' ) {
		/* base 64 encoded value */
		s++;
		b64 = 1;
	}

	/* skip space between : and value */
	while ( isspace( (unsigned char) *s ) ) {
		s++;
	}

	/* check for continued line markers that should be deleted */
	for ( p = s, d = s; *p; p++ ) {
		if ( *p != CONTINUED_LINE_MARKER )
			*d++ = *p;
	}
	*d = '\0';

	if ( b64 ) {
		char *byte = s;

		if ( *s == '\0' ) {
			/* no value is present, error out */
			ber_pvt_log_printf( LDAP_DEBUG_PARSE, ldif_debug,
				_("ldif_parse_line: %s missing base64 value\n"),
				type->bv_val );
			if ( !freeval ) ber_memfree( line );
			return( -1 );
		}

		value->bv_val = s;
		value->bv_len = d - s;
		if ( ldap_int_decode_b64_inplace( value ) != LDAP_SUCCESS ) {
			ber_pvt_log_printf( LDAP_DEBUG_PARSE, ldif_debug,
				_("ldif_parse_line: %s base64 decode failed\n"),
				type->bv_val );
			if ( !freeval ) ber_memfree( line );
			return( -1 );
		}
	} else if ( url ) {
		if ( *s == '\0' ) {
			/* no value is present, error out */
			ber_pvt_log_printf( LDAP_DEBUG_PARSE, ldif_debug,
				_("ldif_parse_line: %s missing URL value\n"),
				type->bv_val );
			if ( !freeval ) ber_memfree( line );
			return( -1 );
		}

		if( ldif_fetch_url( s, &value->bv_val, &value->bv_len ) ) {
			ber_pvt_log_printf( LDAP_DEBUG_ANY, ldif_debug,
				_("ldif_parse_line: %s: URL \"%s\" fetch failed\n"),
				type->bv_val, s );
			if ( !freeval ) ber_memfree( line );
			return( -1 );
		}
		if ( freeval ) *freeval = 1;

	} else {
		value->bv_val = s;
		value->bv_len = (int) (d - s);
	}

	if ( !freeval ) {
		struct berval bv = *type;

		ber_dupbv( type, &bv );

		if( BER_BVISNULL( type )) {
			ber_pvt_log_printf( LDAP_DEBUG_ANY, ldif_debug,
				_("ldif_parse_line: type malloc failed\n"));
			if( url ) ber_memfree( value->bv_val );
			ber_memfree( line );
			return( -1 );
		}

		if( !url ) {
			bv = *value;
			ber_dupbv( value, &bv );
			if( BER_BVISNULL( value )) {
				ber_pvt_log_printf( LDAP_DEBUG_ANY, ldif_debug,
					_("ldif_parse_line: value malloc failed\n"));
				ber_memfree( type->bv_val );
				ber_memfree( line );
				return( -1 );
			}
		}

		ber_memfree( line );
	}

	return( 0 );
}

/*
 * ldif_getline - return the next "line" (minus newline) of input from a
 * string buffer of lines separated by newlines, terminated by \n\n
 * or \0.  this routine handles continued lines, bundling them into
 * a single big line before returning.  if a line begins with a white
 * space character, it is a continuation of the previous line. the white
 * space character (nb: only one char), and preceding newline are changed
 * into CONTINUED_LINE_MARKER chars, to be deleted later by the
 * ldif_parse_line() routine above.
 *
 * ldif_getline will skip over any line which starts '#'.
 *
 * ldif_getline takes a pointer to a pointer to the buffer on the first call,
 * which it updates and must be supplied on subsequent calls.
 */

int
ldif_countlines( LDAP_CONST char *buf )
{
	char *nl;
	int ret = 0;

	if ( !buf ) return ret;

	for ( nl = strchr(buf, '\n'); nl; nl = strchr(nl, '\n') ) {
		nl++;
		if ( *nl != ' ' ) ret++;
	}
	return ret;
}

char *
ldif_getline( char **next )
{
	char *line;

	do {
		if ( *next == NULL || **next == '\n' || **next == '\0' ) {
			return( NULL );
		}

		line = *next;

		while ( (*next = strchr( *next, '\n' )) != NULL ) {
#if CONTINUED_LINE_MARKER != '\r'
			if ( (*next)[-1] == '\r' ) {
				(*next)[-1] = CONTINUED_LINE_MARKER;
			}
#endif

			if ( (*next)[1] != ' ' ) {
				if ( (*next)[1] == '\r' && (*next)[2] == '\n' ) {
					*(*next)++ = '\0';
				}
				*(*next)++ = '\0';
				break;
			}

			**next = CONTINUED_LINE_MARKER;
			(*next)[1] = CONTINUED_LINE_MARKER;
			(*next)++;
		}
	} while( *line == '#' );

	return( line );
}

/*
 * name and OID of attributeTypes that must be base64 encoded in any case
 */
typedef struct must_b64_encode_s {
	struct berval	name;
	struct berval	oid;
} must_b64_encode_s;

static must_b64_encode_s	default_must_b64_encode[] = {
	{ BER_BVC( "userPassword" ), BER_BVC( "2.5.4.35" ) },
	{ BER_BVNULL, BER_BVNULL }
};

static must_b64_encode_s	*must_b64_encode = default_must_b64_encode;

/*
 * register name and OID of attributeTypes that must always be base64 
 * encoded
 *
 * NOTE: this routine mallocs memory in a static struct which must 
 * be explicitly freed when no longer required
 */
int
ldif_must_b64_encode_register( LDAP_CONST char *name, LDAP_CONST char *oid )
{
	int		i;
	ber_len_t	len;

	assert( must_b64_encode != NULL );
	assert( name != NULL );
	assert( oid != NULL );

	len = strlen( name );

	for ( i = 0; !BER_BVISNULL( &must_b64_encode[i].name ); i++ ) {
		if ( len != must_b64_encode[i].name.bv_len ) {
			continue;
		}

		if ( strcasecmp( name, must_b64_encode[i].name.bv_val ) == 0 ) {
			break;
		}
	}

	if ( !BER_BVISNULL( &must_b64_encode[i].name ) ) {
		return 1;
	}

	for ( i = 0; !BER_BVISNULL( &must_b64_encode[i].name ); i++ )
		/* just count */ ;

	if ( must_b64_encode == default_must_b64_encode ) {
		must_b64_encode = ber_memalloc( sizeof( must_b64_encode_s ) * ( i + 2 ) );
		if ( must_b64_encode == NULL ) {
		    return 1;
		}

		for ( i = 0; !BER_BVISNULL( &default_must_b64_encode[i].name ); i++ ) {
			ber_dupbv( &must_b64_encode[i].name, &default_must_b64_encode[i].name );
			ber_dupbv( &must_b64_encode[i].oid, &default_must_b64_encode[i].oid );
		}

	} else {
		must_b64_encode_s	*tmp;

		tmp = ber_memrealloc( must_b64_encode,
			sizeof( must_b64_encode_s ) * ( i + 2 ) );
		if ( tmp == NULL ) {
			return 1;
		}
		must_b64_encode = tmp;
	}

	ber_str2bv( name, len, 1, &must_b64_encode[i].name );
	ber_str2bv( oid, 0, 1, &must_b64_encode[i].oid );

	BER_BVZERO( &must_b64_encode[i + 1].name );

	return 0;
}

void
ldif_must_b64_encode_release( void )
{
	int	i;

	assert( must_b64_encode != NULL );

	if ( must_b64_encode == default_must_b64_encode ) {
		return;
	}

	for ( i = 0; !BER_BVISNULL( &must_b64_encode[i].name ); i++ ) {
		ber_memfree( must_b64_encode[i].name.bv_val );
		ber_memfree( must_b64_encode[i].oid.bv_val );
	}

	ber_memfree( must_b64_encode );

	must_b64_encode = default_must_b64_encode;
}

/*
 * returns 1 iff the string corresponds to the name or the OID of any 
 * of the attributeTypes listed in must_b64_encode
 */
static int
ldif_must_b64_encode( LDAP_CONST char *s )
{
	int		i;
	struct berval	bv;

	assert( must_b64_encode != NULL );
	assert( s != NULL );

	ber_str2bv( s, 0, 0, &bv );

	for ( i = 0; !BER_BVISNULL( &must_b64_encode[i].name ); i++ ) {
		if ( ber_bvstrcasecmp( &must_b64_encode[i].name, &bv ) == 0
			|| ber_bvcmp( &must_b64_encode[i].oid, &bv ) == 0 )
		{
			return 1;
		}
	}

	return 0;
}

/* NOTE: only preserved for binary compatibility */
void
ldif_sput(
	char **out,
	int type,
	LDAP_CONST char *name,
	LDAP_CONST char *val,
	ber_len_t vlen )
{
	ldif_sput_wrap( out, type, name, val, vlen, 0 );
}

void
ldif_sput_wrap(
	char **out,
	int type,
	LDAP_CONST char *name,
	LDAP_CONST char *val,
	ber_len_t vlen,
        ber_len_t wrap )
{
	const unsigned char *byte, *stop;
	unsigned char	buf[3];
	unsigned long	bits;
	char		*save;
	int		pad;
	int		namelen = 0;

	ber_len_t savelen;
	ber_len_t len=0;
	ber_len_t i;

	if ( !wrap )
		wrap = LDIF_LINE_WIDTH;

	/* prefix */
	switch( type ) {
	case LDIF_PUT_COMMENT:
		*(*out)++ = '#';
		len++;

		if( vlen ) {
			*(*out)++ = ' ';
			len++;
		}

		break;

	case LDIF_PUT_SEP:
		*(*out)++ = '\n';
		return;
	}

	/* name (attribute type) */
	if( name != NULL ) {
		/* put the name + ":" */
		namelen = strlen(name);
		strcpy(*out, name);
		*out += namelen;
		len += namelen;

		if( type != LDIF_PUT_COMMENT ) {
			*(*out)++ = ':';
			len++;
		}

	}
#ifdef LDAP_DEBUG
	else {
		assert( type == LDIF_PUT_COMMENT );
	}
#endif

	if( vlen == 0 ) {
		*(*out)++ = '\n';
		return;
	}

	switch( type ) {
	case LDIF_PUT_NOVALUE:
		*(*out)++ = '\n';
		return;

	case LDIF_PUT_URL: /* url value */
		*(*out)++ = '<';
		len++;
		break;

	case LDIF_PUT_B64: /* base64 value */
		*(*out)++ = ':';
		len++;
		break;
	}

	switch( type ) {
	case LDIF_PUT_TEXT:
	case LDIF_PUT_URL:
	case LDIF_PUT_B64:
		*(*out)++ = ' ';
		len++;
		/* fall-thru */

	case LDIF_PUT_COMMENT:
		/* pre-encoded names */
		for ( i=0; i < vlen; i++ ) {
			if ( len > wrap ) {
				*(*out)++ = '\n';
				*(*out)++ = ' ';
				len = 1;
			}

			*(*out)++ = val[i];
			len++;
		}
		*(*out)++ = '\n';
		return;
	}

	save = *out;
	savelen = len;

	*(*out)++ = ' ';
	len++;

	stop = (const unsigned char *) (val + vlen);

	if ( type == LDIF_PUT_VALUE
		&& isgraph( (unsigned char) val[0] ) && val[0] != ':' && val[0] != '<'
		&& isgraph( (unsigned char) val[vlen-1] )
#ifndef LDAP_BINARY_DEBUG
		&& strstr( name, ";binary" ) == NULL
#endif
#ifndef LDAP_PASSWD_DEBUG
		&& !ldif_must_b64_encode( name )
#endif
	) {
		int b64 = 0;

		for ( byte = (const unsigned char *) val; byte < stop;
		    byte++, len++ )
		{
			if ( !isascii( *byte ) || !isprint( *byte ) ) {
				b64 = 1;
				break;
			}
			if ( len >= wrap ) {
				*(*out)++ = '\n';
				*(*out)++ = ' ';
				len = 1;
			}
			*(*out)++ = *byte;
		}

		if( !b64 ) {
			*(*out)++ = '\n';
			return;
		}
	}

	*out = save;
	*(*out)++ = ':';
	*(*out)++ = ' ';
	len = savelen + 2;

	/* convert to base 64 (3 bytes => 4 base 64 digits) */
	for ( byte = (const unsigned char *) val;
		byte < stop - 2;
	    byte += 3 )
	{
		bits = (byte[0] & 0xff) << 16;
		bits |= (byte[1] & 0xff) << 8;
		bits |= (byte[2] & 0xff);

		for ( i = 0; i < 4; i++, len++, bits <<= 6 ) {
			if ( len >= wrap ) {
				*(*out)++ = '\n';
				*(*out)++ = ' ';
				len = 1;
			}

			/* get b64 digit from high order 6 bits */
			*(*out)++ = nib2b64[ (bits & 0xfc0000L) >> 18 ];
		}
	}

	/* add padding if necessary */
	if ( byte < stop ) {
		for ( i = 0; byte + i < stop; i++ ) {
			buf[i] = byte[i];
		}
		for ( pad = 0; i < 3; i++, pad++ ) {
			buf[i] = '\0';
		}
		byte = buf;
		bits = (byte[0] & 0xff) << 16;
		bits |= (byte[1] & 0xff) << 8;
		bits |= (byte[2] & 0xff);

		for ( i = 0; i < 4; i++, len++, bits <<= 6 ) {
			if ( len >= wrap ) {
				*(*out)++ = '\n';
				*(*out)++ = ' ';
				len = 1;
			}

			if( i + pad < 4 ) {
				/* get b64 digit from low order 6 bits */
				*(*out)++ = nib2b64[ (bits & 0xfc0000L) >> 18 ];
			} else {
				*(*out)++ = '=';
			}
		}
	}
	*(*out)++ = '\n';
}


/*
 * ldif_type_and_value return BER malloc'd, zero-terminated LDIF line
 */

/* NOTE: only preserved for binary compatibility */
char *
ldif_put(
	int type,
	LDAP_CONST char *name,
	LDAP_CONST char *val,
	ber_len_t vlen )
{
	return ldif_put_wrap( type, name, val, vlen, 0 );
}

char *
ldif_put_wrap(
	int type,
	LDAP_CONST char *name,
	LDAP_CONST char *val,
	ber_len_t vlen,
	ber_len_t wrap )
{
    char	*buf, *p;
    ber_len_t nlen;

    nlen = ( name != NULL ) ? strlen( name ) : 0;

	buf = (char *) ber_memalloc( LDIF_SIZE_NEEDED_WRAP( nlen, vlen, wrap ) + 1 );

    if ( buf == NULL ) {
		ber_pvt_log_printf( LDAP_DEBUG_ANY, ldif_debug,
			_("ldif_type_and_value: malloc failed!"));
		return NULL;
    }

    p = buf;
    ldif_sput_wrap( &p, type, name, val, vlen, wrap );
    *p = '\0';

    return( buf );
}

int ldif_is_not_printable(
	LDAP_CONST char *val,
	ber_len_t vlen )
{
	if( vlen == 0 || val == NULL  ) {
		return -1;
	}

	if( isgraph( (unsigned char) val[0] ) && val[0] != ':' && val[0] != '<' &&
		isgraph( (unsigned char) val[vlen-1] ) )
	{
		ber_len_t i;

		for ( i = 0; val[i]; i++ ) {
			if ( !isascii( val[i] ) || !isprint( (unsigned char) val[i] ) ) {
				return 1;
			}
		}

		return 0;
	}

	return 1;
}

LDIFFP *
ldif_open(
	LDAP_CONST char *file,
	LDAP_CONST char *mode
)
{
	FILE *fp = fopen( file, mode );
	LDIFFP *lfp = NULL;

	if ( fp ) {
		lfp = ber_memalloc( sizeof( LDIFFP ));
		if ( lfp == NULL ) {
			fclose( fp );
			return NULL;
		}
		lfp->fp = fp;
		lfp->prev = NULL;
	}
	return lfp;
}

LDIFFP *
ldif_open_mem(
	char *ldif,
	size_t size,
	LDAP_CONST char *mode
)
{
#ifdef HAVE_FMEMOPEN
	FILE *fp = fmemopen( ldif, size, mode );
	LDIFFP *lfp = NULL;

	if ( fp ) {
		lfp = ber_memalloc( sizeof( LDIFFP ));
		lfp->fp = fp;
		lfp->prev = NULL;
	}
	return lfp;
#else /* !HAVE_FMEMOPEN */
	return NULL;
#endif /* !HAVE_FMEMOPEN */
}

void
ldif_close(
	LDIFFP *lfp
)
{
	LDIFFP *prev;

	while ( lfp ) {
		fclose( lfp->fp );
		prev = lfp->prev;
		ber_memfree( lfp );
		lfp = prev;
	}
}

#define	LDIF_MAXLINE	4096

/*
 * ldif_read_record - read an ldif record.  Return 1 for success, 0 for EOF,
 * -1 for error.
 */
int
ldif_read_record(
	LDIFFP      *lfp,
	unsigned long *lno,		/* ptr to line number counter              */
	char        **bufp,     /* ptr to malloced output buffer           */
	int         *buflenp )  /* ptr to length of *bufp                  */
{
	char        line[LDIF_MAXLINE], *nbufp;
	ber_len_t   lcur = 0, len;
	int         last_ch = '\n', found_entry = 0, stop, top_comment = 0;

	for ( stop = 0;  !stop;  last_ch = line[len-1] ) {
		/* If we're at the end of this file, see if we should pop
		 * back to a previous file. (return from an include)
		 */
		while ( feof( lfp->fp )) {
pop:
			if ( lfp->prev ) {
				LDIFFP *tmp = lfp->prev;
				fclose( lfp->fp );
				*lfp = *tmp;
				ber_memfree( tmp );
			} else {
				stop = 1;
				break;
			}
		}
		if ( !stop ) {
			if ( fgets( line, sizeof( line ), lfp->fp ) == NULL ) {
				if ( !found_entry && !ferror( lfp->fp ) ) {
					/* ITS#9811 Reached the end looking for an entry, try again */
					goto pop;
				}
				stop = 1;
				len = 0;
			} else {
				len = strlen( line );
			}
		}

		if ( stop ) {
			/* Add \n in case the file does not end with newline */
			if (last_ch != '\n') {
				len = 1;
				line[0] = '\n';
				line[1] = '\0';
				goto last;
			}
			break;
		}

		/* Squash \r\n to \n */
		if ( len > 1 && line[len-2] == '\r' ) {
			len--;
			line[len]   = '\0';
			line[len-1] = '\n';
		}

		if ( last_ch == '\n' ) {
			(*lno)++;

			if ( line[0] == '\n' ) {
				if ( !found_entry ) {
					lcur = 0;
					top_comment = 0;
					continue;
				}
				break;
			}

			if ( !found_entry ) {
				if ( line[0] == '#' ) {
					top_comment = 1;
				} else if ( ! ( top_comment && line[0] == ' ' ) ) {
					/* Found a new entry */
					found_entry = 1;

					if ( isdigit( (unsigned char) line[0] ) ) {
						/* skip index */
						continue;
					}
					if ( !strncasecmp( line, "include:",
						STRLENOF("include:"))) {
						FILE *fp2;
						char *ptr;
						found_entry = 0;

						if ( line[len-1] == '\n' ) {
							len--;
							line[len] = '\0';
						}

						ptr = line + STRLENOF("include:");
						while (isspace((unsigned char) *ptr)) ptr++;
						fp2 = ldif_open_url( ptr );
						if ( fp2 ) {
							LDIFFP *lnew = ber_memalloc( sizeof( LDIFFP ));
							if ( lnew == NULL ) {
								fclose( fp2 );
								return 0;
							}
							lnew->prev = lfp->prev;
							lnew->fp = lfp->fp;
							lfp->prev = lnew;
							lfp->fp = fp2;
							line[len] = '\n';
							len++;
							continue;
						} else {
							/* We failed to open the file, this should
							 * be reported as an error somehow.
							 */
							ber_pvt_log_printf( LDAP_DEBUG_ANY, ldif_debug,
								_("ldif_read_record: include %s failed\n"), ptr );
							return -1;
						}
					}
				}
			}
		}

last:
		if ( *buflenp - lcur <= len ) {
			*buflenp += len + LDIF_MAXLINE;
			nbufp = ber_memrealloc( *bufp, *buflenp );
			if( nbufp == NULL ) {
				return 0;
			}
			*bufp = nbufp;
		}
		strcpy( *bufp + lcur, line );
		lcur += len;
	}

	return( found_entry );
}
