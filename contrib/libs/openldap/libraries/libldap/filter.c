/* search.c */
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
/* Portions Copyright (c) 1990 Regents of the University of Michigan.
 * All rights reserved.
 */

#include "portable.h"

#include <stdio.h>

#include <ac/stdlib.h>

#include <ac/socket.h>
#include <ac/string.h>
#include <ac/time.h>

#include "ldap-int.h"

static int put_simple_vrFilter LDAP_P((
	BerElement *ber,
	char *str ));

static int put_vrFilter_list LDAP_P((
	BerElement *ber,
	char *str ));

static char *put_complex_filter LDAP_P((
	BerElement *ber,
	char *str,
	ber_tag_t tag,
	int not ));

static int put_simple_filter LDAP_P((
	BerElement *ber,
	char *str ));

static int put_substring_filter LDAP_P((
	BerElement *ber,
	char *type,
	char *str,
	char *nextstar ));

static int put_filter_list LDAP_P((
	BerElement *ber,
	char *str,
	ber_tag_t tag ));

static int ldap_is_oid ( const char *str )
{
	int i;

	if( LDAP_ALPHA( str[0] )) {
		for( i=1; str[i]; i++ ) {
			if( !LDAP_LDH( str[i] )) {
				return 0;
			}
		}
		return 1;

	} else if LDAP_DIGIT( str[0] ) {
		int dot=0;
		for( i=1; str[i]; i++ ) {
			if( LDAP_DIGIT( str[i] )) {
				dot=0;

			} else if ( str[i] == '.' ) {
				if( ++dot > 1 ) return 0;

			} else {
				return 0;
			}
		}
		return !dot;
	}

	return 0;
}

static int ldap_is_desc ( const char *str )
{
	int i;

	if( LDAP_ALPHA( str[0] )) {
		for( i=1; str[i]; i++ ) {
			if( str[i] == ';' ) {
				str = &str[i+1];
				goto options;
			}

			if( !LDAP_LDH( str[i] )) {
				return 0;
			}
		}
		return 1;

	} else if LDAP_DIGIT( str[0] ) {
		int dot=0;
		for( i=1; str[i]; i++ ) {
			if( str[i] == ';' ) {
				if( dot ) return 0;
				str = &str[i+1];
				goto options;
			}

			if( LDAP_DIGIT( str[i] )) {
				dot=0;

			} else if ( str[i] == '.' ) {
				if( ++dot > 1 ) return 0;

			} else {
				return 0;
			}
		}
		return !dot;
	}

	return 0;

options:
	if( !LDAP_LDH( str[0] )) {
		return 0;
	}
	for( i=1; str[i]; i++ ) {
		if( str[i] == ';' ) {
			str = &str[i+1];
			goto options;
		}
		if( !LDAP_LDH( str[i] )) {
			return 0;
		}
	}
	return 1;
}

static char *
find_right_paren( char *s )
{
	int	balance, escape;

	balance = 1;
	escape = 0;
	while ( *s && balance ) {
		if ( !escape ) {
			if ( *s == '(' ) {
				balance++;
			} else if ( *s == ')' ) {
				balance--;
			}
		}

		escape = ( *s == '\\' && !escape );

		if ( balance ) s++;
	}

	return *s ? s : NULL;
}

static int hex2value( int c )
{
	if( c >= '0' && c <= '9' ) {
		return c - '0';
	}

	if( c >= 'A' && c <= 'F' ) {
		return c + (10 - (int) 'A');
	}

	if( c >= 'a' && c <= 'f' ) {
		return c + (10 - (int) 'a');
	}

	return -1;
}

char *
ldap_pvt_find_wildcard( const char *s )
{
	for( ; *s; s++ ) {
		switch( *s ) {
		case '*':	/* found wildcard */
			return (char *) s;

		case '(':
		case ')':
			return NULL;

		case '\\':
			if( s[1] == '\0' ) return NULL;

			if( LDAP_HEX( s[1] ) && LDAP_HEX( s[2] ) ) {
				s+=2;

			} else switch( s[1] ) {
			default:
				return NULL;

			/* allow RFC 1960 escapes */
			case '*':
			case '(':
			case ')':
			case '\\':
				s++;
			}
		}
	}

	return (char *) s;
}

/* unescape filter value */
/* support both LDAP v2 and v3 escapes */
/* output can include nul characters! */
ber_slen_t
ldap_pvt_filter_value_unescape( char *fval )
{
	ber_slen_t r, v;
	int v1, v2;

	for( r=v=0; fval[v] != '\0'; v++ ) {
		switch( fval[v] ) {
		case '(':
		case ')':
		case '*':
			return -1;

		case '\\':
			/* escape */
			v++;

			if ( fval[v] == '\0' ) {
				/* escape at end of string */
				return -1;
			}

			if (( v1 = hex2value( fval[v] )) >= 0 ) {
				/* LDAPv3 escape */
				if (( v2 = hex2value( fval[v+1] )) < 0 ) {
					/* must be two digit code */
					return -1;
				}

				fval[r++] = v1 * 16 + v2;
				v++;

			} else {
				/* LDAPv2 escape */
				switch( fval[v] ) {
				case '(':
				case ')':
				case '*':
				case '\\':
					fval[r++] = fval[v];
					break;
				default:
					/* illegal escape */
					return -1;
				}
			}
			break;

		default:
			fval[r++] = fval[v];
		}
	}

	fval[r] = '\0';
	return r;
}

static char *
put_complex_filter( BerElement *ber, char *str, ber_tag_t tag, int not )
{
	char	*next;

	/*
	 * We have (x(filter)...) with str sitting on
	 * the x.  We have to find the paren matching
	 * the one before the x and put the intervening
	 * filters by calling put_filter_list().
	 */

	/* put explicit tag */
	if ( ber_printf( ber, "t{" /*"}"*/, tag ) == -1 ) {
		return NULL;
	}

	str++;
	if ( (next = find_right_paren( str )) == NULL ) {
		return NULL;
	}

	*next = '\0';
	if ( put_filter_list( ber, str, tag ) == -1 ) {
		return NULL;
	}

	/* close the '(' */
	*next++ = ')';

	/* flush explicit tagged thang */
	if ( ber_printf( ber, /*"{"*/ "N}" ) == -1 ) {
		return NULL;
	}

	return next;
}

int
ldap_pvt_put_filter( BerElement *ber, const char *str_in )
{
	int rc;
	char	*freeme;
	char	*str;
	char	*next;
	int	parens, balance, escape;

	/*
	 * A Filter looks like this (RFC 4511 as extended by RFC 4526):
	 *     Filter ::= CHOICE {
	 *         and             [0]     SET SIZE (0..MAX) OF filter Filter,
	 *         or              [1]     SET SIZE (0..MAX) OF filter Filter,
	 *         not             [2]     Filter,
	 *         equalityMatch   [3]     AttributeValueAssertion,
	 *         substrings      [4]     SubstringFilter,
	 *         greaterOrEqual  [5]     AttributeValueAssertion,
	 *         lessOrEqual     [6]     AttributeValueAssertion,
	 *         present         [7]     AttributeDescription,
	 *         approxMatch     [8]     AttributeValueAssertion,
	 *         extensibleMatch [9]     MatchingRuleAssertion,
	 *         ... }
	 *
	 *     SubstringFilter ::= SEQUENCE {
	 *         type         AttributeDescription,
	 *         substrings   SEQUENCE SIZE (1..MAX) OF substring CHOICE {
	 *             initial          [0] AssertionValue, -- only once
	 *             any              [1] AssertionValue,
	 *             final            [2] AssertionValue  -- only once
	 *             }
	 *         }
	 *
	 *	   MatchingRuleAssertion ::= SEQUENCE {
	 *         matchingRule    [1] MatchingRuleId OPTIONAL,
	 *         type            [2] AttributeDescription OPTIONAL,
	 *         matchValue      [3] AssertionValue,
	 *         dnAttributes    [4] BOOLEAN DEFAULT FALSE }
	 *
	 * Note: tags in a CHOICE are always explicit
	 */

	Debug1( LDAP_DEBUG_TRACE, "put_filter: \"%s\"\n", str_in );

	freeme = LDAP_STRDUP( str_in );
	if( freeme == NULL ) return LDAP_NO_MEMORY;
	str = freeme;

	parens = 0;
	while ( *str ) {
		switch ( *str ) {
		case '(': /*')'*/
			str++;
			parens++;

			/* skip spaces */
			while( LDAP_SPACE( *str ) ) str++;

			switch ( *str ) {
			case '&':
				Debug0( LDAP_DEBUG_TRACE, "put_filter: AND\n" );

				str = put_complex_filter( ber, str,
				    LDAP_FILTER_AND, 0 );
				if( str == NULL ) {
					rc = -1;
					goto done;
				}

				parens--;
				break;

			case '|':
				Debug0( LDAP_DEBUG_TRACE, "put_filter: OR\n" );

				str = put_complex_filter( ber, str,
				    LDAP_FILTER_OR, 0 );
				if( str == NULL ) {
					rc = -1;
					goto done;
				}

				parens--;
				break;

			case '!':
				Debug0( LDAP_DEBUG_TRACE, "put_filter: NOT\n" );

				str = put_complex_filter( ber, str,
				    LDAP_FILTER_NOT, 0 );
				if( str == NULL ) {
					rc = -1;
					goto done;
				}

				parens--;
				break;

			case '(':
				rc = -1;
				goto done;

			default:
				Debug0( LDAP_DEBUG_TRACE, "put_filter: simple\n" );

				balance = 1;
				escape = 0;
				next = str;

				while ( *next && balance ) {
					if ( escape == 0 ) {
						if ( *next == '(' ) {
							balance++;
						} else if ( *next == ')' ) {
							balance--;
						}
					}

					if ( *next == '\\' && ! escape ) {
						escape = 1;
					} else {
						escape = 0;
					}

					if ( balance ) next++;
				}

				if ( balance != 0 ) {
					rc = -1;
					goto done;
				}

				*next = '\0';

				if ( put_simple_filter( ber, str ) == -1 ) {
					rc = -1;
					goto done;
				}

				*next++ = /*'('*/ ')';

				str = next;
				parens--;
				break;
			}
			break;

		case /*'('*/ ')':
			Debug0( LDAP_DEBUG_TRACE, "put_filter: end\n" );
			if ( ber_printf( ber, /*"["*/ "]" ) == -1 ) {
				rc = -1;
				goto done;
			}
			str++;
			parens--;
			break;

		case ' ':
			str++;
			break;

		default:	/* assume it's a simple type=value filter */
			Debug0( LDAP_DEBUG_TRACE, "put_filter: default\n" );
			next = strchr( str, '\0' );
			if ( put_simple_filter( ber, str ) == -1 ) {
				rc = -1;
				goto done;
			}
			str = next;
			break;
		}
		if ( !parens )
			break;
	}

	rc = ( parens || *str ) ? -1 : 0;

done:
	LDAP_FREE( freeme );
	return rc;
}

/*
 * Put a list of filters like this "(filter1)(filter2)..."
 */

static int
put_filter_list( BerElement *ber, char *str, ber_tag_t tag )
{
	char	*next = NULL;
	char	save;

	Debug1( LDAP_DEBUG_TRACE, "put_filter_list \"%s\"\n",
		str );

	while ( *str ) {
		while ( *str && LDAP_SPACE( (unsigned char) *str ) ) {
			str++;
		}
		if ( *str == '\0' ) break;

		if ( (next = find_right_paren( str + 1 )) == NULL ) {
			return -1;
		}
		save = *++next;

		/* now we have "(filter)" with str pointing to it */
		*next = '\0';
		if ( ldap_pvt_put_filter( ber, str ) == -1 ) return -1;
		*next = save;
		str = next;

		if( tag == LDAP_FILTER_NOT ) break;
	}

	if( tag == LDAP_FILTER_NOT && ( next == NULL || *str )) {
		return -1;
	}

	return 0;
}

static int
put_simple_filter(
	BerElement *ber,
	char *str )
{
	char		*s;
	char		*value;
	ber_tag_t	ftype;
	int		rc = -1;

	Debug1( LDAP_DEBUG_TRACE, "put_simple_filter: \"%s\"\n",
		str );

	if ( str[0] == '=' ) return -1;

	str = LDAP_STRDUP( str );
	if( str == NULL ) return -1;

	if ( (s = strchr( str, '=' )) == NULL ) {
		goto done;
	}

	value = s + 1;
	*s-- = '\0';

	switch ( *s ) {
	case '<':
		ftype = LDAP_FILTER_LE;
		*s = '\0';
		break;

	case '>':
		ftype = LDAP_FILTER_GE;
		*s = '\0';
		break;

	case '~':
		ftype = LDAP_FILTER_APPROX;
		*s = '\0';
		break;

	case ':':
		/* RFC 4515 extensible filters are off the form:
		 *		type [:dn] [:rule] := value
		 * or	[:dn]:rule := value		
		 */
		ftype = LDAP_FILTER_EXT;
		*s = '\0';

		{
			char *dn = strchr( str, ':' );
			char *rule = NULL;

			if( dn != NULL ) {
				*dn++ = '\0';
				rule = strchr( dn, ':' );

				if( rule == NULL ) {
					/* one colon */
					if ( strcasecmp(dn, "dn") == 0 ) {
						/* must have attribute */
						if( !ldap_is_desc( str ) ) {
							goto done;
						}

						rule = "";

					} else {
					  rule = dn;
					  dn = NULL;
					}
				
				} else {
					/* two colons */
					*rule++ = '\0';

					if ( strcasecmp(dn, "dn") != 0 ) {
						/* must have "dn" */
						goto done;
					}
				}

			}

			if ( *str == '\0' && ( !rule || *rule == '\0' ) ) {
				/* must have either type or rule */
				goto done;
			}

			if ( *str != '\0' && !ldap_is_desc( str ) ) {
				goto done;
			}

			if ( rule && *rule != '\0' && !ldap_is_oid( rule ) ) {
				goto done;
			}

			rc = ber_printf( ber, "t{" /*"}"*/, ftype );

			if( rc != -1 && rule && *rule != '\0' ) {
				rc = ber_printf( ber, "ts", LDAP_FILTER_EXT_OID, rule );
			}

			if( rc != -1 && *str != '\0' ) {
				rc = ber_printf( ber, "ts", LDAP_FILTER_EXT_TYPE, str );
			}

			if( rc != -1 ) {
				ber_slen_t len = ldap_pvt_filter_value_unescape( value );

				if( len >= 0 ) {
					rc = ber_printf( ber, "to",
						LDAP_FILTER_EXT_VALUE, value, len );
				} else {
					rc = -1;
				}
			}

			if( rc != -1 && dn ) {
				rc = ber_printf( ber, "tb",
					LDAP_FILTER_EXT_DNATTRS, (ber_int_t) 1 );
			}

			if( rc != -1 ) { 
				rc = ber_printf( ber, /*"{"*/ "N}" );
			}
		}
		goto done;

	default:
		if( !ldap_is_desc( str ) ) {
			goto done;

		} else {
			char *nextstar = ldap_pvt_find_wildcard( value );

			if ( nextstar == NULL ) {
				goto done;

			} else if ( *nextstar == '\0' ) {
				ftype = LDAP_FILTER_EQUALITY;

			} else if ( strcmp( value, "*" ) == 0 ) {
				ftype = LDAP_FILTER_PRESENT;

			} else {
				rc = put_substring_filter( ber, str, value, nextstar );
				goto done;
			}
		} break;
	}

	if( !ldap_is_desc( str ) ) goto done;

	if ( ftype == LDAP_FILTER_PRESENT ) {
		rc = ber_printf( ber, "ts", ftype, str );

	} else {
		ber_slen_t len = ldap_pvt_filter_value_unescape( value );

		if( len >= 0 ) {
			rc = ber_printf( ber, "t{soN}",
				ftype, str, value, len );
		}
	}

done:
	if( rc != -1 ) rc = 0;
	LDAP_FREE( str );
	return rc;
}

static int
put_substring_filter( BerElement *ber, char *type, char *val, char *nextstar )
{
	int gotstar = 0;
	ber_tag_t	ftype = LDAP_FILTER_SUBSTRINGS;

	Debug2( LDAP_DEBUG_TRACE, "put_substring_filter \"%s=%s\"\n",
		type, val );

	if ( ber_printf( ber, "t{s{" /*"}}"*/, ftype, type ) == -1 ) {
		return -1;
	}

	for( ; *val; val=nextstar ) {
		if ( gotstar )
			nextstar = ldap_pvt_find_wildcard( val );

		if ( nextstar == NULL ) {
			return -1;
		}

		if ( *nextstar == '\0' ) {
			ftype = LDAP_SUBSTRING_FINAL;
		} else {
			*nextstar++ = '\0';
			if ( gotstar++ == 0 ) {
				ftype = LDAP_SUBSTRING_INITIAL;
			} else {
				ftype = LDAP_SUBSTRING_ANY;
			}
		}

		if ( *val != '\0' || ftype == LDAP_SUBSTRING_ANY ) {
			ber_slen_t len = ldap_pvt_filter_value_unescape( val );

			if ( len <= 0  ) {
				return -1;
			}

			if ( ber_printf( ber, "to", ftype, val, len ) == -1 ) {
				return -1;
			}
		}
	}

	if ( ber_printf( ber, /*"{{"*/ "N}N}" ) == -1 ) {
		return -1;
	}

	return 0;
}

static int
put_vrFilter( BerElement *ber, const char *str_in )
{
	int rc;
	char	*freeme;
	char	*str;
	char	*next;
	int	parens, balance, escape;

	/*
	 * A ValuesReturnFilter looks like this:
	 *
	 *	ValuesReturnFilter ::= SEQUENCE OF SimpleFilterItem
	 *      SimpleFilterItem ::= CHOICE {
	 *              equalityMatch   [3]     AttributeValueAssertion,
	 *              substrings      [4]     SubstringFilter,
	 *              greaterOrEqual  [5]     AttributeValueAssertion,
	 *              lessOrEqual     [6]     AttributeValueAssertion,
	 *              present         [7]     AttributeType,
	 *              approxMatch     [8]     AttributeValueAssertion,
	 *		extensibleMatch [9]	SimpleMatchingAssertion -- LDAPv3
	 *      }
	 *
	 *      SubstringFilter ::= SEQUENCE {
	 *              type               AttributeType,
	 *              SEQUENCE OF CHOICE {
	 *                      initial          [0] IA5String,
	 *                      any              [1] IA5String,
	 *                      final            [2] IA5String
	 *              }
	 *      }
	 *
	 *	SimpleMatchingAssertion ::= SEQUENCE {	-- LDAPv3
	 *		matchingRule    [1] MatchingRuleId OPTIONAL,
	 *		type            [2] AttributeDescription OPTIONAL,
	 *		matchValue      [3] AssertionValue }
	 *
	 * (Source: RFC 3876)
	 */

	Debug1( LDAP_DEBUG_TRACE, "put_vrFilter: \"%s\"\n", str_in );

	freeme = LDAP_STRDUP( str_in );
	if( freeme == NULL ) return LDAP_NO_MEMORY;
	str = freeme;

	parens = 0;
	while ( *str ) {
		switch ( *str ) {
		case '(': /*')'*/
			str++;
			parens++;

			/* skip spaces */
			while( LDAP_SPACE( *str ) ) str++;

			switch ( *str ) {
			case '(':
				if ( (next = find_right_paren( str )) == NULL ) {
					rc = -1;
					goto done;
				}

				*next = '\0';

				if ( put_vrFilter_list( ber, str ) == -1 ) {
					rc = -1;
					goto done;
				}

				/* close the '(' */
				*next++ = ')';

				str = next;

				parens--;
				break;


			default:
				Debug0( LDAP_DEBUG_TRACE, "put_vrFilter: simple\n" );

				balance = 1;
				escape = 0;
				next = str;

				while ( *next && balance ) {
					if ( escape == 0 ) {
						if ( *next == '(' ) {
							balance++;
						} else if ( *next == ')' ) {
							balance--;
						}
					}

					if ( *next == '\\' && ! escape ) {
						escape = 1;
					} else {
						escape = 0;
					}

					if ( balance ) next++;
				}

				if ( balance != 0 ) {
					rc = -1;
					goto done;
				}

				*next = '\0';

				if ( put_simple_vrFilter( ber, str ) == -1 ) {
					rc = -1;
					goto done;
				}

				*next++ = /*'('*/ ')';

				str = next;
				parens--;
				break;
			}
			break;

		case /*'('*/ ')':
			Debug0( LDAP_DEBUG_TRACE, "put_vrFilter: end\n" );
			if ( ber_printf( ber, /*"["*/ "]" ) == -1 ) {
				rc = -1;
				goto done;
			}
			str++;
			parens--;
			break;

		case ' ':
			str++;
			break;

		default:	/* assume it's a simple type=value filter */
			Debug0( LDAP_DEBUG_TRACE, "put_vrFilter: default\n" );
			next = strchr( str, '\0' );
			if ( put_simple_vrFilter( ber, str ) == -1 ) {
				rc = -1;
				goto done;
			}
			str = next;
			break;
		}
	}

	rc = parens ? -1 : 0;

done:
	LDAP_FREE( freeme );
	return rc;
}

int
ldap_put_vrFilter( BerElement *ber, const char *str_in )
{
	int rc =0;
	
	if ( ber_printf( ber, "{" /*"}"*/ ) == -1 ) {
		return -1;
	}
	
	rc = put_vrFilter( ber, str_in );

	if ( ber_printf( ber, /*"{"*/ "N}" ) == -1 ) {
		rc = -1;
	}
	
	return rc;
}

static int
put_vrFilter_list( BerElement *ber, char *str )
{
	char	*next = NULL;
	char	save;

	Debug1( LDAP_DEBUG_TRACE, "put_vrFilter_list \"%s\"\n",
		str );

	while ( *str ) {
		while ( *str && LDAP_SPACE( (unsigned char) *str ) ) {
			str++;
		}
		if ( *str == '\0' ) break;

		if ( (next = find_right_paren( str + 1 )) == NULL ) {
			return -1;
		}
		save = *++next;

		/* now we have "(filter)" with str pointing to it */
		*next = '\0';
		if ( put_vrFilter( ber, str ) == -1 ) return -1;
		*next = save;
		str = next;
	}

	return 0;
}

static int
put_simple_vrFilter(
	BerElement *ber,
	char *str )
{
	char		*s;
	char		*value;
	ber_tag_t	ftype;
	int		rc = -1;

	Debug1( LDAP_DEBUG_TRACE, "put_simple_vrFilter: \"%s\"\n",
		str );

	str = LDAP_STRDUP( str );
	if( str == NULL ) return -1;

	if ( (s = strchr( str, '=' )) == NULL ) {
		goto done;
	}

	value = s + 1;
	*s-- = '\0';

	switch ( *s ) {
	case '<':
		ftype = LDAP_FILTER_LE;
		*s = '\0';
		break;

	case '>':
		ftype = LDAP_FILTER_GE;
		*s = '\0';
		break;

	case '~':
		ftype = LDAP_FILTER_APPROX;
		*s = '\0';
		break;

	case ':':
		/* According to ValuesReturnFilter control definition
		 * extensible filters are off the form:
		 *		type [:rule] := value
		 * or	:rule := value		
		 */
		ftype = LDAP_FILTER_EXT;
		*s = '\0';

		{
			char *rule = strchr( str, ':' );

			if( rule == NULL ) {
				/* must have attribute */
				if( !ldap_is_desc( str ) ) {
					goto done;
				}
				rule = "";
			} else {
				*rule++ = '\0';
			}

			if ( *str == '\0' && ( !rule || *rule == '\0' ) ) {
				/* must have either type or rule */
				goto done;
			}

			if ( *str != '\0' && !ldap_is_desc( str ) ) {
				goto done;
			}

			if ( rule && *rule != '\0' && !ldap_is_oid( rule ) ) {
				goto done;
			}

			rc = ber_printf( ber, "t{" /*"}"*/, ftype );

			if( rc != -1 && rule && *rule != '\0' ) {
				rc = ber_printf( ber, "ts", LDAP_FILTER_EXT_OID, rule );
			}

			if( rc != -1 && *str != '\0' ) {
				rc = ber_printf( ber, "ts", LDAP_FILTER_EXT_TYPE, str );
			}

			if( rc != -1 ) {
				ber_slen_t len = ldap_pvt_filter_value_unescape( value );

				if( len >= 0 ) {
					rc = ber_printf( ber, "to",
						LDAP_FILTER_EXT_VALUE, value, len );
				} else {
					rc = -1;
				}
			}

			if( rc != -1 ) { 
				rc = ber_printf( ber, /*"{"*/ "N}" );
			}
		}
		goto done;

	default:
		if( !ldap_is_desc( str ) ) {
			goto done;

		} else {
			char *nextstar = ldap_pvt_find_wildcard( value );

			if ( nextstar == NULL ) {
				goto done;

			} else if ( *nextstar == '\0' ) {
				ftype = LDAP_FILTER_EQUALITY;

			} else if ( strcmp( value, "*" ) == 0 ) {
				ftype = LDAP_FILTER_PRESENT;

			} else {
				rc = put_substring_filter( ber, str, value, nextstar );
				goto done;
			}
		} break;
	}

	if( !ldap_is_desc( str ) ) goto done;

	if ( ftype == LDAP_FILTER_PRESENT ) {
		rc = ber_printf( ber, "ts", ftype, str );

	} else {
		ber_slen_t len = ldap_pvt_filter_value_unescape( value );

		if( len >= 0 ) {
			rc = ber_printf( ber, "t{soN}",
				ftype, str, value, len );
		}
	}

done:
	if( rc != -1 ) rc = 0;
	LDAP_FREE( str );
	return rc;
}

