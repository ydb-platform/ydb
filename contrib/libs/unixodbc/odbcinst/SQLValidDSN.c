/**************************************************
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/
#include <config.h>
#include <odbcinstext.h>

#define SQL_MAX_DSN_LENGTH 32

BOOL SQLValidDSN( LPCSTR	pszDSN )
{
    inst_logClear();

	if ( pszDSN == NULL )
		return FALSE;

	if ( strlen( pszDSN ) < 1 || strlen( pszDSN ) > SQL_MAX_DSN_LENGTH )
		return FALSE;

	if ( strstr( pszDSN, "[" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "]" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "{" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "}" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "(" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, ")" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "," ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, ";" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "?" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "*" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "=" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "!" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "@" ) != NULL )
		return FALSE;

	if ( strstr( pszDSN, "\\" ) != NULL )
		return FALSE;


	return TRUE;
}

BOOL INSTAPI SQLValidDSNW(LPCWSTR lpszDSN)
{
	char *dsn;
	BOOL ret;

    inst_logClear();

	dsn = _single_string_alloc_and_copy( lpszDSN );

	ret = SQLValidDSN( dsn );

	free( dsn );

	return ret;
}
