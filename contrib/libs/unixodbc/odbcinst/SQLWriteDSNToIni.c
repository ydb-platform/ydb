/**************************************************
 * SQLWriteDSNToIni
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

extern void __clear_ini_cache( void );

BOOL SQLWriteDSNToIni(			LPCSTR	pszDSN,
								LPCSTR	pszDriver )
{
	HINI	hIni;
	char	szFileName[ODBC_FILENAME_MAX+1];

	SQLRemoveDSNFromIni( pszDSN );

	/* SANITY CHECKS */
	if ( pszDSN == NULL )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return FALSE;
	}
	if ( pszDSN[0] == '\0' )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return FALSE;
	}
	if ( (strcasecmp( pszDSN, "DEFAULT" ) != 0 ) && (pszDriver == NULL ) )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_NAME, "" );
		return FALSE;
	}
	if ( (strcasecmp( pszDSN, "DEFAULT" ) != 0 ) && (pszDriver[0] == '\0') )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_NAME, "" );
		return FALSE;
	}
	if ( SQLValidDSN( pszDSN ) == FALSE )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_DSN, "" );
		return FALSE;
	}

	/* OK */
	if ( _odbcinst_ConfigModeINI( szFileName ) == FALSE )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_REQUEST_FAILED, "" );
		return FALSE;
	}
#ifdef __OS2__
	if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE, 1L  ) != INI_SUCCESS )
#else
	if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE  ) != INI_SUCCESS )
#endif
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_REQUEST_FAILED, "" );
		return FALSE;
	}
	iniObjectInsert( hIni, (char *)pszDSN );
	if ( pszDriver != NULL )
	{
		iniPropertyInsert( hIni, "Driver", (char *)pszDriver );
	}
	if ( iniCommit( hIni ) != INI_SUCCESS )
	{
		iniClose( hIni );
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_REQUEST_FAILED, "" );
		return FALSE;
	}

	iniClose( hIni );

    __clear_ini_cache();

	return TRUE;
}

BOOL INSTAPI SQLWriteDSNToIniW        (LPCWSTR     lpszDSN,
                                      LPCWSTR     lpszDriver)
{
	char *drv, *dsn;
	BOOL ret;

	dsn = _single_string_alloc_and_copy( lpszDSN );
	drv = _single_string_alloc_and_copy( lpszDriver );

	ret = SQLWriteDSNToIni( dsn, drv );

	free( dsn );
	free( drv );

	return ret;
}
