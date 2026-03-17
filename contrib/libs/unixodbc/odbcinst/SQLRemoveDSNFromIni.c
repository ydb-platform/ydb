/********************************************
 * SQLRemoveDSNFromIni
 *
 * Use the current Config Mode to determine the
 * odbc.ini we will use.
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

BOOL SQLRemoveDSNFromIni(LPCSTR	pszDSN )
{
	HINI	hIni;
	char	szINIFileName[ODBC_FILENAME_MAX+1];

    inst_logClear();

	/* SANITY CHECKS */
	if ( pszDSN == NULL )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_DSN, "" );
		return FALSE;
	}

	if ( pszDSN[0] == '\0' )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_DSN, "" );
		return FALSE;
	}

	/* GET ODBC INI FILE NAME */
	if ( _odbcinst_ConfigModeINI( szINIFileName ) == FALSE )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_COMPONENT_NOT_FOUND, "" );
		return FALSE;
	}

#ifdef __OS2__
	if ( iniOpen( &hIni, szINIFileName, "#;", '[', ']', '=', FALSE, 1L ) != INI_SUCCESS )
#else
	if ( iniOpen( &hIni, szINIFileName, "#;", '[', ']', '=', FALSE ) != INI_SUCCESS )
#endif
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_COMPONENT_NOT_FOUND, "" );
		return FALSE;
	}

	if ( iniObjectSeek( hIni, (char *)pszDSN ) == INI_SUCCESS )
	{
		iniObjectDelete( hIni );
		if ( iniCommit( hIni ) != INI_SUCCESS )
		{
			inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
			iniClose( hIni );
			return FALSE;
		}
	}
	iniClose( hIni );

	return TRUE;
}

BOOL INSTAPI SQLRemoveDSNFromIniW(LPCWSTR lpszDSN)
{
	char *dsn;
	BOOL ret;

    inst_logClear();

	dsn = _single_string_alloc_and_copy( lpszDSN );

	ret = SQLRemoveDSNFromIni( dsn );

	free( dsn );

	return ret;
}
