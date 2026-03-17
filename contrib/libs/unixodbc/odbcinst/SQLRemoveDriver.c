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

BOOL SQLRemoveDriver(			LPCSTR	pszDriver,
								BOOL	nRemoveDSN,
								LPDWORD	pnUsageCount )
{
	HINI	hODBCInstIni;
	char	szValue[INI_MAX_PROPERTY_VALUE+1];
    char    szIniName[ ODBC_FILENAME_MAX * 2 + 1 ];
	char	b1[ ODBC_FILENAME_MAX + 1 ], b2[ ODBC_FILENAME_MAX + 1 ];

    inst_logClear();

	/* SANITY CHECKS */
	if ( pszDriver == NULL )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_NAME, "" );
		return FALSE;
	}
	if ( pszDriver[0] == '\0' )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_NAME, "" );
		return FALSE;
	}
	if ( nRemoveDSN != TRUE && nRemoveDSN != FALSE )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return FALSE;
	}

	(*pnUsageCount) = 0;

#ifdef VMS
    sprintf( szIniName, "%s:%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#else
    sprintf( szIniName, "%s/%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#endif

	/* PROCESS ODBC INST INI FILE */
#ifdef __OS2__	
	if ( iniOpen( &hODBCInstIni, szIniName, "#;", '[', ']', '=', TRUE, 1L ) != INI_SUCCESS )
#else
	if ( iniOpen( &hODBCInstIni, szIniName, "#;", '[', ']', '=', TRUE ) != INI_SUCCESS )
#endif
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_COMPONENT_NOT_FOUND, "" );
		return FALSE;
	}

	/* LETS GET ITS FILE USAGE VALUE (if any) */
	if ( iniPropertySeek( hODBCInstIni, (char *)pszDriver, "UsageCount", "" ) == INI_SUCCESS )
	{
		iniValue( hODBCInstIni, szValue );
        (*pnUsageCount) = atoi( szValue );
	}

	/* DOES THE OBJECT ALREADY EXIST? (also ensures that we have correct current object) */	
	if ( iniObjectSeek( hODBCInstIni, (char *)pszDriver ) == INI_SUCCESS )
	{
        if ( (*pnUsageCount) == 0 )
			(*pnUsageCount) = 1;

		(*pnUsageCount)--;
		if ( (*pnUsageCount) == 0 )
		{
			iniObjectDelete( hODBCInstIni );
			if ( nRemoveDSN )
			{
				/***********************************
				 * TO DO
				 ***********************************/
			}
		}
		else
		{
			if ( iniPropertySeek( hODBCInstIni, (char *)pszDriver, "UsageCount", "" ) == INI_SUCCESS )
			{
				sprintf( szValue, "%ld", (long int)(*pnUsageCount) );
				iniPropertyUpdate( hODBCInstIni, "UsageCount", szValue );
			}
			else
			{
				iniPropertyInsert( hODBCInstIni, "UsageCount", szValue );
			}
		}
		if ( iniCommit( hODBCInstIni ) != INI_SUCCESS )
		{
			inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
			iniClose( hODBCInstIni );
			return FALSE;
		}
	}


	iniClose( hODBCInstIni );

	return TRUE;
}

BOOL INSTAPI SQLRemoveDriverW(LPCWSTR lpszDriver,
                             BOOL fRemoveDSN,
                             LPDWORD lpdwUsageCount)
{
	BOOL ret;
	char *drv = _single_string_alloc_and_copy( lpszDriver );

    inst_logClear();

	ret = SQLRemoveDriver( drv, fRemoveDSN, lpdwUsageCount );

	free( drv );

	return ret;
}
