/**************************************************
 * SQLWritePrivateProfileString
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

BOOL SQLWritePrivateProfileString(
								LPCSTR	pszSection,
								LPCSTR	pszEntry,
								LPCSTR	pszString,
								LPCSTR	pszFileName )
{
	HINI	hIni;
	char	szFileName[ODBC_FILENAME_MAX+1];

        inst_logClear();

	/* SANITY CHECKS */
	if ( pszSection == NULL )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return FALSE;
	}
	if ( pszSection[0] == '\0' )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return FALSE;
	}
	if ( pszFileName == NULL )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return FALSE;
	}

	/*****************************************************
	 * SOME MS CODE (ie some drivers) MAY USE THIS FUNCTION TO WRITE ODBCINST INFO SO...
	 *****************************************************/
	if ( strstr( pszFileName, "odbcinst" ) || strstr( pszFileName, "ODBCINST" ) )
		return _SQLWriteInstalledDrivers( pszSection, pszEntry, pszString );

	if ( pszFileName[0] == '/' )
	{
		strcpy( szFileName, pszFileName );
	}
	else
	{
		if ( !*pszFileName || _odbcinst_ConfigModeINI( szFileName ) == FALSE )
		{
        	inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_REQUEST_FAILED, "" );
			return FALSE;
		}
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

	/* delete section */
	if ( pszEntry == NULL )
	{
		if ( iniObjectSeek( hIni, (char *)pszSection ) == INI_SUCCESS )
			iniObjectDelete( hIni );
	}
	/* delete entry */
	else if	( pszString == NULL )
	{
		if ( iniPropertySeek( hIni, (char *)pszSection, (char *)pszEntry, "" ) == INI_SUCCESS )
        {
			iniPropertyDelete( hIni );
        }
	}
	else
	{
		/* add section */
		if ( iniObjectSeek( hIni, (char *)pszSection ) != INI_SUCCESS )
			iniObjectInsert( hIni, (char *)pszSection );
		/* update entry */
		if ( iniPropertySeek( hIni, (char *)pszSection, (char *)pszEntry, "" ) == INI_SUCCESS )
		{
			iniObjectSeek( hIni, (char *)pszSection );
            /*
             * Get the correct property to update
             */
		    iniPropertySeek( hIni, (char *)pszSection, (char *)pszEntry, "" );
			iniPropertyUpdate( hIni, (char *)pszEntry, (char *)pszString );
		}
		/* add entry */
		else
		{
			iniObjectSeek( hIni, (char *)pszSection );
			iniPropertyInsert( hIni, (char *)pszEntry, (char *)pszString );
		}

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

BOOL INSTAPI SQLWritePrivateProfileStringW(LPCWSTR lpszSection,
                                         LPCWSTR lpszEntry,
                                         LPCWSTR lpszString,
                                         LPCWSTR lpszFilename)
{
	char *sect;
	char *entry;
	char *string;
	char *file;
	BOOL ret;

	sect = lpszSection ? _single_string_alloc_and_copy( lpszSection ) : (char*)NULL;
	entry = lpszEntry ? _single_string_alloc_and_copy( lpszEntry ) : (char*)NULL;
	string = lpszString ? _single_string_alloc_and_copy( lpszString ) : (char*)NULL;
	file = lpszFilename ? _single_string_alloc_and_copy( lpszFilename ) : (char*)NULL;

	ret = SQLWritePrivateProfileString( sect, entry, string, file );

	if ( sect )
		free( sect );
	if ( entry )
		free( entry );
	if ( string )
		free( string );
	if ( file )
		free( file );

	return ret;
}
