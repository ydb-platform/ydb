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

BOOL SQLWriteFileDSN(			LPCSTR	pszFileName,
								LPCSTR	pszAppName,
								LPCSTR	pszKeyName,
								LPCSTR	pszString )
{
	HINI	hIni;
	char	szFileName[ODBC_FILENAME_MAX+7];

    if ( pszFileName == NULL ) 
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_PATH, "" );
		return FALSE;
    }
	else if ( pszFileName[0] == '/' )
	{
		strncpy( szFileName, pszFileName, sizeof(szFileName) - 5 );
	}
	else
	{	
		char szPath[ODBC_FILENAME_MAX+1];
		*szPath = '\0';
		_odbcinst_FileINI( szPath );
		snprintf( szFileName, sizeof(szFileName) - 5, "%s/%s", szPath, pszFileName );
	}

    if ( strlen( szFileName ) < 4 || strcmp( szFileName + strlen( szFileName ) - 4, ".dsn" ))
    {
        strcat( szFileName, ".dsn" );
    }

#ifdef __OS2__
	if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE, 0L ) != INI_SUCCESS )
#else
	if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE ) != INI_SUCCESS )
#endif
	{
       	inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_PATH, "" );
		return FALSE;
	}

	/* delete section */
	if ( pszString == NULL && pszKeyName == NULL )
	{
		if ( iniObjectSeek( hIni, (char *)pszAppName ) == INI_SUCCESS )
        {
			iniObjectDelete( hIni );
        }
	}
	/* delete entry */
	else if	( pszString == NULL )
	{
		if ( iniPropertySeek( hIni, (char *)pszAppName, (char *)pszKeyName, "" ) == INI_SUCCESS )
        {
			iniPropertyDelete( hIni );
        }
	}
	else
	{
		/* add section */
		if ( iniObjectSeek( hIni, (char *)pszAppName ) != INI_SUCCESS )
        {
			iniObjectInsert( hIni, (char *)pszAppName );
        }
		/* update entry */
		if ( iniPropertySeek( hIni, (char *)pszAppName, (char *)pszKeyName, "" ) == INI_SUCCESS )
		{
			iniObjectSeek( hIni, (char *)pszAppName );
			iniPropertyUpdate( hIni, (char *)pszKeyName, (char *)pszString );
		}
		/* add entry */
		else
		{
			iniObjectSeek( hIni, (char *)pszAppName );
			iniPropertyInsert( hIni, (char *)pszKeyName, (char *)pszString );
		}
	}

	if ( iniCommit( hIni ) != INI_SUCCESS )
	{
		iniClose( hIni );
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_REQUEST_FAILED, "" );
		return FALSE;
	}

	iniClose( hIni );

	return TRUE;
}

BOOL INSTAPI SQLWriteFileDSNW(LPCWSTR  lpszFileName,
                              LPCWSTR  lpszAppName,
                              LPCWSTR  lpszKeyName,
                              LPCWSTR  lpszString)
{
	BOOL ret;
	char *file;
	char *app;
	char *key;
	char *str;

	file = lpszFileName ? _single_string_alloc_and_copy( lpszFileName ) : (char*)NULL;
	app = lpszAppName ? _single_string_alloc_and_copy( lpszAppName ) : (char*)NULL;
	key = lpszKeyName ? _single_string_alloc_and_copy( lpszKeyName ) : (char*)NULL;
	str = lpszString ? _single_string_alloc_and_copy( lpszString ) : (char*)NULL;

	ret = app ? SQLWriteFileDSN( file, app, key, str ) : FALSE;

	if ( file )
		free( file );
	if ( app )
		free( app );
	if ( key )
		free( key );
	if ( str )
		free( str );

	return ret;
}
