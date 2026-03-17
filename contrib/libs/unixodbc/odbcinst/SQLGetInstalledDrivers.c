/**************************************************
 * SQLGetInstalledDrivers
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

BOOL SQLGetInstalledDrivers(	LPSTR	pszBuf,
								WORD	nBufMax,
								WORD	*pnBufOut )
{
	HINI	hIni;
	WORD	nBufPos		= 0;
	WORD	nToCopySize	= 0;
	char	szObjectName[INI_MAX_OBJECT_NAME+1];
    char    szIniName[ ODBC_FILENAME_MAX * 2 + 1 ];
	char 	b1[ ODBC_FILENAME_MAX + 1 ], b2[ ODBC_FILENAME_MAX + 1 ];

    inst_logClear();

#ifdef VMS
    sprintf( szIniName, "%s:%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#else
    sprintf( szIniName, "%s/%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#endif

#ifdef __OS2__
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', TRUE, 1L ) != INI_SUCCESS )
#else
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', TRUE ) != INI_SUCCESS )
#endif
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_COMPONENT_NOT_FOUND, "" );
		return FALSE;
	}
	
	memset( pszBuf, '\0', nBufMax );

	iniObjectFirst( hIni );
	while ( iniObjectEOL( hIni ) == FALSE )
	{
		iniObject( hIni, szObjectName );
		if ( strcmp( szObjectName, "ODBC" ) == 0 )
		{
		    iniObjectNext( hIni );
		    continue;
		}

		if ( (strlen( szObjectName )+1) > (nBufMax - nBufPos) )
		{
			nToCopySize = nBufMax - nBufPos;
			strncpy( &(pszBuf[nBufPos]), szObjectName, nToCopySize );
			nBufPos = nBufMax;
			break;
		}
		else
		{
			strcpy( &(pszBuf[nBufPos]), szObjectName );
			nBufPos += strlen( szObjectName )+1;
		}
		iniObjectNext( hIni );
	}
	iniClose( hIni );

	if ( pnBufOut )
		*pnBufOut = nBufPos;
	
	return TRUE;
}


BOOL INSTAPI SQLGetInstalledDriversW  (LPWSTR      lpszBuf,
                                      WORD       cbBufMax,
                                      WORD      * pcbBufOut)
{
	char *path;
	BOOL ret;

    inst_logClear();

	path = calloc( cbBufMax, 1 );

	ret = SQLGetInstalledDrivers( path, cbBufMax, pcbBufOut );

	if ( ret ) 
	{
		_multi_string_copy_to_wide( lpszBuf, path, cbBufMax );
	}

	free( path );

	return ret;
}
