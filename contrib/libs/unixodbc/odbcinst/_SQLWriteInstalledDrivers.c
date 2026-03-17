/**************************************************
 * _SQLWriteInstalledDrivers
 *
 * Added to allow ODBC Config programs and the iODBC
 * driver manager to access system information without
 * having to worry about where it is... just like accessing
 * Data Source information. So no surprise... its just
 * like SQLWritePrivateProfileString()!
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

BOOL _SQLWriteInstalledDrivers(
								LPCSTR	pszSection,
								LPCSTR	pszEntry,
								LPCSTR	pszString )
{
	HINI	hIni;
    char    szIniName[ ODBC_FILENAME_MAX * 2 + 1 ];
	char	b1[ ODBC_FILENAME_MAX + 1 ], b2[ ODBC_FILENAME_MAX + 1 ];

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

	/* OK */

#ifdef VMS
    sprintf( szIniName, "%s:%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#else
    sprintf( szIniName, "%s/%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#endif

#ifdef __OS2__
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', TRUE, 1L  ) != INI_SUCCESS )
#else
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', TRUE  ) != INI_SUCCESS )
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
			iniPropertyDelete( hIni );
	}
	else
	{
		/* add section */
		if ( iniObjectSeek( hIni, (char *)pszSection ) != INI_SUCCESS )
		{
			iniObjectInsert( hIni, (char *)pszSection );
		}

		/* update entry */
		if ( iniPropertySeek( hIni, (char *)pszSection, (char *)pszEntry, "" ) == INI_SUCCESS )
		{
/*			iniObjectSeek( hIni, (char *)pszSection ); */
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

	return TRUE;
}




