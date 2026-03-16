/*************************************************
 * SQLInstallDriverManager
 *
 * I return the default dir for core components.. but
 * thats it.
 * This may differ slightly from the spec.
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

BOOL SQLInstallDriverManager(	LPSTR	pszPath,
								WORD	nPathMax,
								WORD	*pnPathOut )
{
    char  szIniName[ INI_MAX_OBJECT_NAME + 1 ];
	char  b1[ ODBC_FILENAME_MAX + 1 ];

    inst_logClear();

	/* SANITY CHECKS */
	if ( pszPath == NULL || nPathMax < 2 )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return 0;
	}

    sprintf( szIniName, "%s", odbcinst_system_file_path( b1 ) );

	/* DO SOMETHING */
	strncpy( pszPath, szIniName, nPathMax );
	if ( pnPathOut != NULL )
		*pnPathOut = strlen( pszPath );

	return TRUE;
}

BOOL INSTAPI SQLInstallDriverManagerW (LPWSTR      lpszPath,
                                      WORD       cbPathMax,
                                      WORD      * pcbPathOut)
{
	char *path;
	BOOL ret;

    inst_logClear();

	path = calloc( cbPathMax, 1 );

	ret = SQLInstallDriverManager( path, cbPathMax, pcbPathOut );

	if ( ret ) 
	{
		_single_string_copy_to_wide( lpszPath, path, cbPathMax );
	}

	free( path );

	return ret;
}
