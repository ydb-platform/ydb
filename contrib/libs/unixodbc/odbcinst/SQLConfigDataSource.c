/**************************************************
 * SQLConfigDataSource
 *
 * Determine the DriverSetup file name and then try to pass
 * the work along to its ConfigDSN().
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

#ifdef UNIXODBC_SOURCE
#include <ltdl.h>
#endif

#include <odbcinstext.h>

static BOOL SQLConfigDataSourceWide(	HWND	hWnd,
								WORD	nRequest,
								LPCSTR	pszDriver,				/* USER FRIENDLY NAME (not file name) */
								LPCSTR	pszAttributes,
			   					LPCWSTR pszDriverW,
								LPCWSTR pszAttributesW	)
{
	BOOL	(*pFunc)( HWND, WORD, LPCSTR, LPCSTR	);
	BOOL	(*pFuncW)( HWND, WORD, LPCWSTR, LPCWSTR	);
	BOOL	nReturn;
	void 	*hDLL	= FALSE;
	HINI	hIni;
	char	szDriverSetup[INI_MAX_PROPERTY_VALUE+1];
    char    szIniName[ ODBC_FILENAME_MAX * 2 + 3 ];
	char	b1[ ODBC_FILENAME_MAX + 1 ], b2[ ODBC_FILENAME_MAX + 1 ];
    int     config_mode;

	/* SANITY CHECKS */
	if ( pszDriver == NULL || pszAttributes == NULL )
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return FALSE;
	}

	if ( pszDriver[0] == '\0' )
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return FALSE;
	}
	switch ( nRequest )
	{
	case ODBC_ADD_DSN:
	case ODBC_CONFIG_DSN:
	case ODBC_REMOVE_DSN:
	case ODBC_ADD_SYS_DSN:
	case ODBC_CONFIG_SYS_DSN:
	case ODBC_REMOVE_SYS_DSN:
	case ODBC_REMOVE_DEFAULT_DSN:
		break;
	default:
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_REQUEST_TYPE, "" );
		return FALSE;
	}

#ifdef VMS
    sprintf( szIniName, "%s:%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#else
    sprintf( szIniName, "%s/%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#endif

    __lock_config_mode();
    config_mode = __get_config_mode();

	/* OK */
#ifdef __OS2__
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', TRUE, 1L ) != INI_SUCCESS )
#else
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', TRUE ) != INI_SUCCESS )
#endif
	{
		inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
        __set_config_mode( config_mode );
        __unlock_config_mode();
		return FALSE;
	}

    /*
     * initialize libtool
     */

    lt_dlinit();
#ifdef MODULEDIR
    lt_dlsetsearchpath(MODULEDIR);
#endif

#ifdef PLATFORM64
	if ( iniPropertySeek( hIni, (char *)pszDriver, "Setup64", "" ) == INI_SUCCESS || 
				iniPropertySeek( hIni, (char *)pszDriver, "Setup", "" ) == INI_SUCCESS )
#else
	if ( iniPropertySeek( hIni, (char *)pszDriver, "Setup", "" ) == INI_SUCCESS )
#endif
	{
   		iniValue( hIni, szDriverSetup );

		iniClose( hIni );

		if ( szDriverSetup[ 0 ] == '\0' ) 
		{
			char szError[ 512 ];
			sprintf( szError, "Could not find Setup property for (%.400s) in system information", pszDriver );
			inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, szError );
            __set_config_mode( config_mode );
            __unlock_config_mode();
			return FALSE;
		}

		nReturn = FALSE;
		if ( (hDLL = lt_dlopen( szDriverSetup ))  )
		{
			pFunc = (BOOL (*)(HWND, WORD, LPCSTR, LPCSTR )) lt_dlsym( hDLL, "ConfigDSN" );
			pFuncW = (BOOL (*)(HWND, WORD, LPCWSTR, LPCWSTR )) lt_dlsym( hDLL, "ConfigDSNW" );
			if ( pFunc )
            {
                /*
                 * set the mode
                 */
	            switch ( nRequest )
                {
                    case ODBC_ADD_DSN:
                    case ODBC_CONFIG_DSN:
                    case ODBC_REMOVE_DSN:
                    case ODBC_REMOVE_DEFAULT_DSN:
                      __set_config_mode( ODBC_USER_DSN );
                      break;

                    case ODBC_ADD_SYS_DSN:
                      __set_config_mode( ODBC_SYSTEM_DSN );
                      nRequest = ODBC_ADD_DSN;
                      break;

                    case ODBC_CONFIG_SYS_DSN:
                      __set_config_mode( ODBC_SYSTEM_DSN );
                      nRequest = ODBC_CONFIG_DSN;
                      break;

                    case ODBC_REMOVE_SYS_DSN:
                      __set_config_mode( ODBC_SYSTEM_DSN );
                      nRequest = ODBC_REMOVE_DSN;
                      break;
                }
				nReturn = pFunc( hWnd, nRequest, pszDriver, pszAttributes );
            }
			else if ( pFuncW ) 
			{
               	/*
               	* set the mode
               	*/
	           	switch ( nRequest )
               	{
                   	case ODBC_ADD_DSN:
                   	case ODBC_CONFIG_DSN:
                   	case ODBC_REMOVE_DSN:
                   	case ODBC_REMOVE_DEFAULT_DSN:
                     	__set_config_mode( ODBC_USER_DSN );
                     	break;

                   	case ODBC_ADD_SYS_DSN:
                     	__set_config_mode( ODBC_SYSTEM_DSN );
                     	nRequest = ODBC_ADD_DSN;
                     	break;

                   	case ODBC_CONFIG_SYS_DSN:
                     	__set_config_mode( ODBC_SYSTEM_DSN );
                     	nRequest = ODBC_CONFIG_DSN;
                     	break;

                   	case ODBC_REMOVE_SYS_DSN:
                     	__set_config_mode( ODBC_SYSTEM_DSN );
                     	nRequest = ODBC_REMOVE_DSN;
                     	break;
               	}
				nReturn = pFuncW( hWnd, nRequest, pszDriverW, pszAttributesW );
			}
			else 
			{
				inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
			}
		}
		else
			inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );

        __set_config_mode( config_mode );
        __unlock_config_mode();

		return nReturn;

	}

	inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
	iniClose( hIni );

    __set_config_mode( config_mode );
    __unlock_config_mode();

	return FALSE;
}

BOOL INSTAPI SQLConfigDataSourceW     (HWND       hwndParent,
                                      WORD       fRequest,
                                      LPCWSTR     lpszDriver,
                                      LPCWSTR     lpszAttributes)
{
	char *drv, *attr;
	BOOL ret;

    inst_logClear();

	drv = _single_string_alloc_and_copy( lpszDriver );
	attr = _multi_string_alloc_and_copy( lpszAttributes );

	ret = SQLConfigDataSourceWide( hwndParent, fRequest, drv, attr, lpszDriver, lpszAttributes );

	free( drv );
	free( attr );

	return ret;
}

BOOL INSTAPI SQLConfigDataSource      (HWND       hwndParent,
                                      WORD       fRequest,
                                      LPCSTR     lpszDriver,
                                      LPCSTR     lpszAttributes)
{
	SQLWCHAR *drv, *attr;
	BOOL ret;

    inst_logClear();

	drv = _single_string_alloc_and_expand( lpszDriver );
	attr = _multi_string_alloc_and_expand( lpszAttributes );

	ret = SQLConfigDataSourceWide( hwndParent, fRequest, lpszDriver, lpszAttributes, drv, attr );

	free( drv );
	free( attr );

	return ret;
}
