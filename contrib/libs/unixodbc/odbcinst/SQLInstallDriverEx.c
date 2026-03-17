/*************************************************
 * SQLInstallDriverEx
 *
 * pnUsageCount	UsageCount is incremented and decremented
 *				only in this lib. This is done whenever
 *				a request is made to install or remove
 *				a driver.
 *				This differs slightly from the MS spec.
 *				see UsageCount entries in odbcinst.ini
 *
 * pszPathOut	This lacks some smarts. I will pass pszPathIn
 *				back here or, if pszPathIn=NULL, I will default
 *				to /usr/lib
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

BOOL SQLInstallDriverEx(		LPCSTR	pszDriver,
								LPCSTR	pszPathIn,
								LPSTR	pszPathOut,
								WORD	nPathOutMax,
								WORD	*pnPathOut,
								WORD	nRequest,
								LPDWORD	pnUsageCount )
{
	HINI	hIni;
	char	szObjectName[INI_MAX_OBJECT_NAME+1];
	char	szNameValue[INI_MAX_PROPERTY_NAME+INI_MAX_PROPERTY_VALUE+3];
	char	szPropertyName[INI_MAX_PROPERTY_NAME+1];
	char	szValue[INI_MAX_PROPERTY_VALUE+1];
    char    szIniName[ ODBC_FILENAME_MAX * 2 + 1 ];

	BOOL	bInsertUsageCount;
	int		nElement;
	int		nUsageCount 			= 0;				/* SHOULD GET THIS FROM SOMEWHERE ? */
	char	b1[ ODBC_FILENAME_MAX + 1 ], b2[ ODBC_FILENAME_MAX + 1 ];


    inst_logClear();

	/* SANITY CHECKS */
	if ( pszDriver == NULL || pszPathOut == NULL )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
		return FALSE;
	}
	if ( nRequest != ODBC_INSTALL_INQUIRY && nRequest != ODBC_INSTALL_COMPLETE )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_REQUEST_TYPE, "" );
		return FALSE;
	}

	memset( pszPathOut, '\0', nPathOutMax );

    if ( pszPathIn )
    {
#ifdef VMS
        snprintf( szIniName, sizeof(szIniName), "%s:%s", pszPathIn, odbcinst_system_file_name( b2 ) );
#else
        snprintf( szIniName, sizeof(szIniName), "%s/%s", pszPathIn, odbcinst_system_file_name( b2 ) );
#endif
    }
    else
    {
#ifdef VMS
        sprintf( szIniName, "%s:%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#else
        sprintf( szIniName, "%s/%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ) );
#endif
    }

	/* PROCESS ODBC INST INI FILE */

#ifdef __OS2__
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', TRUE, 1L ) != INI_SUCCESS )
#else
	if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', TRUE ) != INI_SUCCESS )
#endif
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_COMPONENT_NOT_FOUND, "" );
		return FALSE;
	}

	if ( iniElement( (char *)pszDriver, '\0', '\0', 0, szObjectName, INI_MAX_OBJECT_NAME ) != INI_SUCCESS )
	{
		iniClose( hIni );
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_KEYWORD_VALUE, "" );
		return FALSE;
	}

	/* LETS GET ITS FILE USAGE VALUE (if any) */
	if ( iniPropertySeek( hIni, szObjectName, "UsageCount", "" ) == INI_SUCCESS )
	{
		iniValue( hIni, szValue );
        nUsageCount = atoi( szValue );
	}

	/* DOES THE OBJECT ALREADY EXIST? (also ensures that we have correct current object) */	
	if ( iniObjectSeek( hIni, szObjectName ) == INI_SUCCESS )
	{
        if ( nUsageCount == 0 )
			nUsageCount = 1;

		if ( nRequest == ODBC_INSTALL_COMPLETE )
		{
			iniObjectDelete( hIni );
		}
	}

	/* LETS ADD THE SECTION AND ENTRY */
	nUsageCount++;
	if ( nRequest == ODBC_INSTALL_COMPLETE )
	{
		bInsertUsageCount = TRUE;
		iniObjectInsert( hIni, szObjectName );
		for (	nElement=1;
				iniElement( (char *)pszDriver, '\0', '\0', nElement, szNameValue, INI_MAX_PROPERTY_NAME+INI_MAX_PROPERTY_VALUE+3 ) == INI_SUCCESS;
				nElement++ )
		{
			iniElement( szNameValue, '=', '\0', 0, szPropertyName, INI_MAX_PROPERTY_NAME );
			iniElementEOL( szNameValue, '=', '\0', 1, szValue, INI_MAX_PROPERTY_VALUE );
			if ( szPropertyName[0] != '\0' )
			{
				/* OVERRIDE ANY USAGE COUNT CHANGES */
				if ( strcasecmp( szPropertyName, "UsageCount" ) == 0 )
				{
                    bInsertUsageCount = FALSE;
					sprintf( szValue, "%d", nUsageCount );
				}
				iniPropertyInsert( hIni, szPropertyName, szValue );
			}
			else
			{
				iniClose( hIni );
				inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_KEYWORD_VALUE, "" );
				return FALSE;
			}
		} /* for */

		if ( bInsertUsageCount )
		{
			/* LETS INSERT USAGE COUNT */
			sprintf( szValue, "%d", nUsageCount );
			iniPropertyInsert( hIni, "UsageCount",  szValue );
		}

		if ( iniCommit( hIni ) != INI_SUCCESS )
		{
			inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_PATH, "" );
			iniClose( hIni );
			return FALSE;
		}
	}
	iniClose( hIni );

	/* OK, SO WHATS LEFT? */
	if ( pszPathIn == NULL )
    {
        if ( pszPathOut )
        {
            if ( strlen( odbcinst_system_file_path( b1 )) < nPathOutMax )
            {
                strcpy( pszPathOut, odbcinst_system_file_path( b1 ));
            }
            else
            {
                strncpy( pszPathOut, odbcinst_system_file_path( b1 ), nPathOutMax );
                pszPathOut[ nPathOutMax - 1 ] = '\0';
            }
        }
    }
	else
    {
        if ( pszPathOut && nPathOutMax > 0 )
        {
            if ( strlen( pszPathIn ) < nPathOutMax )
            {
                strcpy( pszPathOut, pszPathIn );
            }
            else
            {
                strncpy( pszPathOut, pszPathIn, nPathOutMax );
                pszPathOut[ nPathOutMax - 1 ] = '\0';
            }
        }
    }

	if ( pnPathOut != NULL )
    {
        if (  pszPathIn == NULL )
        {
            *pnPathOut = strlen( odbcinst_system_file_path( b1 ));
        }
        else
        {
            *pnPathOut = strlen( pszPathIn );
        }
    }
	
	if ( pnUsageCount != NULL )
    {
		*pnUsageCount = nUsageCount;
    }

	return TRUE;
}

BOOL INSTAPI SQLInstallDriverExW(LPCWSTR lpszDriver,
                             LPCWSTR       lpszPathIn,
                             LPWSTR    lpszPathOut,
                             WORD      cbPathOutMax,
                             WORD     *pcbPathOut,
                             WORD       fRequest,
                             LPDWORD    lpdwUsageCount)
{
	char *drv;
	char *pth;
	char *pout;
	WORD len;
	BOOL ret;

    inst_logClear();

	drv = lpszDriver ? _multi_string_alloc_and_copy( lpszDriver ) : (char*)NULL;
	pth = lpszPathIn ? _single_string_alloc_and_copy( lpszPathIn ) : (char*)NULL;

	if ( lpszPathOut ) 
	{
		if ( cbPathOutMax > 0 )
		{
			pout = calloc( cbPathOutMax + 1, 1 );
		}
		else
		{
			pout = NULL;
		}
	}
	else
	{
		pout = NULL;
	}

	ret = pout ? SQLInstallDriverEx( drv, pth, pout, cbPathOutMax, &len, fRequest, lpdwUsageCount ) : FALSE;

	if ( ret )
	{
		if ( pout && lpszPathOut )
		{
			_single_copy_to_wide( lpszPathOut, pout, len + 1 );
		}
	}

	if ( pcbPathOut )
	{
		*pcbPathOut = len;
	}

	if ( drv )
		free( drv );
	if ( pth )
		free( pth );
	if ( pout )
		free( pout );

	return ret;
}
