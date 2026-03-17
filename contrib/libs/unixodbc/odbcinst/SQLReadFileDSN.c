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

static void GetEntries( HINI    hIni,
                        LPCSTR  pszSection,
                        LPSTR   pRetBuffer,
                        int     nRetBuffer
                      )
{
    char    szPropertyName[INI_MAX_PROPERTY_NAME+1];
    char    szValueName[INI_MAX_PROPERTY_NAME+1];

    /* COLLECT ALL ENTRIES FOR THE GIVEN SECTION */
    iniObjectSeek( hIni, (char *)pszSection );
    iniPropertyFirst( hIni );

    *pRetBuffer = '\0';

    while ( iniPropertyEOL( hIni ) != TRUE )
    {
        iniProperty( hIni, szPropertyName );
        iniValue( hIni, szValueName );

        if ( strlen( pRetBuffer ) + strlen( szPropertyName ) < nRetBuffer )
        {
            strcat( pRetBuffer, szPropertyName );
            if ( strlen( pRetBuffer ) + 1 < nRetBuffer )
            {
                strcat( pRetBuffer, "=" );
                if ( strlen( pRetBuffer ) + strlen( szValueName ) < nRetBuffer )
                {
                    strcat( pRetBuffer, szValueName );
                    if ( strlen( pRetBuffer ) + 1 < nRetBuffer )
                    {
                        strcat( pRetBuffer, ";" );
                    }
                }
            }
        }

        iniPropertyNext( hIni );
    }
}

static void GetSections(    HINI    hIni,
                            LPSTR   pRetBuffer,
                            int     nRetBuffer
                       )
{
    char    szObjectName[INI_MAX_OBJECT_NAME+1];

    *pRetBuffer = '\0';

    /* JUST COLLECT SECTION NAMES */
    iniObjectFirst( hIni );
    while ( iniObjectEOL( hIni ) != TRUE )
    {
        iniObject( hIni, szObjectName );

        if ( strcasecmp( szObjectName, "ODBC Data Sources" ) != 0 )
        {
            if ( strlen( pRetBuffer ) + strlen( szObjectName ) + 1 < nRetBuffer )
            {
                strcat( pRetBuffer, szObjectName );
                strcat( pRetBuffer, ";" );
            }
        }
        iniObjectNext( hIni );
    }
}

BOOL SQLReadFileDSN(            LPCSTR  pszFileName,
                                LPCSTR  pszAppName,
                                LPCSTR  pszKeyName,
                                LPSTR   pszString,
                                WORD    nString,
                                WORD    *pnString )
{
    HINI    hIni;
    char    szValue[INI_MAX_PROPERTY_VALUE+1];
    char    szFileName[ODBC_FILENAME_MAX+2];

    inst_logClear();

    /* SANITY CHECKS */
    if ( pszString == NULL || nString < 1  )
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_BUFF_LEN, "" );
        return FALSE;
    }
    if ( pszFileName == NULL && pszAppName == NULL && pszKeyName == NULL )
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
        return FALSE;
    }
    if ( pszAppName == NULL && pszKeyName != NULL )
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_REQUEST_TYPE, "" );
        return FALSE;
    }
    if ( pszFileName && strlen( pszFileName ) > ODBC_FILENAME_MAX ) {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_PATH, "" );
        return FALSE;
    }

    *pszString = '\0';

    /*****************************************************
     * GATHER ALL RELEVANT DSN INFORMATION INTO AN hIni
     *****************************************************/
    if ( pszFileName && pszFileName[0] == '/' )
    {
        strcpy( szFileName, pszFileName );
        if ( strlen( szFileName ) < 4 || strcmp( szFileName + strlen( szFileName ) - 4, ".dsn" ))
        {
            strcat( szFileName, ".dsn" );
        }

/* on OS/2 the file DSN is a text file */
#ifdef __OS2__
        if ( iniOpen( &hIni, (char*)szFileName, "#;", '[', ']', '=', TRUE, 0L )
             != INI_SUCCESS )
#else
        if ( iniOpen( &hIni, (char*)szFileName, "#;", '[', ']', '=', TRUE )
             != INI_SUCCESS )
#endif
        {
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL,
                             ODBC_ERROR_INVALID_PATH, "" );

            return FALSE;
        }
    }
    else if ( pszFileName )
    {
        char szPath[ODBC_FILENAME_MAX+1];
        *szPath = '\0';
        _odbcinst_FileINI( szPath );
#ifdef HAVE_SNPRINTF
        snprintf( szFileName, sizeof( szFileName ), "%s/%s", szPath, pszFileName );
#else
        sprintf( szFileName, "%s/%s", szPath, pszFileName );
#endif

        if ( strlen( szFileName ) < 4 || strcmp( szFileName + strlen( szFileName ) - 4, ".dsn" ))
        {
            strcat( szFileName, ".dsn" );
        }

/* on OS/2 the file DSN is a text file */
#ifdef __OS2__
        if ( iniOpen( &hIni, (char*) szFileName, "#;", '[', ']', '=', TRUE, 0L )
             != INI_SUCCESS )
#else
        if ( iniOpen( &hIni, (char*) szFileName, "#;", '[', ']', '=', TRUE )
             != INI_SUCCESS )
#endif
        {
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL,
                             ODBC_ERROR_INVALID_PATH, "" );

            return FALSE;
        }
    }

    if ( pszAppName == NULL && pszKeyName == NULL )
    {
        GetSections( hIni, pszString, nString );
    }
    else if ( pszAppName != NULL && pszKeyName == NULL )
    {
        GetEntries( hIni, pszAppName, pszString, nString );
    }
    else
    {
        /* TRY TO GET THE ONE ITEM MATCHING Section & Entry */
        if ( iniPropertySeek( hIni, (char *)pszAppName, (char *)pszKeyName, "" ) != INI_SUCCESS )
        {
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL,
                             ODBC_ERROR_REQUEST_FAILED, "" );

            if ( pszFileName )
            {
                iniClose( hIni );
            }

            return FALSE;
        }
        else
        {
            iniValue( hIni, szValue );
            strncpy( pszString, szValue, nString );
            pszString[ nString - 1 ] = '\0';
        }
    }

    if ( pszFileName )
    {
        iniClose( hIni );
    }

    if ( pnString )
    {
        *pnString = strlen( pszString );
    }

    return TRUE;
}

BOOL INSTAPI  SQLReadFileDSNW(LPCWSTR  lpszFileName,
                              LPCWSTR  lpszAppName,
                              LPCWSTR  lpszKeyName,
                              LPWSTR   lpszString,
                              WORD     cbString,
                              WORD    *pcbString)
{
    char *file;
    char *app;
    char *key;
    char *str;
    WORD len;
    BOOL ret;

    inst_logClear();

    file = lpszFileName ? _single_string_alloc_and_copy( lpszFileName ) : (char*)NULL;
    app = lpszAppName ? _single_string_alloc_and_copy( lpszAppName ) : (char*)NULL;
    key = lpszKeyName ? _single_string_alloc_and_copy( lpszKeyName ) : (char*)NULL;

    if ( lpszString )
    {
        if ( cbString > 0 )
        {
            str = calloc( cbString + 1, 1 );
        }
        else
        {
            str = NULL;
        }
    }
    else
    {
        str = NULL;
    }

    ret = SQLReadFileDSN( file, app, key, str, cbString, &len );

    if ( ret )
    {
        if ( str && lpszString )
        {
            len = _single_copy_to_wide( lpszString, str, len + 1 );
        }
    }

    if ( file )
        free( file );
    if ( app )
        free( app );
    if ( key )
        free( key );
    if ( str )
        free( str );

    if ( pcbString )
        *pcbString = len;

    return ret;
}
