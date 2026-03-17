/**************************************************
 * SQLManageDataSources
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

/*! 
 * \brief   Get the short name of the UI plugin.
 * 
 *          The short name is the file name without path or file extension.
 *
 *          We silently prepend "lib" here as well.
 *
 * \param   pszName     Place to put short name. Should be FILENAME_MAX bytes. 
 * \param   pszUI       Our generic window handle.
 * 
 * \return  char*       pszName returned for convenience.
 */
char *_getUIPluginName( char *pszName, char *pszUI )
{
    *pszName = '\0';

    /* is it being provided by caller? */
    if ( pszUI && *pszUI )
    {
        sprintf( pszName, "lib%s", pszUI );
        return pszName;
    }

    /* is it being provided by env var? */
    {
        char *pEnvVar = getenv( "ODBCINSTUI" );
        if ( pEnvVar )
        {
            sprintf( pszName, "lib%s", pEnvVar );
            return pszName;
        }
    }

    /* is it being provided by odbcinst.ini? */
    {
        char sz[FILENAME_MAX];
        *sz='\0';
        SQLGetPrivateProfileString( "ODBC", "ODBCINSTUI", "", sz, FILENAME_MAX, "odbcinst.ini" );
        if ( *sz )
        {
            sprintf( pszName, "lib%s", sz );
            return pszName;
        }
    }

    /* default to qt4 */
    strcpy( pszName, ODBCINSTPLUGIN );

    return pszName;
}

/*! 
 * \brief   Append the file extension used by the OS for plugins.
 * 
 *          We use SHLIBEXT which is picked up at configure/build time.
 *
 * \param   pszNameAndExtension   Output. Needs to be FILENAME_MAX bytes.
 * \param   pszName               Input.
 * 
 * \return  char*   pszNameAndExtension returned for convenience.
 */
char *_appendUIPluginExtension( char *pszNameAndExtension, char *pszName )
{
    if ( strlen( SHLIBEXT ) > 0 )
        sprintf( pszNameAndExtension, "%s%s", pszName, SHLIBEXT );
    else
        sprintf( pszNameAndExtension, "%s.so", pszName );

    return pszName;
}

/*! 
 * \brief   Prepends the path used for the plugins.
 * 
 *          We use DEFLIB_PATH and if it is not available...
 *          path may not get prepended.
 *
 * \param   pszPathAndName    Output. Needs to be FILENAME_MAX bytes.
 * \param   pszName           Input.
 * 
 * \return  char*   pszPathAndName is returned for convenience.
 */
char *_prependUIPluginPath( char *pszPathAndName, char *pszName )
{
    if ( strlen( DEFLIB_PATH ) > 0 )
        sprintf( pszPathAndName, "%s/%s", DEFLIB_PATH, pszName );
    else
        sprintf( pszPathAndName, "%s", pszName );

    return pszPathAndName;
}

/*! 
 * \brief   UI to manage most ODBC system information.
 * 
 *          This calls into the UI plugin library to do our work for us. The caller can provide
 *          the name (base name) of the library or let us determine which library to use.
 *          See \sa _getUIPluginName for details on how the choice is made.
 *          
 * \param   hWnd    Input. Parent window handle. This is HWND as per the ODBC
 *                  specification but in unixODBC we use a generic window
 *                  handle. Caller must cast a HODBCINSTWND to HWND at call. 
 * 
 * \return  BOOL
 *
 * \sa      ODBCINSTWND
 */
BOOL SQLManageDataSources( HWND hWnd )
{
    HODBCINSTWND    hODBCInstWnd    = (HODBCINSTWND)hWnd;
    char            szName[FILENAME_MAX];
    char            szNameAndExtension[FILENAME_MAX];
    char            szPathAndName[FILENAME_MAX];
	void *          hDLL;
	BOOL	        (*pSQLManageDataSources)(HWND);

    inst_logClear();

    /* ODBC specification states that hWnd is mandatory. */
	if ( !hWnd )
	{
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_HWND, "No hWnd" );
		return FALSE;
	}

    /* initialize libtool */
    if ( lt_dlinit() )
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "lt_dlinit() failed" );
		return FALSE;
    }
#ifdef MODULEDIR
    lt_dlsetsearchpath(MODULEDIR);
#endif

    /* get plugin name */
    _appendUIPluginExtension( szNameAndExtension, _getUIPluginName( szName, hODBCInstWnd->szUI ) );

    /* lets try loading the plugin using an implicit path */
    hDLL = lt_dlopen( szNameAndExtension );
    if ( hDLL )
    {
        /* change the name (SQLManageDataSources to ODBCManageDataSources) to prevent us from calling ourself */
        pSQLManageDataSources = (BOOL (*)(HWND))lt_dlsym( hDLL, "ODBCManageDataSources" );
        if ( pSQLManageDataSources ) {
            BOOL ret;
            ret = pSQLManageDataSources( ( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ) );

            lt_dlclose( hDLL );
            return ret;
        }
        else
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, (char*)lt_dlerror() );

        lt_dlclose( hDLL );
    }
    else
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_WARNING, ODBC_ERROR_GENERAL_ERR, (char*)lt_dlerror() );
        /* try with explicit path */
        _prependUIPluginPath( szPathAndName, szNameAndExtension );
        hDLL = lt_dlopen( szPathAndName );
        if ( hDLL )
        {
            /* change the name (SQLManageDataSources to ODBCManageDataSources) to prevent us from calling ourself   */
            /* its only safe to use hWnd if szUI was specified by the caller                                        */
            pSQLManageDataSources = (BOOL (*)(HWND))lt_dlsym( hDLL, "ODBCManageDataSources" );
            if ( pSQLManageDataSources ) {
                BOOL ret;

                ret = pSQLManageDataSources( ( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ) );

                lt_dlclose( hDLL );
                return ret;
            }
            else
                inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, (char*)lt_dlerror() );

            lt_dlclose( hDLL );
        }
        else
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, (char*)lt_dlerror() );
    }

    /* report failure to caller */
    inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "Failed to load/use a UI plugin." );

    return FALSE;
}


