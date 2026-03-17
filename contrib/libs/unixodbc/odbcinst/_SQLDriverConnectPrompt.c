#include <config.h>

#ifdef UNIXODBC_SOURCE
#include <ltdl.h>
#endif

#include <odbcinstext.h>

BOOL _SQLDriverConnectPrompt( 
	HWND hwnd, 
	SQLCHAR *dsn, 
	SQLSMALLINT len_dsn )
{
    HODBCINSTWND  hODBCInstWnd = (HODBCINSTWND)hwnd;
    char          szName[FILENAME_MAX];
    char          szNameAndExtension[FILENAME_MAX];
    char          szPathAndName[FILENAME_MAX];
    void *        hDLL;
    BOOL          (*pODBCDriverConnectPrompt)(HWND, SQLCHAR *, SQLSMALLINT );
    BOOL          ret;

    /* initialize libtool */
    if ( lt_dlinit() )
    {
        return FALSE;
    }

    /* get plugin name */

	if ( hODBCInstWnd ) 
	{
    	_appendUIPluginExtension( szNameAndExtension, _getUIPluginName( szName, hODBCInstWnd->szUI ) );
	}
	else 
	{
    	_appendUIPluginExtension( szNameAndExtension, _getUIPluginName( szName, NULL ) );
	}

    /* lets try loading the plugin using an implicit path */
    hDLL = lt_dlopen( szNameAndExtension );
    if ( hDLL )
    {
        /* change the name, as it avoids it finding it in the calling lib */
        pODBCDriverConnectPrompt = (BOOL (*)( HWND, SQLCHAR *, SQLSMALLINT ))lt_dlsym( hDLL, "ODBCDriverConnectPrompt" );
        if ( pODBCDriverConnectPrompt ) 
		{
			if ( hODBCInstWnd ) 
			{
            	ret = pODBCDriverConnectPrompt(( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ), dsn, len_dsn );
			}
			else 
			{
            	ret = pODBCDriverConnectPrompt( NULL, dsn, len_dsn );
			}
		}
		else 
		{
			ret = FALSE;
		}

        lt_dlclose( hDLL );

        return ret;
    }
    else
    {
        /* try with explicit path */
        _prependUIPluginPath( szPathAndName, szNameAndExtension );
        hDLL = lt_dlopen( szPathAndName );
        if ( hDLL )
        {
            /* change the name, as it avoids linker finding it in the calling lib */
        	pODBCDriverConnectPrompt = (BOOL (*)(HWND, SQLCHAR *, SQLSMALLINT ))lt_dlsym( hDLL, "ODBCDriverConnectPrompt" );
        	if ( pODBCDriverConnectPrompt ) 
			{
				if ( hODBCInstWnd ) 
				{
            		ret = pODBCDriverConnectPrompt(( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ), dsn, len_dsn );
				}
				else 
				{
            		ret = pODBCDriverConnectPrompt( NULL, dsn, len_dsn );
				}
			}
			else 
			{
				ret = FALSE;
			}

            lt_dlclose( hDLL );
            return ret;
        }
    }

    return FALSE;
}

BOOL _SQLDriverConnectPromptW( 
	HWND hwnd, 
	SQLWCHAR *dsn, 
	SQLSMALLINT len_dsn )
{
    HODBCINSTWND  hODBCInstWnd = (HODBCINSTWND)hwnd;
    char          szName[FILENAME_MAX];
    char          szNameAndExtension[FILENAME_MAX];
    char          szPathAndName[FILENAME_MAX];
    void *        hDLL;
    BOOL          (*pODBCDriverConnectPromptW)(HWND, SQLWCHAR *, SQLSMALLINT );
    BOOL          ret;

    /* initialize libtool */
    if ( lt_dlinit() )
    {
        return FALSE;
    }

    /* get plugin name */

	if ( hODBCInstWnd ) 
	{
    	_appendUIPluginExtension( szNameAndExtension, _getUIPluginName( szName, hODBCInstWnd->szUI ) );
	}
	else 
	{
    	_appendUIPluginExtension( szNameAndExtension, _getUIPluginName( szName, NULL ) );
	}

    /* lets try loading the plugin using an implicit path */
    hDLL = lt_dlopen( szNameAndExtension );
    if ( hDLL )
    {
        /* change the name, as it avoids it finding it in the calling lib */
        pODBCDriverConnectPromptW = (BOOL (*)( HWND, SQLWCHAR *, SQLSMALLINT ))lt_dlsym( hDLL, "ODBCDriverConnectPromptW" );
        if ( pODBCDriverConnectPromptW ) 
		{
			if ( hODBCInstWnd ) 
			{
            	ret = pODBCDriverConnectPromptW(( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ), dsn, len_dsn );
			}
			else 
			{
            	ret = pODBCDriverConnectPromptW( NULL, dsn, len_dsn );
			}
		}
		else 
		{
			ret = FALSE;
		}

        lt_dlclose( hDLL );

        return ret;
    }
    else
    {
        /* try with explicit path */
        _prependUIPluginPath( szPathAndName, szNameAndExtension );
        hDLL = lt_dlopen( szPathAndName );
        if ( hDLL )
        {
            /* change the name, as it avoids linker finding it in the calling lib */
        	pODBCDriverConnectPromptW = (BOOL (*)(HWND, SQLWCHAR *, SQLSMALLINT ))lt_dlsym( hDLL, "ODBCDriverConnectPromptW" );
        	if ( pODBCDriverConnectPromptW ) 
			{
				if ( hODBCInstWnd ) 
				{
            		ret = pODBCDriverConnectPromptW(( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ), dsn, len_dsn );
				}
				else 
				{
            		ret = pODBCDriverConnectPromptW( NULL, dsn, len_dsn );
				}
			}
			else 
			{
				ret = FALSE;
			}

            lt_dlclose( hDLL );

            return ret;
        }
    }

    return FALSE;
}
