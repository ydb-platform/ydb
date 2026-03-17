/**************************************************
 * SQLCreateDataSource
 *
 * This is a 100% UI so simply pass it on to odbcinst's UI
 * shadow share.
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

/*
 * Take a wide string consisting of null terminated sections, and copy to a ASCII version
 */

char* _multi_string_alloc_and_copy( LPCWSTR in )
{
    char *chr;
    int len = 0;

    if ( !in )
    {
        return NULL;
    }

    while ( in[ len ] != 0 || in[ len + 1 ] != 0 )
    {
        len ++;
    }

    chr = malloc( len + 2 );
    if ( !chr )
        return NULL;

    len = 0;
    while ( in[ len ] != 0 || in[ len + 1 ] != 0 )
    {
        chr[ len ] = 0xFF & in[ len ];
        len ++;
    }
    chr[ len ++ ] = '\0';
    chr[ len ++ ] = '\0';

    return chr;
}

#ifdef WITH_UTF8_INI

char* _single_string_alloc_and_copy( LPCWSTR in )
{
    char *chr;
    int len = 0, ulen = 0;

    if ( !in )
    {
        return NULL;
    }

    while ( in[ len ] != 0 )
    {
        if ( in[ len ] < 0x80 ) {
            ulen ++;
        }
        else if ( in[ len ] < 0x800 ) {
            ulen += 2;
        }
        else {
            ulen += 3;
        }
        len ++;
    }

    chr = malloc( ulen + 1 );
    if ( !chr )
        return NULL;

    len = 0;
    ulen = 0;
    while ( in[ len ] != 0 )
    {
        if ( in[ len ] < 0x80 ) {
            chr[ ulen ] = in[ len ];
            len ++;
            ulen ++;
        }
        else if ( in[ len ] < 0x800 ) {
            chr[ ulen ++ ] = ( 0xC0 | ( in[ len ]  >> 6 ));
            chr[ ulen ] = ( 0x80 | ( in[ len ] & 0x3F ));
            len ++;
            ulen ++;
        }
        else {
            chr[ ulen ++ ] = ( 0xE0 | ( in[ len ] >> 12 ));
            chr[ ulen ++ ] = ( 0x80 | ( in[ len ]  >> 6 & 0x3F ));
            chr[ ulen ] = ( 0x80 | ( in[ len ] & 0x3F ));
            len ++;
            ulen ++;
        }
    }
    chr[ ulen ++ ] = '\0';

    return chr;
}

#else

char* _single_string_alloc_and_copy( LPCWSTR in )
{
    char *chr;
    int len = 0;

    if ( !in )
    {
        return NULL;
    }

    while ( in[ len ] != 0 )
    {
        len ++;
    }

    chr = malloc( len + 1 );
    if ( !chr )
        return NULL;

    len = 0;
    while ( in[ len ] != 0 )
    {
        chr[ len ] = 0xFF & in[ len ];
        len ++;
    }
    chr[ len ++ ] = '\0';

    return chr;
}

#endif

SQLWCHAR* _multi_string_alloc_and_expand( LPCSTR in )
{
    SQLWCHAR *chr;
    int len = 0;

    if ( !in )
    {
        return NULL;
    }
    
    while ( in[ len ] != 0 || in[ len + 1 ] != 0 )
    {
        len ++;
    }

    chr = malloc(sizeof( SQLWCHAR ) * ( len + 2 ));
    if ( !chr )
        return NULL;

    len = 0;
    while ( in[ len ] != 0 || in[ len + 1 ] != 0 )
    {
        chr[ len ] = in[ len ];
        len ++;
    }
    chr[ len ++ ] = 0;
    chr[ len ++ ] = 0;

    return chr;
}

SQLWCHAR* _single_string_alloc_and_expand( LPCSTR in )
{
    SQLWCHAR *chr;
    int len = 0;

    if ( !in )
    {
        return NULL;
    }

    while ( in[ len ] != 0 )
    {
        len ++;
    }

    chr = malloc( sizeof( SQLWCHAR ) * ( len + 1 ));
    if ( !chr )
        return NULL;

    len = 0;
    while ( in[ len ] != 0 )
    {
        chr[ len ] = in[ len ];
        len ++;
    }
    chr[ len ++ ] = 0;

    return chr;
}

void _single_string_copy_to_wide( SQLWCHAR *out, LPCSTR in, int len )
{
    while ( len > 0 && *in )
    {
        *out = *in;
        out++;
        in++;
        len --;
    }
    *out = 0;
}

#ifdef WITH_UTF8_INI

int _single_copy_to_wide( SQLWCHAR *out, LPCSTR in, int len )
{
    int clen = 0;

    while ( len >= 0 )
    {
        if ((*in & 0x80) == 0x00 ) {
            *out = *in;

            in ++;
            len --;
            if ( *out == 0 ) {
                break;
            }
        }
        else if ((*in & 0xE0) == 0xC0) {
            *out = (*in++ & 0x3F);
            *out = *out << 6;
            *out |= (*in & 0x7F);

            in ++;
            len -= 2;
        }
        else if ((*in & 0xF0) == 0xE0) {
            *out = *in++ & 0x1F;
            *out = *out << 12;
            *out |= ((*in++ & 0x7F) << 6);
            *out |= (*in & 0x3F);

            in ++;
            len -= 3;
        }
        out ++;
        clen ++;
    }

    return clen;
}

#else

int _single_copy_to_wide( SQLWCHAR *out, LPCSTR in, int len )
{
    int clen;

    clen = len;

    while ( len >= 0 )
    {
        *out = *in;
        out++;
        in++;
        len --;
    }

    return clen;
}

#endif

void _single_copy_from_wide( SQLCHAR *out, LPCWSTR in, int len )
{
    while ( len >= 0 )
    {
        *out = *in;
        out++;
        in++;
        len --;
    }
}

#ifdef WITH_UTF8_INI

int _multi_string_copy_to_wide( SQLWCHAR *out, LPCSTR in, int len )
{
    int clen = 0;

    while ( len > 0 && ( in[ 0 ] || in[ 1 ] ))
    {
        if ((*in & 0x80) == 0x00 ) {
            *out = *in;

            in ++;
            len --;
        }
        else if ((*in & 0xE0) == 0xC0) {
            *out = (*in++ & 0x3F);
            *out = *out << 6;
            *out |= (*in & 0x7F);

            in ++;
            len -= 2;
        }
        else if ((*in & 0xF0) == 0xE0) {
            *out = *in++ & 0x1F;
            *out = *out << 12;
            *out |= ((*in++ & 0x7F) << 6);
            *out |= (*in & 0x3F);

            in ++;
            len -= 3;
        }
        out ++;
        clen ++;
    }

    *out++ = 0;
    *out++ = 0;

    return clen;
}

#else

int _multi_string_copy_to_wide( SQLWCHAR *out, LPCSTR in, int len )
{
    int clen = 0;

    while ( len > 0 && ( in[ 0 ] || in[ 1 ] ))
    {
        *out = *in;
        out++;
        in++;
        len --;
        clen ++;
    }
    *out++ = 0;
    *out++ = 0;

    return clen;
}

#endif

int _multi_string_length( LPCSTR in )
{
    LPCSTR ch;

    if ( !in )
        return 0;

    for ( ch = in ; !(*ch == 0 && *(ch + 1) == 0) ; ch ++ );

    /* The convention seems to be to exclude the very last '\0' character from
     * the count, so that is what we do here.
     */
    return ch - in + 1;
}


/*! 
 * \brief   Invokes a UI (a wizard) to walk User through creating a DSN.
 * 
 * \param   hWnd    Input. Parent window handle. This is HWND as per the ODBC
 *                  specification but in unixODBC we use a generic window
 *                  handle. Caller must cast a HODBCINSTWND to HWND at call. 
 * \param   pszDS   Input. Data Source Name. This can be a NULL pointer.
 * 
 * \return  BOOL
 *
 * \sa      ODBCINSTWND
 */
BOOL SQLCreateDataSource( HWND hWnd, LPCSTR pszDS )
{
    HODBCINSTWND  hODBCInstWnd = (HODBCINSTWND)hWnd;
    char          szName[FILENAME_MAX];
    char          szNameAndExtension[FILENAME_MAX];
    char          szPathAndName[FILENAME_MAX];
    void *        hDLL;
    BOOL          (*pSQLCreateDataSource)(HWND, LPCSTR);

    inst_logClear();

    /* ODBC specification states that hWnd is mandatory. */
    if ( !hWnd )
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_HWND, "" );
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
        /* change the name, as it avoids it finding it in the calling lib */
        pSQLCreateDataSource = (BOOL (*)(HWND, LPCSTR))lt_dlsym( hDLL, "ODBCCreateDataSource" );

        if ( pSQLCreateDataSource ) {
            BOOL ret;

            ret = pSQLCreateDataSource( ( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ), pszDS );

            lt_dlclose( hDLL );
            return ret;
        }
        else
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, (char*)lt_dlerror() );

        lt_dlclose( hDLL );
    }
    else
    {
        /* try with explicit path */
        _prependUIPluginPath( szPathAndName, szNameAndExtension );
        hDLL = lt_dlopen( szPathAndName );
        if ( hDLL )
        {
            /* change the name, as it avoids linker finding it in the calling lib */
            pSQLCreateDataSource = (BOOL (*)(HWND,LPCSTR))lt_dlsym( hDLL, "ODBCCreateDataSource" );
            if ( pSQLCreateDataSource ) {
                BOOL ret;

                ret = pSQLCreateDataSource( ( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ), pszDS );

                lt_dlclose( hDLL );
                return ret;
            }
            else
                inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, (char*)lt_dlerror() );

            lt_dlclose( hDLL );
        }
    }

    /* report failure to caller */
    inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );

    return FALSE;
}

/*! 
 * \brief   A wide char version of \sa SQLCreateDataSource.
 * 
 * \sa      SQLCreateDataSource
 */
BOOL INSTAPI SQLCreateDataSourceW( HWND hwndParent, LPCWSTR lpszDSN )
{
    HODBCINSTWND  hODBCInstWnd = (HODBCINSTWND)hwndParent;
    char          szName[FILENAME_MAX];
    char          szNameAndExtension[FILENAME_MAX];
    char          szPathAndName[FILENAME_MAX];
    void *        hDLL;
    BOOL          (*pSQLCreateDataSource)(HWND, LPCWSTR);

    inst_logClear();

    /* ODBC specification states that hWnd is mandatory. */
    if ( !hwndParent )
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_INVALID_HWND, "" );
        return FALSE;
    }

    /* initialize libtool */
    if ( lt_dlinit() )
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "lt_dlinit() failed" );
        return FALSE;
    }

    /* get plugin name */
    _appendUIPluginExtension( szNameAndExtension, _getUIPluginName( szName, hODBCInstWnd->szUI ) );

    /* lets try loading the plugin using an implicit path */
    hDLL = lt_dlopen( szNameAndExtension );
    if ( hDLL )
    {
        /* change the name, as it avoids it finding it in the calling lib */
        pSQLCreateDataSource = (BOOL (*)(HWND, LPCWSTR))lt_dlsym( hDLL, "ODBCCreateDataSourceW" );
        if ( pSQLCreateDataSource ) {
            BOOL ret;

            ret = pSQLCreateDataSource( ( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ), lpszDSN );

            lt_dlclose( hDLL );
            return ret;
        }
        else
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, (char*)lt_dlerror() );

        lt_dlclose( hDLL );
    }
    else
    {
        /* try with explicit path */
        _prependUIPluginPath( szPathAndName, szNameAndExtension );
        hDLL = lt_dlopen( szPathAndName );
        if ( hDLL )
        {
            /* change the name, as it avoids linker finding it in the calling lib */
            pSQLCreateDataSource = (BOOL (*)(HWND,LPCWSTR))lt_dlsym( hDLL, "ODBCCreateDataSourceW" );
            if ( pSQLCreateDataSource ) {
                BOOL ret;

                ret = pSQLCreateDataSource( ( *(hODBCInstWnd->szUI) ? hODBCInstWnd->hWnd : NULL ), lpszDSN );

                lt_dlclose( hDLL );
                return ret;
            }
            else
                inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, (char*)lt_dlerror() );

            lt_dlclose( hDLL );
        }
    }

    /* report failure to caller */
    inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );

    return FALSE;
}
