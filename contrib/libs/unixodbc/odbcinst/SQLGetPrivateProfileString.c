/****************************************************
 * SQLGetPrivateProfileString
 *
 * Mostly used with odbc.ini files but can be used for odbcinst.ini
 *
 * IF pszFileName[0] == '/' THEN
 *		use pszFileName
 * ELSE
 *		use _odbcinst_ConfigModeINI() to get the complete file name for the current mode.
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
#include <time.h>
#include <odbcinstext.h>

#ifdef ENABLE_INI_CACHING

#ifdef HAVE_LIBPTH

#include <pth.h>

static pth_mutex_t mutex_ini = PTH_MUTEX_INIT;
static int pth_init_called = 0;

static int mutex_entry( pth_mutex_t *mutex )
{
    if ( !pth_init_called )
    {
        pth_init();
        pth_init_called = 1;
    }
    return pth_mutex_acquire( mutex, 0, NULL );
}

static int mutex_exit( pth_mutex_t *mutex )
{
    return pth_mutex_release( mutex );
}

#elif HAVE_LIBPTHREAD

#include <pthread.h>

static pthread_mutex_t mutex_ini = PTHREAD_MUTEX_INITIALIZER;

static int mutex_entry( pthread_mutex_t *mutex )
{
    return pthread_mutex_lock( mutex );
}

static int mutex_exit( pthread_mutex_t *mutex )
{
    return pthread_mutex_unlock( mutex );
}

#elif HAVE_LIBTHREAD

#include <thread.h>

static mutex_t mutex_ini;

static int mutex_entry( mutex_t *mutex )
{
    return mutex_lock( mutex );
}

static int mutex_exit( mutex_t *mutex )
{
    return mutex_unlock( mutex );
}

#else

#define mutex_entry(x)
#define mutex_exit(x)

#endif

static struct ini_cache *ini_cache_head = NULL;

static int _check_ini_cache( int *ret,
                     LPCSTR  pszSection,
                     LPCSTR  pszEntry,
                     LPCSTR  pszDefault,
                     LPSTR   pRetBuffer,
                     int     nRetBuffer,
                     LPCSTR  pszFileName )
{
    struct ini_cache *ini_cache = ini_cache_head, *prev = NULL;
    UWORD config_mode;
    long tstamp = time( NULL );

    if ( pszSection == NULL || pszEntry == NULL )
    {
        return 0;
    }

    config_mode = __get_config_mode();

    /*
     * look for expired entries, remove one each call
     */

    for ( prev = NULL, ini_cache = ini_cache_head; ini_cache; ini_cache = ini_cache -> next )
    {
        if ( ini_cache -> timestamp < tstamp )
        {
            if ( prev )
            {
                prev -> next = ini_cache -> next;
            }
            else
            {
                ini_cache_head = ini_cache -> next;
            }

            if ( ini_cache -> fname )
                free( ini_cache -> fname );

            if ( ini_cache -> section )
                free( ini_cache -> section );

            if ( ini_cache -> entry )
                free( ini_cache -> entry );

            if ( ini_cache -> value )
                free( ini_cache -> value );

            if ( ini_cache -> default_value )
                free( ini_cache -> default_value );

            free( ini_cache );

            break;
        }

        prev = ini_cache;
    }

    for ( ini_cache = ini_cache_head; ini_cache; ini_cache = ini_cache -> next )
    {
        if ( !pszFileName && ini_cache -> fname )
            continue;
        if ( pszFileName && !ini_cache -> fname )
            continue;
        if ( pszFileName && ini_cache -> fname && strcmp( pszFileName, ini_cache -> fname ))
            continue;

        if ( ini_cache -> config_mode != config_mode )
            continue;

        if ( !pszSection && ini_cache -> section )
            continue;
        if ( pszSection && !ini_cache -> section )
            continue;
        if ( pszSection && ini_cache -> section && strcmp( pszSection, ini_cache -> section ))
            continue;

        if ( !pszEntry && ini_cache -> entry )
            continue;
        if ( pszEntry && !ini_cache -> entry )
            continue;
        if ( pszEntry && ini_cache -> entry && strcmp( pszEntry, ini_cache -> entry ))
            continue;

        if ( !pszDefault && ini_cache -> default_value )
            continue;
        if ( pszDefault && !ini_cache -> default_value )
            continue;
        if ( pszDefault && ini_cache -> default_value && strcmp( pszDefault, ini_cache -> default_value ))
            continue;

        if ( !pRetBuffer && ini_cache -> value )
            continue;
        if ( pRetBuffer && !ini_cache -> value )
            continue;

        if ( nRetBuffer < ini_cache -> buffer_size )
            continue;

        if ( pRetBuffer )
        {
            if ( ini_cache -> value ) {
                if ( nRetBuffer < strlen( ini_cache -> value )) {
                    strncpy( pRetBuffer, ini_cache -> value, nRetBuffer );
                    pRetBuffer[ nRetBuffer - 1 ] = '\0';
                }
                else {
                    strcpy( pRetBuffer, ini_cache -> value );
                }
            }

            *ret = ini_cache -> ret_value;
            return 1;
        }
    }

    return 0;
}

static void _save_ini_cache( int ret,
                    LPCSTR  pszSection,
                    LPCSTR  pszEntry,
                    LPCSTR  pszDefault,
                    LPSTR   pRetBuffer,
                    int     nRetBuffer,
                    LPCSTR  pszFileName )
{
    struct ini_cache *ini_cache;
    UWORD config_mode;
    long tstamp = time( NULL ) + 20;    /* expiry every 20 seconds */

    ini_cache = calloc( sizeof( struct ini_cache ), 1 );
    if ( !ini_cache )
    {
        return;
    }

    if ( pszFileName )
        ini_cache -> fname = strdup( pszFileName );

    if ( pszSection )
        ini_cache -> section = strdup( pszSection );

    if ( pszEntry )
        ini_cache -> entry = strdup( pszEntry );

    if ( pRetBuffer && ret >= 0 )
        ini_cache -> value = strdup( pRetBuffer );

    if ( pszDefault )
        ini_cache -> default_value = strdup( pszDefault );

    ini_cache -> buffer_size = nRetBuffer;
    ini_cache -> ret_value = ret;

    config_mode = __get_config_mode();
    ini_cache -> config_mode = config_mode;

    ini_cache -> timestamp = tstamp;

    ini_cache -> next = ini_cache_head;
    ini_cache_head = ini_cache;
}

static void _clear_ini_cache( void ) 
{
    struct ini_cache *ini_cache = ini_cache_head;

    while (( ini_cache = ini_cache_head ) != NULL  )
    {
        ini_cache_head = ini_cache -> next;

        if ( ini_cache -> fname )
            free( ini_cache -> fname );

        if ( ini_cache -> section )
            free( ini_cache -> section );

        if ( ini_cache -> entry )
            free( ini_cache -> entry );

        if ( ini_cache -> value )
            free( ini_cache -> value );

        if ( ini_cache -> default_value )
            free( ini_cache -> default_value );

        free( ini_cache );
    }
}

/*
 * wrappers to provide thread safety
 */

static int check_ini_cache( int *ret,
                     LPCSTR  pszSection,
                     LPCSTR  pszEntry,
                     LPCSTR  pszDefault,
                     LPSTR   pRetBuffer,
                     int     nRetBuffer,
                     LPCSTR  pszFileName )
{
	int rval;

	mutex_entry( &mutex_ini );

	rval = _check_ini_cache( ret, pszSection, pszEntry, pszDefault,
			pRetBuffer, nRetBuffer, pszFileName );

	mutex_exit( &mutex_ini );

	return rval;
}

static void save_ini_cache( int ret,
                    LPCSTR  pszSection,
                    LPCSTR  pszEntry,
                    LPCSTR  pszDefault,
                    LPSTR   pRetBuffer,
                    int     nRetBuffer,
                    LPCSTR  pszFileName )
{
	int cval;

	mutex_entry( &mutex_ini );

	/*
	 * check its not been inserted since the last check
	 */

	if ( !_check_ini_cache( &cval, pszSection, pszEntry, pszDefault,
			pRetBuffer, nRetBuffer, pszFileName )) {

		_save_ini_cache( ret, pszSection, pszEntry, pszDefault,
			pRetBuffer, nRetBuffer, pszFileName );
	}

	mutex_exit( &mutex_ini );
}

void __clear_ini_cache( void ) 
{
	mutex_entry( &mutex_ini );

    _clear_ini_cache();

	mutex_exit( &mutex_ini );
}

#else

static int check_ini_cache( int *ret,
                     LPCSTR  pszSection,
                     LPCSTR  pszEntry,
                     LPCSTR  pszDefault,
                     LPSTR   pRetBuffer,
                     int     nRetBuffer,
                     LPCSTR  pszFileName )
{
    return 0;
}

static void save_ini_cache( int ret,
                    LPCSTR  pszSection,
                    LPCSTR  pszEntry,
                    LPCSTR  pszDefault,
                    LPSTR   pRetBuffer,
                    int     nRetBuffer,
                    LPCSTR  pszFileName )
{
}

void __clear_ini_cache( void ) 
{
}

#endif

int __SQLGetPrivateProfileStringNL( LPCSTR  pszSection,
                                LPCSTR  pszEntry,
                                LPCSTR  pszDefault,
                                LPSTR   pRetBuffer,
                                int     nRetBuffer,
                                LPCSTR  pszFileName
                              )
{
    HINI    hIni;
    int     nBufPos         = 0;
    char    szValue[INI_MAX_PROPERTY_VALUE+1];
    char    szFileName[ODBC_FILENAME_MAX+1];
    UWORD   nConfigMode;
    int     ini_done = 0;
    int     ret;

    inst_logClear();

    if ( check_ini_cache( &ret, pszSection, pszEntry, pszDefault, pRetBuffer, nRetBuffer, pszFileName ))
    {
        return ret;
    }

    /* SANITY CHECKS */
    if ( pRetBuffer == NULL || nRetBuffer < 2 )
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
        return -1;
    }
    if ( pszSection != NULL && pszEntry != NULL && pszDefault == NULL )
    {
        inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "need default value - try empty string" );
        return -1;
    }

    *pRetBuffer = '\0';

    /*****************************************************
     * SOME MS CODE (ie some drivers) MAY USE THIS FUNCTION TO GET ODBCINST INFO SO...
     *****************************************************/
    if ( pszFileName != NULL )
    {
        if ( strstr( pszFileName, "odbcinst" ) || strstr( pszFileName, "ODBCINST" ) )
        {
            ret = _SQLGetInstalledDrivers(  pszSection, pszEntry, pszDefault, pRetBuffer, nRetBuffer );

            if ( ret == -1 )
            {
                /* try to use any default provided */
                if ( pRetBuffer && nRetBuffer > 0 )
                {
                    if ( pszDefault )
                    {
                        strncpy( pRetBuffer, pszDefault, nRetBuffer );
                        pRetBuffer[ nRetBuffer - 1 ] = '\0';
                    }
                }
            }
            else
            {
                save_ini_cache( ret, pszSection, pszEntry, pszDefault, pRetBuffer, nRetBuffer, pszFileName );
            }

            return ret;
        }
        else if ( !*pszFileName )
        {
            return 0; /* Asking for empty filename returns nothing, not even default */
        }
    }

    /*****************************************************
     * GATHER ALL RELEVANT DSN INFORMATION INTO AN hIni
     *****************************************************/
    if ( pszFileName != 0 && pszFileName[0] == '/' )
    {
#ifdef __OS2__
        if ( iniOpen( &hIni, (char*)pszFileName, "#;", '[', ']', '=', TRUE, 1L )
             != INI_SUCCESS )
#else
        if ( iniOpen( &hIni, (char*)pszFileName, "#;", '[', ']', '=', TRUE )
             != INI_SUCCESS )
#endif
        {
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL,
                             ODBC_ERROR_COMPONENT_NOT_FOUND, "" );
            return -1;
        }
    }
    else
    {
        nConfigMode     = __get_config_mode();
        nBufPos         = 0;
        szFileName[0]   = '\0';

        switch ( nConfigMode )
        {
        case ODBC_BOTH_DSN:
            if ( _odbcinst_UserINI( szFileName, TRUE ))
            {
#ifdef __OS2__
                if ( iniOpen( &hIni, (char*) szFileName, "#;", '[', ']', '=', TRUE, 1L )
                     == INI_SUCCESS )
#else
                if ( iniOpen( &hIni, (char*) szFileName, "#;", '[', ']', '=', TRUE )
                     == INI_SUCCESS )
#endif
                {
                    ini_done = 1;
                }
            }
            _odbcinst_SystemINI( szFileName, TRUE );
            if ( !ini_done )
            {
#ifdef __OS2__
                if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE, 1L )
                     != INI_SUCCESS )
#else
                if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE )
                     != INI_SUCCESS )
#endif
                {
                    inst_logPushMsg( __FILE__, __FILE__, __LINE__,
                                     LOG_CRITICAL, ODBC_ERROR_COMPONENT_NOT_FOUND, "" );
                    return -1;
                }
            }
            else
            {
                iniAppend( hIni, szFileName );
            }
            break;

        case ODBC_USER_DSN:
            _odbcinst_UserINI( szFileName, TRUE );
#ifdef __OS2__
            if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE, 1L )
                 != INI_SUCCESS )
#else
            if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE )
                 != INI_SUCCESS )
#endif
            {
                inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL,
                                 ODBC_ERROR_COMPONENT_NOT_FOUND, "" );
                return -1;
            }
            break;

        case ODBC_SYSTEM_DSN:
            _odbcinst_SystemINI( szFileName, TRUE );
#ifdef __OS2__
            if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE, 1L )
                 != INI_SUCCESS )
#else
            if ( iniOpen( &hIni, szFileName, "#;", '[', ']', '=', TRUE )
                 != INI_SUCCESS )
#endif
            {
                inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL,
                                 ODBC_ERROR_COMPONENT_NOT_FOUND, "" );
                return -1;
            }
            break;

        default:
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL,
                             ODBC_ERROR_GENERAL_ERR, "Invalid Config Mode" );
            return -1;
        }
    }

    /*****************************************************
     * EXTRACT SECTIONS
     *****************************************************/
    if ( pszSection == NULL )
    {
        _odbcinst_GetSections( hIni, pRetBuffer, nRetBuffer, &nBufPos );

        if (nBufPos > 0)
            ret = _multi_string_length(pRetBuffer);
        else
            ret = 0;  /* Indicate not found */
    }
    /*****************************************************
     * EXTRACT ENTRIES
     *****************************************************/
    else if ( pszEntry == NULL )
    {
        _odbcinst_GetEntries( hIni, pszSection, pRetBuffer, nRetBuffer, &nBufPos );
        if (nBufPos > 0)
            ret = _multi_string_length(pRetBuffer);
        else
            ret = 0;  /* Indicate not found */
    }
    /*****************************************************
     * EXTRACT AN ENTRY
     *****************************************************/
    else
    {
        if ( pszSection == NULL || pszEntry == NULL || pszDefault == NULL )
        {
            inst_logPushMsg( __FILE__, __FILE__, __LINE__, LOG_CRITICAL, ODBC_ERROR_GENERAL_ERR, "" );
            return -1;
        }

        /* TRY TO GET THE ONE ITEM MATCHING Section & Entry */
        if ( iniPropertySeek( hIni, (char *)pszSection, (char *)pszEntry, "" ) != INI_SUCCESS )
        {
            /*
             * (NG) this seems to be ignoring the length of pRetBuffer !!!
             */
            /* strncpy( pRetBuffer, pszDefault, INI_MAX_PROPERTY_VALUE ); */
            if ( pRetBuffer && nRetBuffer > 0 && pszDefault )
            {
                strncpy( pRetBuffer, pszDefault, nRetBuffer );
                pRetBuffer[ nRetBuffer - 1 ] = '\0';
            }
        }
        else
        {
            iniValue( hIni, szValue );
	        if ( pRetBuffer ) 
	        {
	            strncpy( pRetBuffer, szValue, nRetBuffer );
	            pRetBuffer[ nRetBuffer - 1 ] = '\0';
	        }
            nBufPos = strlen( szValue );
        }
        
        ret = strlen( pRetBuffer );
    }

    iniClose( hIni );

    save_ini_cache( ret, pszSection, pszEntry, pszDefault, pRetBuffer, nRetBuffer, pszFileName );

    return ret;
}

int SQLGetPrivateProfileString( LPCSTR  pszSection,
                                LPCSTR  pszEntry,
                                LPCSTR  pszDefault,
                                LPSTR   pRetBuffer,
                                int     nRetBuffer,
                                LPCSTR  pszFileName
                              )
{
int ret;

    __lock_config_mode();
    ret = __SQLGetPrivateProfileStringNL( pszSection, pszEntry, pszDefault, pRetBuffer, nRetBuffer, pszFileName );
    __unlock_config_mode();

    return ret;
}

int  INSTAPI SQLGetPrivateProfileStringW( LPCWSTR lpszSection,
                                        LPCWSTR lpszEntry,
                                        LPCWSTR lpszDefault,
                                        LPWSTR  lpszRetBuffer,
                                        int    cbRetBuffer,
                                        LPCWSTR lpszFilename)
{
	int ret;
	char *sect;
	char *entry;
	char *def;
	char *buf;
	char *name;

    inst_logClear();

	sect = lpszSection ? _single_string_alloc_and_copy( lpszSection ) : (char*)NULL;
	entry = lpszEntry ? _single_string_alloc_and_copy( lpszEntry ) : (char*)NULL;
	def = lpszDefault ? _single_string_alloc_and_copy( lpszDefault ) : (char*)NULL;
	name = lpszFilename ? _single_string_alloc_and_copy( lpszFilename ) : (char*)NULL;

	if ( lpszRetBuffer ) 
	{
		if ( cbRetBuffer > 0 )
		{
			buf = calloc( cbRetBuffer + 1, 1 );
		}
		else
		{
			buf = NULL;
		}
	}
	else
	{
		buf = NULL;
	}

	ret = buf ? SQLGetPrivateProfileString( sect, entry, def, buf, cbRetBuffer, name ) : -1;

	if ( sect )
		free( sect );
	if ( entry )
		free( entry );
	if ( def )
		free( def );
	if ( name )
		free( name );

	if ( ret > 0 )
	{
		if ( buf && lpszRetBuffer )
		{
            if ( !lpszSection || !lpszEntry )
                ret = _multi_string_copy_to_wide( lpszRetBuffer, buf, ret );
            else
                ret = _single_copy_to_wide( lpszRetBuffer, buf, ret );
		}
	}

	if ( buf )
	{
		free( buf );
	}

	return ret;
}
