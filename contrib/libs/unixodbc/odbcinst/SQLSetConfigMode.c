/**************************************************
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 * Nick Gorham      - nick@lurcher.org
 **************************************************/

#include <config.h>
#include <stdlib.h>
#include <odbcinstext.h>

/*
 * This avoids all sorts of problems with using putenv, we need to check
 * that drivers can see this as well though....
 * It has to mess with threads and locking to prevent the calls in connect where
 * its getting the lib info from messing what ini file is being read
 */

static int __config_mode = ODBC_BOTH_DSN;

#ifdef HAVE_LIBPTH

#include <pth.h>

static pth_mutex_t mutex_config = PTH_MUTEX_INIT;
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
                
static pthread_mutex_t mutex_config = PTHREAD_MUTEX_INITIALIZER;

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

static mutex_t mutex_config;

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

void __lock_config_mode( void )
{
    mutex_entry( &mutex_config );
}

void __unlock_config_mode( void )
{
    mutex_exit( &mutex_config );
}

void __set_config_mode( int mode )
{
    __config_mode = mode;
}

int __get_config_mode( void )
{
    char *p;

    /* 
     * if the environment var is set then it overrides the flag
     */

	p  = getenv( "ODBCSEARCH" );
	if ( p )
	{
		if ( strcmp( p, "ODBC_SYSTEM_DSN" ) == 0 )
		{
            __config_mode = ODBC_SYSTEM_DSN;
		}
		else if ( strcmp( p, "ODBC_USER_DSN" ) == 0 )
		{
            __config_mode = ODBC_USER_DSN;
		}
		else if ( strcmp( p, "ODBC_BOTH_DSN" ) == 0 )
		{
            __config_mode = ODBC_BOTH_DSN;
		}
	}

    return __config_mode;
}

BOOL SQLSetConfigMode( UWORD nConfigMode )
{
    inst_logClear();

    __lock_config_mode();
    __set_config_mode( nConfigMode );
    __unlock_config_mode();

    return TRUE;
}
