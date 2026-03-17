/*********************************************************************
 *
 * This is based on code created by Peter Harvey,
 * (pharvey@codebydesign.com).
 *
 * Modified and extended by Nick Gorham
 * (nick@lurcher.org).
 *
 * Any bugs or problems should be considered the fault of Nick and not
 * Peter.
 *
 * copyright (c) 1999 Nick Gorham
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 **********************************************************************
 *
 * $Id: __handles.c,v 1.13 2009/05/15 15:23:56 lurcher Exp $
 *
 * $Log: __handles.c,v $
 * Revision 1.13  2009/05/15 15:23:56  lurcher
 * Fix pooled connection thread problems
 *
 * Revision 1.12  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.11  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.10  2007/02/28 15:37:49  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.9  2006/05/31 17:35:34  lurcher
 * Add unicode ODBCINST entry points
 *
 * Revision 1.8  2004/09/28 08:44:46  lurcher
 * Fix memory leak in pthread descriptor code
 *
 * Revision 1.7  2004/07/24 17:55:37  lurcher
 * Sync up CVS
 *
 * Revision 1.6  2003/06/04 12:49:45  lurcher
 *
 * Further PID logging tweeks
 *
 * Revision 1.5  2003/06/02 16:51:36  lurcher
 *
 * Add TracePid option
 *
 * Revision 1.4  2002/08/12 16:20:44  lurcher
 *
 * Make it try and find a working iconv set of encodings
 *
 * Revision 1.3  2002/08/12 13:17:52  lurcher
 *
 * Replicate the way the MS DM handles loading of driver libs, and allocating
 * handles in the driver. usage counting in the driver means that dlopen is
 * only called for the first use, and dlclose for the last. AllocHandle for
 * the driver environment is only called for the first time per driver
 * per application environment.
 *
 * Revision 1.2  2002/02/22 10:23:22  lurcher
 *
 * s/Trace File/TraceFile
 *
 * Revision 1.1.1.1  2001/10/17 16:40:07  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.14  2001/06/25 12:55:15  nick
 *
 * Fix threading problem with multiple ENV's
 *
 * Revision 1.13  2001/06/04 15:24:49  nick
 *
 * Add port to MAC OSX and QT3 changes
 *
 * Revision 1.12  2001/05/15 13:33:44  jason
 *
 * Wrapped calls to stats with COLLECT_STATS
 *
 * Revision 1.11  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.10  2001/03/02 14:24:23  nick
 *
 * Fix thread detection for Solaris
 *
 * Revision 1.9  2001/01/04 13:16:25  nick
 *
 * Add support for GNU portable threads and tidy up some UNICODE compile
 * warnings
 *
 * Revision 1.8  2000/12/18 11:51:59  martin
 *
 * stats specific mode to uodbc_open_stats.
 *
 * Revision 1.7  2000/12/18 11:03:58  martin
 *
 * Add support for the collection and retrieval of handle statistics.
 *
 * Revision 1.6  2000/12/17 11:17:22  nick
 *
 * Remove typo
 *
 * Revision 1.5  2000/12/17 11:00:32  nick
 *
 * Add thread safe bits to pooling
 *
 * Revision 1.4  2000/11/29 17:53:59  nick
 *
 * Fix race condition
 *
 * Revision 1.3  2000/10/25 09:39:42  nick
 *
 * Clear handles out, to avoid reuse
 *
 * Revision 1.2  2000/09/08 08:58:17  nick
 *
 * Add SQL_DRIVER_HDESC to SQLGetinfo
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.16  2000/06/29 17:27:52  ngorham
 *
 * Add fast validate option
 *
 * Revision 1.15  2000/06/27 17:34:12  ngorham
 *
 * Fix a problem when the second part of the connect failed a seg fault
 * was generated in the error reporting
 *
 * Revision 1.14  2001/03/28 23:09:57  ngorham
 *
 * Fix logging
 *
 * Revision 1.13  2000/03/11 15:55:47  ngorham
 *
 * A few more changes and bug fixes (see NEWS)
 *
 * Revision 1.12  2000/02/25 00:02:00  ngorham
 *
 * Add a patch to support IBM DB2, and Solaris threads
 *
 * Revision 1.11  2000/02/22 22:14:45  ngorham
 *
 * Added support for solaris threads
 * Added check to overcome bug in PHP4
 * Fixed bug in descriptors and ODBC 3 drivers
 *
 * Revision 1.10  1999/12/11 13:01:57  ngorham
 *
 * Add some fixes to the Postgres driver for long types
 *
 * Revision 1.9  1999/12/01 09:20:07  ngorham
 *
 * Fix some threading problems
 *
 * Revision 1.8  1999/11/13 23:41:01  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.7  1999/11/10 03:51:34  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.6  1999/08/05 18:59:49  ngorham
 *
 * Typo error found by Greg Bentz
 *
 * Revision 1.5  1999/08/03 21:47:39  shandyb
 * Moving to automake: changed files in DriverManager
 *
 * Revision 1.4  1999/07/10 21:10:17  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.3  1999/07/04 21:05:08  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/30 23:56:56  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:09  sShandyb
 * first go at it
 *
 * Revision 1.1.1.1  1999/05/27 18:23:18  pharvey
 * Imported sources
 *
 * Revision 1.3  1999/05/09 23:27:11  nick
 * All the API done now
 *
 * Revision 1.2  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include <ctype.h>
#include "drivermanager.h"
#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
#include "__stats.h"
#include <uodbc_stats.h>
#endif

static char const rcsid[]= "$RCSfile: __handles.c,v $ $Revision: 1.13 $";

/*
 * these are used to enable us to check if a handle is
 * valid without the danger of a seg-vio.
 */

static DMHENV environment_root;
static DMHDBC connection_root;
static DMHSTMT statement_root;
static DMHDESC descriptor_root;


/*
 * use just one mutex for all the lists, this avoids any issues
 * with deadlocks, the performance issue should be minimal, if it
 * turns out to be a problem, we can readdress this
 *
 * We also have a mutex to protect the connection pooling code
 *
 * If compiled with thread support the DM allows four different
 * thread strategies:
 *
 * Level 0 - Only the DM internal structures are protected.
 * The driver is assumed to take care of itself
 *
 * Level 1 - The driver is protected down to the statement level.
 * Each statement will be protected, and the same for the connect 
 * level for connect functions. Note that descriptors are considered
 * equal to statements when it comes to thread protection.
 *
 * Level 2 - The driver is protected at the connection level. Only
 * one thread can be in a particular driver at one time.
 *
 * Level 3 - The driver is protected at the env level, only one thing
 * at a time.
 *
 * By default the driver opens connections with lock level 0; drivers
 * are expected to be thread safe now. This can be changed by adding
 * the line
 *
 * Threading = N
 *
 * to the driver entry in odbcinst.ini, where N is the locking level 
 * (0-3)
 * 
 */

#ifdef HAVE_LIBPTH

#include <pth.h>

static pth_mutex_t mutex_lists = PTH_MUTEX_INIT;
static pth_mutex_t mutex_env = PTH_MUTEX_INIT;
static pth_mutex_t mutex_pool = PTH_MUTEX_INIT;
static pth_mutex_t mutex_iconv = PTH_MUTEX_INIT;
static int pth_init_called = 0;

static pth_cond_t cond_pool = PTH_COND_INIT;

static int local_mutex_entry( pth_mutex_t *mutex )
{
    if ( !pth_init_called )
    {
        pth_init();
        pth_init_called = 1;
    }
    return pth_mutex_acquire( mutex, 0, NULL );
}

static int local_mutex_exit( pth_mutex_t *mutex )
{
    return pth_mutex_release( mutex );
}

static int local_cond_timedwait( pth_cond_t *cond, pth_mutex_t *mutex, struct timespec *until )
{
    /* NOTE: timedwait is not present in PTH */
    return pth_cond_await( cond, mutex, 0 );
}

static void local_cond_signal( pth_cond_t *cond )
{
    pth_cond_notify( cond, 0 );
}

#elif HAVE_LIBPTHREAD

#include <pthread.h>

static pthread_mutex_t mutex_lists = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_env = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_pool = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t mutex_iconv = PTHREAD_MUTEX_INITIALIZER;

static pthread_cond_t cond_pool = PTHREAD_COND_INITIALIZER;

static int local_mutex_entry( pthread_mutex_t *mutex )
{
    return pthread_mutex_lock( mutex );
}

static int local_mutex_exit( pthread_mutex_t *mutex )
{
    return pthread_mutex_unlock( mutex );
}

static int local_cond_timedwait( pthread_cond_t *cond, pthread_mutex_t *mutex, struct timespec *until )
{
    return pthread_cond_timedwait( cond, mutex, until );
}

static void local_cond_signal( pthread_cond_t *cond )
{
    pthread_cond_signal( cond );
}

#elif HAVE_LIBTHREAD

#include <thread.h>

static mutex_t mutex_lists;
static mutex_t mutex_env;
static mutex_t mutex_pool;
static mutex_t mutex_iconv;

static cond_t cond_pool;

static int local_mutex_entry( mutex_t *mutex )
{
    return mutex_lock( mutex );
}

static int local_mutex_exit( mutex_t *mutex )
{
    return mutex_unlock( mutex );
}

static int local_cond_timedwait( cond_t *cond, mutex_t *mutex, struct timespec *until )
{
    return cond_timedwait( cond, mutex, until );
}

static void local_cond_signal( cond_t *cond )
{
    cond_signal( cond );
}

#else

#define local_mutex_entry(x)
#define local_mutex_exit(x)
#define local_cond_timedwait(x,y,z) 0
#define local_cond_signal(x)

#endif

/*
 * protection for connection pooling
 */

void mutex_pool_entry( void )
{
    local_mutex_entry( &mutex_pool );
}

void mutex_pool_exit( void )
{
    local_mutex_exit( &mutex_pool );
}

/*
 * protection for iconv
 */

void mutex_iconv_entry( void )
{
    local_mutex_entry( &mutex_iconv );
}

void mutex_iconv_exit( void )
{
    local_mutex_exit( &mutex_iconv );
}

/*
 * protection for lib loading and counting, reuse the lists mutex as this 
 * is the lowest level protection the DM uses
 */

void mutex_lib_entry( void )
{
    local_mutex_entry( &mutex_lists );
}

void mutex_lib_exit( void )
{
    local_mutex_exit( &mutex_lists );
}

static DMHENV __locked_alloc_env()
{
    DMHENV environment;

    environment = calloc( sizeof( *environment ), 1 );

    if ( environment )
    {
        char tracing_string[ 64 ];
        char tracing_file[ 64 ];

#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
        if (uodbc_open_stats(&environment->sh, UODBC_STATS_WRITE) != 0)
        {
            ;
        }
        uodbc_update_stats(environment->sh, UODBC_STATS_TYPE_HENV, (void *)1);
#endif
        
        /*
         * add to list of env handles
         */

        environment -> next_class_list = environment_root;
        environment_root = environment;
        environment -> type = HENV_MAGIC;

        SQLGetPrivateProfileString( "ODBC", "Trace", "No",
                    tracing_string, sizeof( tracing_string ), 
                    "odbcinst.ini" );

        if ( tracing_string[ 0 ] == '1' ||
                toupper( tracing_string[ 0 ] ) == 'Y' ||
                    ( toupper( tracing_string[ 0 ] ) == 'O' &&
                    toupper( tracing_string[ 1 ] ) == 'N' ))
        {
            SQLGetPrivateProfileString( "ODBC", "TraceFile", "/tmp/sql.log",
                    tracing_file, sizeof( tracing_file ), 
                    "odbcinst.ini" );

            /*
             * start logging
             */

            SQLGetPrivateProfileString( "ODBC", "TracePid", "No",
                    tracing_string, sizeof( tracing_string ), 
                    "odbcinst.ini" );

            if ( tracing_string[ 0 ] == '1' ||
                        toupper( tracing_string[ 0 ] ) == 'Y' ||
                        ( toupper( tracing_string[ 0 ] ) == 'O' &&
                        toupper( tracing_string[ 1 ] ) == 'N' ))
            {
                dm_log_open( "ODBC", tracing_file, 1 );
            }
            else
            {
                dm_log_open( "ODBC", tracing_file, 0 );
            }

            sprintf( environment -> msg,
                    "\n\t\tExit:[SQL_SUCCESS]\n\t\t\tEnvironment = %p",	environment );

            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO, environment -> msg );
        }
        setup_error_head( &environment -> error, environment,
                SQL_HANDLE_ENV );

    }

    return environment;
}

static DMHENV shared_environment;

DMHENV __share_env( int *first )
{
    DMHENV environment;

    local_mutex_entry( &mutex_lists );

    if ( shared_environment ) {

        *first = 0;

        environment = shared_environment;
    }
    else {
        environment = __locked_alloc_env();

        *first = 1;

        shared_environment = environment;
    }

    local_mutex_exit( &mutex_lists );

    return environment;
}


/*
 * allocate and register a environment handle
 */

DMHENV __alloc_env( void )
{
    DMHENV environment = NULL;

    local_mutex_entry( &mutex_lists );

    environment = __locked_alloc_env();

    local_mutex_exit( &mutex_lists );

    return environment;
}

/*
 * check that a env is real
 */

int __validate_env_mark_released( DMHENV env )
{
    if ( shared_environment && env == shared_environment ) {
        return 1;
    }

#ifdef FAST_HANDLE_VALIDATE

    if ( env && *(( int * ) env ) == HENV_MAGIC )
        return 1;
    else
        return 0;

#else

    DMHENV ptr;
    int ret = 0;

    local_mutex_entry( &mutex_lists );

    ptr = environment_root;

    while( ptr )
    {
        if ( ptr == env )
        {
            ret = 1;
            env -> released = 1;
            break;
        }

        ptr = ptr -> next_class_list;
    }

    local_mutex_exit( &mutex_lists );

    return ret;

#endif
}

int __validate_env( DMHENV env )
{
    if ( shared_environment && env == shared_environment ) {
        return 1;
    }

#ifdef FAST_HANDLE_VALIDATE

    if ( env && *(( int * ) env ) == HENV_MAGIC )
        return 1;
    else
        return 0;

#else

    DMHENV ptr;
    int ret = 0;

    local_mutex_entry( &mutex_lists );

    ptr = environment_root;

    while( ptr )
    {
        if ( ptr == env )
        {
            if ( env -> released ) 
            {
                fprintf( stderr, "unixODBC: API Error, env handle used after being free\n" );
                ret = 0;
            }
            else 
            {
                ret = 1;
            }
            break;
        }

        ptr = ptr -> next_class_list;
    }

    local_mutex_exit( &mutex_lists );

    return ret;

#endif
}

/*
 * remove from list
 */

void __release_env( DMHENV environment )
{
    DMHENV last = NULL;
    DMHENV ptr;

    if ( shared_environment && environment == shared_environment ) {
        return;
    } 

    local_mutex_entry( &mutex_lists );

    ptr = environment_root;

    while( ptr )
    {
        if ( environment == ptr )
        {
            break;
        }
        last = ptr;
        ptr = ptr -> next_class_list;
    }

    if ( ptr )
    {
        if ( last )
        {
            last -> next_class_list = ptr -> next_class_list;
        }
        else
        {
            environment_root = ptr -> next_class_list;
        }
    }

    clear_error_head( &environment -> error );

	/*
	 * free log
	 */

    dm_log_close();

#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
    if (environment->sh)
        uodbc_close_stats(environment->sh);
#endif
    
    /*
     * clear just to make sure
     */

    memset( environment, 0, sizeof( *environment ));

    free( environment );

    local_mutex_exit( &mutex_lists );
}

/*
 * get the root, for use in SQLEndTran and SQLTransact
 */

DMHDBC __get_dbc_root( void )
{
    return connection_root;
}

/*
 * allocate and register a connection handle
 */

DMHDBC __alloc_dbc( void )
{
    DMHDBC connection = NULL;

    local_mutex_entry( &mutex_lists );

    connection = calloc( sizeof( *connection ), 1 );

    if ( connection )
    {
        /*
         * add to list of connection handles
         */

        connection -> next_class_list = connection_root;
        connection_root = connection;
        connection -> type = HDBC_MAGIC;

        setup_error_head( &connection -> error, connection,
                SQL_HANDLE_DBC );

#ifdef HAVE_LIBPTH
        pth_mutex_init( &connection -> mutex );
        /*
         * for the moment protect at the environment level
         */
        connection -> protection_level = TS_LEVEL3;
#elif HAVE_LIBPTHREAD
        pthread_mutex_init( &connection -> mutex, NULL );
        /*
         * for the moment protect at the environment level
         */
        connection -> protection_level = TS_LEVEL3;
#elif HAVE_LIBTHREAD
        mutex_init( &connection -> mutex, USYNC_THREAD, NULL );
        connection -> protection_level = TS_LEVEL3;
#endif

#ifdef HAVE_ICONV
        connection -> iconv_cd_uc_to_ascii = (iconv_t)(-1);
        connection -> iconv_cd_ascii_to_uc = (iconv_t)(-1);
#endif
    }

    local_mutex_exit( &mutex_lists );

    return connection;
}

/*
 * adjust the threading level
 */

void dbc_change_thread_support( DMHDBC connection, int level )
{
#if defined ( HAVE_LIBPTHREAD ) || defined( HAVE_LIBTHREAD ) || defined( HAVE_LIBPTH )
    int old_level;

    if ( connection -> protection_level == level )
	return;

    old_level =  connection -> protection_level;
    connection -> protection_level = level;

    if ( level == TS_LEVEL3 )
    {
	/*
         * if we are moving from level 3 we may have to release the existing
         * connection lock, and create the env lock
         */
	if(old_level != TS_LEVEL0)
            local_mutex_exit( &connection -> mutex );
        local_mutex_entry( &mutex_env );
    }
    else if ( old_level == TS_LEVEL3 )
    {
         /*
         * if we are moving from level 3 we may have to create the new
         * connection lock, and remove the env lock
         */
	if(level != TS_LEVEL0)
	    local_mutex_entry( &connection -> mutex );
        local_mutex_exit( &mutex_env );
    }

#endif
}

/*
 * check that a connection is real
 */

int __validate_dbc( DMHDBC connection )
{
#ifdef FAST_HANDLE_VALIDATE

    if ( connection && *(( int * ) connection ) == HDBC_MAGIC )
        return 1;
    else
        return 0;

#else

    DMHDBC ptr;
    int ret = 0;

    local_mutex_entry( &mutex_lists );

    ptr = connection_root;

    while( ptr )
    {
        if ( ptr == connection )
        {
            ret = 1;
            break;
        }

        ptr = ptr -> next_class_list;
    }

    local_mutex_exit( &mutex_lists );

    return ret;
#endif
}

/*
 * remove from list
 */

void __release_dbc( DMHDBC connection )
{
    DMHDBC last = NULL;
    DMHDBC ptr;

    local_mutex_entry( &mutex_lists );

    ptr = connection_root;

    while( ptr )
    {
        if ( connection == ptr )
        {
            break;
        }
        last = ptr;
        ptr = ptr -> next_class_list;
    }

    if ( ptr )
    {
        if ( last )
        {
            last -> next_class_list = ptr -> next_class_list;
        }
        else
        {
            connection_root = ptr -> next_class_list;
        }
    }

    clear_error_head( &connection -> error );

    /*
     * shutdown unicode
     */

    unicode_shutdown( connection );

#ifdef HAVE_LIBPTH
#elif HAVE_LIBPTHREAD
    pthread_mutex_destroy( &connection -> mutex );
#elif HAVE_LIBTHREAD
    mutex_destroy( &connection -> mutex );
#endif

    if ( connection -> save_attr )
    {
        struct save_attr *sa = connection -> save_attr;
        while ( sa )
        {
            struct save_attr *nsa = sa -> next;
            free( sa -> str_attr );
            free( sa );
            sa = nsa;
        }
    }

    if ( connection -> _driver_connect_string ) {
        free( connection -> _driver_connect_string );
    }
    if ( connection -> _user ) {
        free( connection -> _user );
    }
    if ( connection -> _password ) {
        free( connection -> _password );
    }
    if ( connection -> _server ) {
        free( connection -> _server );
    }

    /*
     * clear just to make sure
     */

    memset( connection, 0, sizeof( *connection ));

    free( connection );

    local_mutex_exit( &mutex_lists );
}

/*
 * allocate and register a statement handle
 */

DMHSTMT __alloc_stmt( void )
{
    DMHSTMT statement = NULL;

    local_mutex_entry( &mutex_lists );

    statement = calloc( sizeof( *statement ), 1 );

    if ( statement )
    {
        /*
         * add to list of statement handles
         */

        statement -> next_class_list = statement_root;
#ifdef FAST_HANDLE_VALIDATE
        if ( statement_root )
        {
            statement_root -> prev_class_list = statement;
        }
#endif    
        statement_root = statement;
        statement -> type = HSTMT_MAGIC;

        setup_error_head( &statement -> error, statement,
                SQL_HANDLE_STMT );

#ifdef HAVE_LIBPTH
        pth_mutex_init( &statement -> mutex );
#elif HAVE_LIBPTHREAD
        pthread_mutex_init( &statement -> mutex, NULL );
#elif HAVE_LIBTHREAD
        mutex_init( &statement -> mutex, USYNC_THREAD, NULL );
#endif

    }

    local_mutex_exit( &mutex_lists );

    return statement;
}

/*
 * assigns a statements to the connection
 */

void __register_stmt ( DMHDBC connection, DMHSTMT statement )
{
    local_mutex_entry( &mutex_lists );

    connection -> statement_count ++;
    statement -> connection = connection;
#ifdef FAST_HANDLE_VALIDATE
    statement -> next_conn_list = connection -> statements;
    connection -> statements = statement;
#endif
    local_mutex_exit( &mutex_lists );
}

/*
 * Sets statement state after commit or rollback transaction
 */ 
void __set_stmt_state ( DMHDBC connection, SQLSMALLINT cb_value )
{
    DMHSTMT         statement;
    SQLINTEGER stmt_remaining;

    local_mutex_entry( &mutex_lists );
#ifdef FAST_HANDLE_VALIDATE
    statement      = connection -> statements;
    while ( statement )
    {
        if ( (statement -> state == STATE_S2 ||
              statement -> state == STATE_S3) &&
             cb_value == SQL_CB_DELETE )
        {
            statement -> state = STATE_S1;
            statement -> prepared = 0;
        }
        else if ( statement -> state == STATE_S4 ||
              statement -> state == STATE_S5 ||
              statement -> state == STATE_S6 ||
              statement -> state == STATE_S7 )
        {
            if( !statement -> prepared && 
                (cb_value == SQL_CB_DELETE ||
                 cb_value == SQL_CB_CLOSE) )
            {
                statement -> state = STATE_S1;
            }
            else if( statement -> prepared )
            {
                if( cb_value == SQL_CB_DELETE )
                {
                    statement -> state = STATE_S1;
                    statement -> prepared = 0;
                }
                else if( cb_value == SQL_CB_CLOSE )
                {
                    if ( statement -> state == STATE_S4 )
                      statement -> state = STATE_S2;
                    else
                      statement -> state = STATE_S3;
                }
            }
        }
        statement = statement -> next_conn_list;
    }
#else
    statement      = statement_root;
    stmt_remaining = connection -> statement_count;

    while ( statement && stmt_remaining > 0 )
    {
        if ( statement -> connection == connection )
        {
            if ( (statement -> state == STATE_S2 ||
                  statement -> state == STATE_S3) &&
                 cb_value == SQL_CB_DELETE )
            {
                statement -> state = STATE_S1;
                statement -> prepared = 0;
            }
            else if ( statement -> state == STATE_S4 ||
                  statement -> state == STATE_S5 ||
                  statement -> state == STATE_S6 ||
                  statement -> state == STATE_S7 )
            {
                if( !statement -> prepared && 
                    (cb_value == SQL_CB_DELETE ||
                     cb_value == SQL_CB_CLOSE) )
                {
                    statement -> state = STATE_S1;
                }
                else if( statement -> prepared )
                {
                    if( cb_value == SQL_CB_DELETE )
                    {
                        statement -> state = STATE_S1;
                        statement -> prepared = 0;
                    }
                    else if( cb_value == SQL_CB_CLOSE )
                    {
                        if ( statement -> state == STATE_S4 )
                          statement -> state = STATE_S2;
                        else
                          statement -> state = STATE_S3;
                    }
                }
            }

            stmt_remaining --;
        }

        statement = statement -> next_class_list;
    }
#endif
    local_mutex_exit( &mutex_lists );
}

/*
 * clear all statements on a DBC
 */

int __clean_stmt_from_dbc( DMHDBC connection )
{
    DMHSTMT ptr, last;
    int ret = 0;

    local_mutex_entry( &mutex_lists );
#ifdef FAST_HANDLE_VALIDATE
    while ( connection -> statements )
    {
        ptr  = connection -> statements;
        last = connection -> statements -> prev_class_list;
        
        connection -> statements = ptr -> next_conn_list;
        if ( last )
        {
            last -> next_class_list = ptr -> next_class_list;
            if ( last -> next_class_list )
            {
                last -> next_class_list -> prev_class_list = last;
            }
        }
        else
        {
            statement_root = ptr -> next_class_list;
            if ( statement_root )
            {
                statement_root -> prev_class_list = NULL;
            }
        }
        clear_error_head( &ptr -> error );

#ifdef HAVE_LIBPTH
#elif HAVE_LIBPTHREAD
        pthread_mutex_destroy( &ptr -> mutex );
#elif HAVE_LIBTHREAD
        mutex_destroy( &ptr -> mutex );
#endif
        free( ptr );
    }        
#else
    last = NULL;
    ptr  = statement_root;

    while( ptr )
    {
        if ( ptr -> connection == connection )
        {
            if ( last )
            {
                last -> next_class_list = ptr -> next_class_list;
            }
            else
            {
                statement_root = ptr -> next_class_list;
            }
            clear_error_head( &ptr -> error );

#ifdef HAVE_LIBPTH
#elif HAVE_LIBPTHREAD
            pthread_mutex_destroy( &ptr -> mutex );
#elif HAVE_LIBTHREAD
            mutex_destroy( &ptr -> mutex );
#endif
            free( ptr );

            /*
             * go back to the start
             */

            last = NULL;
            ptr = statement_root;
        }
        else
        {
            last = ptr;
            ptr = ptr -> next_class_list;
        }
    }
#endif
    local_mutex_exit( &mutex_lists );

    return ret;
}

int __check_stmt_from_dbc_v( DMHDBC connection, int statecount, ... )
{
    va_list ap;
    int states[ MAX_STATE_ARGS ];
    DMHSTMT ptr;
    int found = 0;
    int i;

    va_start (ap, statecount);
    for ( i = 0; i < statecount; i ++ ) {
        states[ i ] = va_arg (ap, int );
    }
    va_end (ap);

    local_mutex_entry( &mutex_lists );
#ifdef FAST_HANDLE_VALIDATE
    ptr = connection -> statements;
    while( !found && ptr )
    {
        for ( i = 0; i < statecount; i ++ ) {
            if ( ptr -> state == states[ i ] ) {
                found = 1;
                break;
            }
       }
    
        ptr = ptr -> next_conn_list;
    }
#else
    ptr = statement_root;
    while( !found && ptr )
    {
        if ( ptr -> connection == connection )
        {
            for ( i = 0; i < statecount; i ++ ) {
                if ( ptr -> state == states[ i ] ) {
                    found = 1;
                    break;
                }
            }
        }

        ptr = ptr -> next_class_list;
    }
#endif
    local_mutex_exit( &mutex_lists );

    return found;
}

/*
 * check if any statements on this connection are in a given state
 */

int __check_stmt_from_dbc( DMHDBC connection, int state )
{
    DMHSTMT ptr;
    int found = 0;

    local_mutex_entry( &mutex_lists );
#ifdef FAST_HANDLE_VALIDATE
    ptr = connection -> statements;
    while( ptr )
    {
        if ( ptr -> state == state ) 
        {
            found = 1;
            break;
        }
    
        ptr = ptr -> next_conn_list;
    }
#else
    ptr = statement_root;
    while( ptr )
    {
        if ( ptr -> connection == connection )
        {
            if ( ptr -> state == state ) 
            {
                found = 1;
                break;
            }
        }

        ptr = ptr -> next_class_list;
    }
#endif
    local_mutex_exit( &mutex_lists );

    return found;
}

int __check_stmt_from_desc( DMHDESC desc, int state )
{
    DMHDBC connection;
    DMHSTMT ptr;
    int found = 0;

    local_mutex_entry( &mutex_lists );
    connection = desc -> connection;
#ifdef FAST_HANDLE_VALIDATE
    ptr = connection -> statements;
    while( ptr )
    {
        if ( ptr -> ipd == desc || ptr -> ird == desc || ptr -> apd == desc || ptr -> ard == desc ) 
        {
            if ( ptr -> state == state ) 
            {
                found = 1;
                break;
            }
        }
    
        ptr = ptr -> next_conn_list;
    }
#else
    ptr = statement_root;
    while( ptr )
    {
        if ( ptr -> connection == connection )
        {
            if ( ptr -> ipd == desc || ptr -> ird == desc || ptr -> apd == desc || ptr -> ard == desc ) 
            {
                if ( ptr -> state == state ) 
                {
                    found = 1;
                    break;
                }
            }
        }

        ptr = ptr -> next_class_list;
    }
#endif
    local_mutex_exit( &mutex_lists );

    return found;
}

int __check_stmt_from_desc_ird( DMHDESC desc, int state )
{
    DMHDBC connection;
    DMHSTMT ptr;
    int found = 0;

    local_mutex_entry( &mutex_lists );
    connection = desc -> connection;
#ifdef FAST_HANDLE_VALIDATE
    ptr = connection -> statements;
    while( ptr )
    {
        if ( ptr -> ird == desc ) 
        {
            if ( ptr -> state == state ) 
            {
                found = 1;
                break;
            }
        }
    
        ptr = ptr -> next_conn_list;
    }
#else
    ptr = statement_root;
    while( ptr )
    {
        if ( ptr -> connection == connection )
        {
            if ( ptr -> ird == desc ) 
            {
                if ( ptr -> state == state ) 
                {
                    found = 1;
                    break;
                }
            }
        }

        ptr = ptr -> next_class_list;
    }
#endif
    local_mutex_exit( &mutex_lists );

    return found;
}

/*
 * check any statements that are associated with a descriptor
 */

/*
 * check that a statement is real
 */

int __validate_stmt( DMHSTMT statement )
{
#ifdef FAST_HANDLE_VALIDATE

    if ( statement && *(( int * ) statement ) == HSTMT_MAGIC )
        return 1;
    else
        return 0;

#else

    DMHSTMT ptr;
    int ret = 0;

    local_mutex_entry( &mutex_lists );

    ptr = statement_root;

    while( ptr )
    {
        if ( ptr == statement )
        {
            ret = 1;
            break;
        }

        ptr = ptr -> next_class_list;
    }

    local_mutex_exit( &mutex_lists );

    return ret;

#endif
}

/*
 * remove from list
 */

void __release_stmt( DMHSTMT statement )
{
    DMHSTMT last = NULL;
    DMHSTMT ptr;

    local_mutex_entry( &mutex_lists );
#ifdef FAST_HANDLE_VALIDATE
    /*
     * A check never mind
     */
    if ( statement && ( *(( int * ) statement ) == HSTMT_MAGIC ))
    {
        ptr  = statement;
        last = statement->prev_class_list;
        
        if ( statement -> connection )
        {
            DMHDBC connection = statement -> connection;
            DMHSTMT conn_last = NULL;
            DMHSTMT  conn_ptr = connection -> statements;
            while ( conn_ptr )
            {
                if ( statement == conn_ptr )
                {
                    break;
                }
                conn_last = conn_ptr;
                conn_ptr  = conn_ptr -> next_conn_list;
            }
            if ( conn_ptr )
            {
                if ( conn_last )
                {
                    conn_last -> next_conn_list = conn_ptr -> next_conn_list;
                }
                else
                {
                    connection -> statements    = conn_ptr -> next_conn_list;
                }
            }
        }
    }
    else
    {
        ptr  = NULL;
        last = NULL;
    }
#else
    ptr = statement_root;

    while( ptr )
    {
        if ( statement == ptr )
        {
            break;
        }
        last = ptr;
        ptr = ptr -> next_class_list;
    }
#endif
    if ( ptr )
    {
        if ( last )
        {
            last -> next_class_list = ptr -> next_class_list;
#ifdef FAST_HANDLE_VALIDATE
            if ( last -> next_class_list )
            {
                last -> next_class_list -> prev_class_list = last;
            }
#endif            
        }
        else
        {
            statement_root = ptr -> next_class_list;
#ifdef FAST_HANDLE_VALIDATE
            if ( statement_root )
            {
                statement_root -> prev_class_list = NULL;
            }
#endif            
        }
    }

    clear_error_head( &statement -> error );

#ifdef HAVE_LIBPTH
#elif HAVE_LIBPTHREAD
    pthread_mutex_destroy( &statement -> mutex );
#elif HAVE_LIBTHREAD
    mutex_destroy( &statement -> mutex );
#endif

    /*
     * clear just to make sure
     */

    memset( statement, 0, sizeof( *statement ));

    free( statement );

    local_mutex_exit( &mutex_lists );
}

/*
 * allocate and register a descriptor handle
 */

DMHDESC __alloc_desc( void )
{
    DMHDESC descriptor;

    local_mutex_entry( &mutex_lists );

    descriptor = calloc( sizeof( *descriptor ), 1 );

    if ( descriptor )
    {
        /*
         * add to list of descriptor handles
         */

        descriptor -> next_class_list = descriptor_root;
#ifdef FAST_HANDLE_VALIDATE
        if ( descriptor_root )
        {
            descriptor_root -> prev_class_list = descriptor;
        }
#endif    
        descriptor_root = descriptor;
        descriptor -> type = HDESC_MAGIC;

        setup_error_head( &descriptor -> error, descriptor, SQL_HANDLE_DESC );

#ifdef HAVE_LIBPTH
        pth_mutex_init( &descriptor -> mutex );
#elif HAVE_LIBPTHREAD
        pthread_mutex_init( &descriptor -> mutex, NULL );
#elif HAVE_LIBTHREAD
        mutex_init( &descriptor -> mutex, USYNC_THREAD, NULL );
#endif
    }

    local_mutex_exit( &mutex_lists );

    return descriptor;
}

/*
 * check that a descriptor is real
 */

int __validate_desc( DMHDESC descriptor )
{
#ifdef FAST_HANDLE_VALIDATE

    if ( descriptor && *(( int * ) descriptor ) == HDESC_MAGIC )
        return 1;
    else
        return 0;

#else

    DMHDESC ptr;
    int ret = 0;

    local_mutex_entry( &mutex_lists );

    ptr = descriptor_root;

    while( ptr )
    {
        if ( ptr == descriptor )
        {
            ret = 1;
            break;
        }

        ptr = ptr -> next_class_list;
    }

    local_mutex_exit( &mutex_lists );

    return ret;

#endif
}

/*
 * clear all descriptors on a DBC
 */

int __clean_desc_from_dbc( DMHDBC connection )
{
    DMHDESC ptr, last;
    int ret = 0;

    local_mutex_entry( &mutex_lists );
    last = NULL;
    ptr = descriptor_root;

    while( ptr )
    {
        if ( ptr -> connection == connection )
        {
            if ( last )
            {
                last -> next_class_list = ptr -> next_class_list;
#ifdef FAST_HANDLE_VALIDATE
                if ( last -> next_class_list )
                {
                    last -> next_class_list -> prev_class_list = last;
                }
#endif            
            }
            else
            {
                descriptor_root = ptr -> next_class_list;
#ifdef FAST_HANDLE_VALIDATE
                if ( descriptor_root )
                {
                    descriptor_root -> prev_class_list = NULL;
                }
#endif            
            }
            clear_error_head( &ptr -> error );

#ifdef HAVE_LIBPTH
#elif HAVE_LIBPTHREAD
            pthread_mutex_destroy( &ptr -> mutex );
#elif HAVE_LIBTHREAD
            mutex_destroy( &ptr -> mutex );
#endif
            free( ptr );

            /*
             * go back to the start
             */

            last = NULL;
            ptr = descriptor_root;
        }
        else
        {
            last = ptr;
            ptr = ptr -> next_class_list;
        }
    }

    local_mutex_exit( &mutex_lists );

    return ret;
}


/*
 * remove from list
 */

void __release_desc( DMHDESC descriptor )
{
    DMHDESC last = NULL;
    DMHDESC ptr;
    DMHSTMT assoc_stmt;

    local_mutex_entry( &mutex_lists );
#ifdef FAST_HANDLE_VALIDATE
    /*
     * A check never mind
     */
    if ( descriptor && ( *(( int * ) descriptor ) == HDESC_MAGIC ))
    {
        ptr  = descriptor;
        last = descriptor->prev_class_list;
    }
    else
    {
        ptr  = NULL;
        last = NULL;
    }
#else
    ptr = descriptor_root;

    while( ptr )
    {
        if ( descriptor == ptr )
        {
            break;
        }
        last = ptr;
        ptr = ptr -> next_class_list;
    }
#endif

    if ( ptr )
    {
        if ( last )
        {
            last -> next_class_list = ptr -> next_class_list;
#ifdef FAST_HANDLE_VALIDATE
            if ( last -> next_class_list )
            {
                last -> next_class_list -> prev_class_list = last;
            }
#endif            
        }
        else
        {
            descriptor_root = ptr -> next_class_list;
#ifdef FAST_HANDLE_VALIDATE
            if ( descriptor_root )
            {
                descriptor_root -> prev_class_list = NULL;
            }
#endif            
        }
    }

    clear_error_head( &descriptor -> error );
    /* If there are any statements still pointing to this descriptor, revert them to implicit */
    assoc_stmt = statement_root;
    while ( assoc_stmt )
    {
        DMHDESC *pDesc[] = {
            &assoc_stmt -> ipd, &assoc_stmt -> apd, &assoc_stmt -> ird, &assoc_stmt -> ard
        };
        DMHDESC impDesc[] = {
            assoc_stmt -> implicit_ipd, assoc_stmt -> implicit_apd,
            assoc_stmt -> implicit_ird, assoc_stmt -> implicit_ard
        };
        int i;
        for ( i = 0; i < 4; i++ )
        {
            if ( *pDesc[i] == descriptor )
            {
                *pDesc[i] = impDesc[i];
            }
        }
        assoc_stmt = assoc_stmt -> next_class_list;
    }

#ifdef HAVE_LIBPTH
#elif HAVE_LIBPTHREAD
    pthread_mutex_destroy( &descriptor -> mutex );
#elif HAVE_LIBTHREAD
    mutex_destroy( &descriptor -> mutex );
#endif

    /*
     * clear just to make sure
     */

    memset( descriptor, 0, sizeof( *descriptor ));

    free( descriptor );

    local_mutex_exit( &mutex_lists );
}

#if defined ( HAVE_LIBPTHREAD ) || defined ( HAVE_LIBTHREAD ) || defined( HAVE_LIBPTH )

void thread_protect( int type, void *handle )
{
    DMHDBC connection;
    DMHSTMT statement;
    DMHDESC descriptor;

    switch( type )
    {
      case SQL_HANDLE_ENV:
        local_mutex_entry( &mutex_env );
        break;

      case SQL_HANDLE_DBC:
        connection = handle;
        if ( connection -> protection_level == TS_LEVEL3 )
        {
            local_mutex_entry( &mutex_env );
        }
        else if ( connection -> protection_level == TS_LEVEL2 ||
                connection -> protection_level == TS_LEVEL1 )
        {
            local_mutex_entry( &connection -> mutex );
        }
        break;

      case SQL_HANDLE_STMT:
        statement = handle;
        if ( statement -> connection -> protection_level == TS_LEVEL3 )
        {
            local_mutex_entry( &mutex_env );
        }
        else if ( statement -> connection -> protection_level == TS_LEVEL2 )
        {
            local_mutex_entry( &statement -> connection -> mutex );
        }
        else if ( statement -> connection -> protection_level == TS_LEVEL1 )
        {
            local_mutex_entry( &statement -> mutex );
        }
        break;

      case SQL_HANDLE_DESC:
        descriptor = handle;
        if ( descriptor -> connection -> protection_level == TS_LEVEL3 )
        {
            local_mutex_entry( &mutex_env );
        }
        if ( descriptor -> connection -> protection_level == TS_LEVEL2 )
        {
            local_mutex_entry( &descriptor -> connection -> mutex );
        }
        if ( descriptor -> connection -> protection_level == TS_LEVEL1 )
        {
            local_mutex_entry( &descriptor -> mutex );
        }
        break;
    }
}

void thread_release( int type, void *handle )
{
    DMHDBC connection;
    DMHSTMT statement;
    DMHDESC descriptor;

    switch( type )
    {
      case SQL_HANDLE_ENV:
        local_mutex_exit( &mutex_env );
        break;

      case SQL_HANDLE_DBC:
        connection = handle;
        if ( connection -> protection_level == TS_LEVEL3 )
        {
            local_mutex_exit( &mutex_env );
        }
        else if ( connection -> protection_level == TS_LEVEL2 ||
                connection -> protection_level == TS_LEVEL1 )
        {
            local_mutex_exit( &connection -> mutex );
        }
        break;

      case SQL_HANDLE_STMT:
        statement = handle;
        if ( statement -> connection -> protection_level == TS_LEVEL3 )
        {
            local_mutex_exit( &mutex_env );
        }
        else if ( statement -> connection -> protection_level == TS_LEVEL2 )
        {
            local_mutex_exit( &statement -> connection -> mutex );
        }
        else if ( statement -> connection -> protection_level == TS_LEVEL1 )
        {
            local_mutex_exit( &statement -> mutex );
        }
        break;

      case SQL_HANDLE_DESC:
        descriptor = handle;
        if ( descriptor -> connection -> protection_level == TS_LEVEL3 )
        {
            local_mutex_exit( &mutex_env );
        }
        else if ( descriptor -> connection -> protection_level == TS_LEVEL2 )
        {
            local_mutex_exit( &descriptor -> connection -> mutex );
        }
        else if ( descriptor -> connection -> protection_level == TS_LEVEL1 )
        {
            local_mutex_exit( &descriptor -> mutex );
        }
        break;
    }
}

/*
 * Waits on pool condition variable until signaled, or 1s timeout elapses.
 *
 * Will be called with mutexes locked according to threading level as follows:
 *
 * 0   - mutex_pool
 * 1,2 - connection->mutex mutex_pool
 * 3   - mutex_env mutex_pool
 *
 * Returns
 *   nonzero on timeout
 *   zero when signaled
 */


int pool_timedwait( DMHDBC connection )
{
    int ret = 0;
    struct timespec waituntil;

#ifdef HAVE_CLOCK_GETTIME
    clock_gettime( CLOCK_REALTIME, &waituntil );
    waituntil.tv_sec ++;
#else
    waituntil.tv_sec = time( NULL );
    waituntil.tv_nsec = 0;

    waituntil.tv_sec ++;
#endif

    switch ( connection -> protection_level )
    {
        case TS_LEVEL3:
            mutex_pool_exit();
            ret = local_cond_timedwait( &cond_pool, &mutex_env, &waituntil );
            mutex_pool_entry();
            break;
        case TS_LEVEL2:
        case TS_LEVEL1:
            mutex_pool_exit();
            ret = local_cond_timedwait( &cond_pool, &connection -> mutex, &waituntil );
            mutex_pool_entry();
            break;
        case TS_LEVEL0:
            ret = local_cond_timedwait( &cond_pool, &mutex_pool, &waituntil );
            break;
    }
    return ret;
}

void pool_signal()
{
    local_cond_signal( &cond_pool );
}

#endif

#ifdef WITH_HANDLE_REDIRECT

/*
 * try and find a handle that has the suplied handle as the driver handle
 * there will be threading issues with this, so be carefull. 
 * However it will normally only get used with "broken" drivers.
 */


void *find_parent_handle( DRV_SQLHANDLE drv_hand, int type )
{
	void *found_handle = NULL;

    local_mutex_entry( &mutex_lists );

	switch( type ) {
		case SQL_HANDLE_DBC:
			{
				DMHDBC hand = connection_root;
				while( hand ) {
					if ( hand -> driver_dbc == drv_hand ) {
						found_handle = hand;
						break;
					}
					hand = hand -> next_class_list;
				}
			}
			break;

		case SQL_HANDLE_STMT:
			{
				DMHSTMT hand = statement_root;
				while( hand ) {
					if ( hand -> driver_stmt == drv_hand ) {
						found_handle = hand;
						break;
					}
					hand = hand -> next_class_list;
				}
			}
			break;

		case SQL_HANDLE_DESC:
			{
				DMHDESC hand = descriptor_root;
				while( hand ) {
					if ( hand -> driver_desc == drv_hand ) {
						found_handle = hand;
						break;
					}
					hand = hand -> next_class_list;
				}
			}
			break;

		default:
			break;
	}

    local_mutex_exit( &mutex_lists );

	return found_handle;
}

#endif
