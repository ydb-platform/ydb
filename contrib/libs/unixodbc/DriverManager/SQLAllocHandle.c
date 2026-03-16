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
 * $Id: SQLAllocHandle.c,v 1.13 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLAllocHandle.c,v $
 * Revision 1.13  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.12  2007/12/17 13:13:03  lurcher
 * Fix a couple of descriptor typo's
 *
 * Revision 1.11  2007/02/12 11:49:34  lurcher
 * Add QT4 support to existing GUI parts
 *
 * Revision 1.10  2006/06/28 08:08:41  lurcher
 * Add timestamp with timezone to Postgres7.1 driver
 *
 * Revision 1.9  2005/11/21 17:25:43  lurcher
 * A few DM fixes for Oracle's ODBC driver
 *
 * Revision 1.8  2005/11/08 09:37:10  lurcher
 * Allow the driver and application to have different length handles
 *
 * Revision 1.7  2005/10/06 08:50:58  lurcher
 * Fix problem with SQLDrivers not returning first entry
 *
 * Revision 1.6  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.5  2003/02/25 13:28:21  lurcher
 *
 * Allow errors on the drivers AllocHandle to be reported
 * Fix a problem that caused errors to not be reported in the log
 * Remove a redundant line from the spec file
 *
 * Revision 1.4  2002/12/12 18:21:21  lurcher
 *
 * Fix bug where if a SQLAllocHandle in the driver failed, the driver manager
 * seg faulted
 *
 * Revision 1.3  2002/08/12 13:17:52  lurcher
 *
 * Replicate the way the MS DM handles loading of driver libs, and allocating
 * handles in the driver. usage counting in the driver means that dlopen is
 * only called for the first use, and dlclose for the last. AllocHandle for
 * the driver environment is only called for the first time per driver
 * per application environment.
 *
 * Revision 1.2  2002/07/25 09:30:26  lurcher
 *
 * Additional unicode and iconv changes
 *
 * Revision 1.1.1.1  2001/10/17 16:40:03  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.13  2001/09/27 17:05:48  nick
 *
 * Assorted fixes and tweeks
 *
 * Revision 1.12  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 * Revision 1.11  2001/08/03 15:19:00  nick
 *
 * Add changes to set values before connect
 *
 * Revision 1.10  2001/07/31 12:03:46  nick
 *
 * Fix how the DM gets the CLI year for SQLGetInfo
 * Fix small bug in strncasecmp
 *
 * Revision 1.9  2001/06/04 15:24:49  nick
 *
 * Add port to MAC OSX and QT3 changes
 *
 * Revision 1.8  2001/05/15 13:33:44  jason
 *
 * Wrapped calls to stats with COLLECT_STATS
 *
 * Revision 1.7  2001/04/12 17:43:35  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.6  2000/12/18 11:03:58  martin
 *
 * Add support for the collection and retrieval of handle statistics.
 *
 * Revision 1.5  2000/12/14 18:10:18  nick
 *
 * Add connection pooling
 *
 * Revision 1.4  2000/11/22 19:03:40  nick
 *
 * Fix problem with error status in SQLSpecialColumns
 *
 * Revision 1.3  2000/11/22 18:35:43  nick
 *
 * Check input handle before touching output handle
 *
 * Revision 1.2  2000/11/14 10:15:27  nick
 *
 * Add test for localtime_r
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.19  2000/06/21 11:07:35  ngorham
 *
 * Stop Errors from SQLAllocHandle being lost
 *
 * Revision 1.18  2000/05/22 17:10:34  ngorham
 *
 * Fix problems with the FetchScroll -> ExtendedFetch Mapping
 *
 * Revision 1.17  2000/05/21 21:49:18  ngorham
 *
 * Assorted fixes
 *
 * Revision 1.16  2001/04/27 01:29:35  ngorham
 *
 * Added a couple of fixes from Tim Roepken
 *
 * Revision 1.15  2000/01/03 14:24:01  ngorham
 *
 * Fix bug where a failed alloc of a statement would dump core
 *
 * Revision 1.14  2000/01/01 18:21:16  ngorham
 *
 * Fix small bug where a invalid input handle to SQLAllocHandle can cause
 * a seg fault.
 *
 * Revision 1.13  1999/12/01 09:20:07  ngorham
 *
 * Fix some threading problems
 *
 * Revision 1.12  1999/11/15 21:42:52  ngorham
 *
 * Remove some debug info
 *
 * Revision 1.11  1999/11/13 23:40:58  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.10  1999/11/10 03:51:33  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.9  1999/10/24 23:54:17  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.8  1999/10/20 23:01:41  ngorham
 *
 * Fixed problem with the connection counting
 *
 * Revision 1.7  1999/09/26 18:55:03  ngorham
 *
 * Fixed a problem where the cursor lib was being used by default
 *
 * Revision 1.6  1999/09/21 22:34:23  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.5  1999/09/19 22:24:33  ngorham
 *
 * Added support for the cursor library
 *
 * Revision 1.4  1999/07/10 21:10:15  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.3  1999/07/04 21:05:06  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/30 23:56:54  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:05  sShandyb
 * first go at it
 *
 * Revision 1.3  1999/06/02 20:12:10  ngorham
 *
 * Fixed botched log entry, and removed the dos \r from the sql header files.
 *
 * Revision 1.2  1999/06/02 19:57:20  ngorham
 *
 * Added code to check if a attempt is being made to compile with a C++
 * Compiler, and issue a message.
 * Start work on the ODBC2-3 conversions.
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.4  1999/05/09 23:27:11  nick
 * All the API done now
 *
 * Revision 1.3  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.2  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.1  1999/04/25 23:02:41  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"
#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
#include "__stats.h"
#include <uodbc_stats.h>
#endif

static char const rcsid[]= "$RCSfile: SQLAllocHandle.c,v $ $Revision: 1.13 $";

/*
 * connection pooling stuff
 */

extern int pooling_enabled;
extern int pool_max_size;
extern int pool_wait_timeout;


/*
 * this is used so that it can be called without falling
 * fowl of any other instances in any other modules.
 */

SQLRETURN __SQLAllocHandle( SQLSMALLINT handle_type,
           SQLHANDLE input_handle,
           SQLHANDLE *output_handle,
           SQLINTEGER requested_version )
{
    switch( handle_type )
    {
      case SQL_HANDLE_ENV:
      case SQL_HANDLE_SENV:
        {
            DMHENV environment;
            char pooling_string[ 128 ];
            char pool_max_size_string [ 128 ];
            char pool_wait_timeout_string [ 128 ];

            if ( !output_handle ) 
            {
                return SQL_ERROR;
            }

            if ( input_handle )
            {
                return SQL_INVALID_HANDLE;
            }

            /*
             * check connection pooling attributes
             */

            SQLGetPrivateProfileString( "ODBC", "Pooling", "0",
				pooling_string, sizeof( pooling_string ), 
                "ODBCINST.INI" );

            if ( pooling_string[ 0 ] == '1' ||
                toupper( pooling_string[ 0 ] ) == 'Y' ||
                ( toupper( pooling_string[ 0 ] ) == 'O' &&
                    toupper( pooling_string[ 1 ] ) == 'N' ))
            {
                pooling_enabled = 1;
            }

#ifdef WITH_SHARDENV
            if ( pooling_enabled )
            {
                int first;
            
                SQLGetPrivateProfileString( "ODBC", "PoolMaxSize", "0",
                    pool_max_size_string, sizeof( pool_max_size_string ),
                    "ODBCINST.INI" );
                pool_max_size = atoi( pool_max_size_string );

                SQLGetPrivateProfileString( "ODBC", "PoolWaitTimeout", "30",
                    pool_wait_timeout_string, sizeof( pool_wait_timeout_string ),
                    "ODBCINST.INI" );
                pool_wait_timeout = atoi( pool_wait_timeout_string );

                if ( !( environment = __share_env( &first )))
                {
                    *output_handle = SQL_NULL_HENV;
                    return SQL_ERROR;
                }
                *output_handle = (SQLHANDLE) environment;

                if ( first ) {
                    /*
                     * setup environment state
                     */

                    environment -> state = STATE_E1;
                    environment -> requested_version = requested_version;
                    environment -> version_set = !!requested_version;
                    environment -> sql_driver_count = -1;

                    /*
                     * if SQLAllocEnv is called then it's probable that
                     * the application wants ODBC2.X type behaviour
                     *
                     * In this case we don't need to set the version via
                     * SQLSetEnvAttr()
                     *
                     */

                    environment -> connection_count = 0;
                }
            }
            else {

                if ( !( environment = __alloc_env()))
                {
                    *output_handle = SQL_NULL_HENV;
                    return SQL_ERROR;
                }
                *output_handle = (SQLHANDLE) environment;
    
                /*
                * setup environment state
                */
    
                environment -> state = STATE_E1;
                environment -> requested_version = requested_version;
                environment -> version_set = !!requested_version;
        	    environment -> sql_driver_count = -1;
    
                /*
                * if SQLAllocEnv is called then it's probable that
                * the application wants ODBC2.X type behaviour
                *
                * In this case we don't need to set the version via
                * SQLSetEnvAttr()
                *
                */
    
                environment -> connection_count = 0;
           }
#else

            if ( pooling_enabled )
            {
                int first;
            
                SQLGetPrivateProfileString( "ODBC", "PoolMaxSize", "0",
                    pool_max_size_string, sizeof( pool_max_size_string ),
                    "ODBCINST.INI" );
                pool_max_size = atoi( pool_max_size_string );

                SQLGetPrivateProfileString( "ODBC", "PoolWaitTimeout", "30",
                    pool_wait_timeout_string, sizeof( pool_wait_timeout_string ),
                    "ODBCINST.INI" );
                pool_wait_timeout = atoi( pool_wait_timeout_string );
            }

            if ( !( environment = __alloc_env()))
            {
                *output_handle = SQL_NULL_HENV;
                return SQL_ERROR;
            }
            *output_handle = (SQLHANDLE) environment;

            /*
            * setup environment state
            */

            environment -> state = STATE_E1;
            environment -> requested_version = requested_version;
            environment -> version_set = !!requested_version;
            environment -> sql_driver_count = -1;

            /*
            * if SQLAllocEnv is called then it's probable that
            * the application wants ODBC2.X type behaviour
            *
            * In this case we don't need to set the version via
            * SQLSetEnvAttr()
            *
            */
    
            environment -> connection_count = 0;
#endif
    
           return SQL_SUCCESS;
        }
        break;

      case SQL_HANDLE_DBC:
        {
            DMHENV environment = (DMHENV) input_handle;
            DMHDBC connection;

            if ( !__validate_env( environment ))
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: SQL_INVALID_HANDLE" );

                return SQL_INVALID_HANDLE;
            }

            if ( output_handle )
                *output_handle = SQL_NULL_HDBC;

            thread_protect( SQL_HANDLE_ENV, environment );

            function_entry(( void * ) input_handle );

            if ( log_info.log_flag )
            { 
                /*
                 * log that we are here
                 */

                sprintf( environment -> msg, 
                        "\n\t\tEntry:\n\t\t\tHandle Type = %d\n\t\t\tInput Handle = %p",
                        handle_type,
                        (void*)input_handle );

                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        environment -> msg );
            }

            if ( !output_handle )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY009" );

                __post_internal_error( &environment -> error,
                        ERROR_HY009, NULL, 
                        SQL_OV_ODBC3 );

                return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
            }

            /*
             * check that a version has been requested
             */

            if ( environment -> requested_version == 0 )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY010" );

                __post_internal_error( &environment -> error,
                        ERROR_HY010, NULL,
                        SQL_OV_ODBC3 );

                *output_handle = SQL_NULL_HDBC;

                return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
            }

            connection = __alloc_dbc();
            if ( !connection )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY013" );

                __post_internal_error( &environment -> error,
                    ERROR_HY013, NULL,
                    environment -> requested_version );

                *output_handle = SQL_NULL_HDBC;

                return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
            }

            /*
             * sort out the states
             */

            connection -> state = STATE_C2;
            if ( environment -> state == STATE_E1 )
            {
                environment -> state = STATE_E2;
            }
            environment -> connection_count ++;
            connection -> environment = environment;

            connection -> cursors = SQL_CUR_DEFAULT;
            connection -> login_timeout = SQL_LOGIN_TIMEOUT_DEFAULT;
            connection -> login_timeout_set = 0;
            connection -> auto_commit = SQL_AUTOCOMMIT_ON;
            connection -> auto_commit_set = 0;
            connection -> async_enable = 0;
            connection -> async_enable_set = 0;
            connection -> auto_ipd = 0;
            connection -> auto_ipd_set = 0;
            connection -> connection_timeout = 0;
            connection -> connection_timeout_set = 0;
            connection -> metadata_id = 0;
            connection -> metadata_id_set = 0;
            connection -> packet_size = 0;
            connection -> packet_size_set = 0;
            connection -> quite_mode = 0;
            connection -> quite_mode_set = 0;
            connection -> txn_isolation = 0;
            connection -> txn_isolation_set = 0;
            strcpy( connection -> cli_year, "1995" );

            connection -> env_attribute.count = 0;
            connection -> env_attribute.list = NULL;
            connection -> dbc_attribute.count = 0;
            connection -> dbc_attribute.list = NULL;
            connection -> stmt_attribute.count = 0;
            connection -> stmt_attribute.list = NULL;
            connection -> save_attr = NULL;

#ifdef HAVE_ICONV
            connection -> iconv_cd_uc_to_ascii = (iconv_t)-1;
            connection -> iconv_cd_ascii_to_uc = (iconv_t)-1;
            strcpy( connection -> unicode_string, DEFAULT_ICONV_ENCODING );
#endif

            *output_handle = (SQLHANDLE) connection;

            if ( log_info.log_flag )
            {
                sprintf( environment -> msg, 
                        "\n\t\tExit:[SQL_SUCCESS]\n\t\t\tOutput Handle = %p",
                            connection );

                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        environment -> msg );
            }
#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
            uodbc_update_stats(environment->sh, UODBC_STATS_TYPE_HDBC,
                               (void *)1);
#endif

            thread_release( SQL_HANDLE_ENV, environment );
            return SQL_SUCCESS;
        }
        break;

      case SQL_HANDLE_STMT:
        {
            SQLRETURN ret, ret1;
            DMHDBC connection = (DMHDBC) input_handle;
            DMHSTMT statement;

            if ( !__validate_dbc( connection ))
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: SQL_INVALID_HANDLE" );

                return SQL_INVALID_HANDLE;
            }

            if ( output_handle )
                *output_handle = SQL_NULL_HSTMT;

            thread_protect( SQL_HANDLE_DBC, connection );

            function_entry(( void * ) input_handle );

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg, 
                    "\n\t\tEntry:\n\t\t\tHandle Type = %d\n\t\t\tInput Handle = %p",
                    handle_type,
                    (void*)input_handle );

                dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    connection -> msg );
            }

            if ( !output_handle )
            {
                dm_log_write( __FILE__, 
                       __LINE__, 
                       LOG_INFO, 
                       LOG_INFO, 
                       "Error: HY009" );

                __post_internal_error( &connection -> error,
                        ERROR_HY009, NULL, 
                        connection -> environment -> requested_version  );

                return function_return_nodrv( SQL_HANDLE_DBC, connection , SQL_ERROR );
            }

            if ( connection -> state == STATE_C1 ||
                    connection -> state == STATE_C2 ||
                    connection -> state == STATE_C3 )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: 08003" );

                __post_internal_error( &connection -> error,
                        ERROR_08003, NULL,
                        connection -> environment -> requested_version );

                *output_handle = SQL_NULL_HSTMT;

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }

            statement = __alloc_stmt();
            if ( !statement )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY013" );

                __post_internal_error( &connection -> error,
                        ERROR_HY013, NULL,
                        connection -> environment -> requested_version );

                *output_handle = SQL_NULL_HSTMT;

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }

            /*
             * pass the call on
             */

            if ( requested_version >= SQL_OV_ODBC3 )
            {
                if ( CHECK_SQLALLOCHANDLE( connection ))
                {
                    ret = SQLALLOCHANDLE( connection,
                            SQL_HANDLE_STMT,
                            connection -> driver_dbc,
                            &statement -> driver_stmt,
                            statement );

                    if ( !SQL_SUCCEEDED( ret ))
                        __release_stmt( statement );
                }
                else if ( CHECK_SQLALLOCSTMT( connection ))
                {
                    ret = SQLALLOCSTMT( connection,
                            connection -> driver_dbc,
                            &statement -> driver_stmt,
                            statement );

                    if ( !SQL_SUCCEEDED( ret ))
                        __release_stmt( statement );
                }
                else
                {
                    dm_log_write( __FILE__, 
                            __LINE__, 
                            LOG_INFO, 
                            LOG_INFO, 
                            "Error: IM003" );

                    __post_internal_error( &connection -> error,
                            ERROR_IM003, NULL,
                            connection -> environment -> requested_version );

                    __release_stmt( statement );

                    *output_handle = SQL_NULL_HSTMT;

                    return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
                }
            }
            else
            {
                if ( CHECK_SQLALLOCSTMT( connection ))
                {
                    ret = SQLALLOCSTMT( connection,
                            connection -> driver_dbc,
                            &statement -> driver_stmt,
                            statement );

                    if ( !SQL_SUCCEEDED( ret ))
                        __release_stmt( statement );
                }
                else if ( CHECK_SQLALLOCHANDLE( connection ))
                {
                    ret = SQLALLOCHANDLE( connection,
                            SQL_HANDLE_STMT,
                            connection -> driver_dbc,
                            &statement -> driver_stmt,
                            statement );

                    if ( !SQL_SUCCEEDED( ret ))
                        __release_stmt( statement );
                }
                else
                {
                    dm_log_write( __FILE__, 
                            __LINE__, 
                            LOG_INFO, 
                            LOG_INFO, 
                            "Error: IM003" );

                    __post_internal_error( &connection -> error,
                            ERROR_IM003, NULL,
                            connection -> environment -> requested_version );

                    __release_stmt( statement );

                    *output_handle = SQL_NULL_HSTMT;
                
                    return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
                }
            }

            if ( SQL_SUCCEEDED( ret ))
            {
                /*
                 * sort out the states
                 */

                statement -> state = STATE_S1;
                if ( connection -> state == STATE_C4 )
                    connection -> state = STATE_C5;

                __register_stmt ( connection, statement );

                *output_handle = (SQLHANDLE) statement;

                statement -> metadata_id = SQL_FALSE;

                /*
                 * if we are connected to a 3 driver then
                 * we need to get the 4 implicit descriptors
                 * so we know that they are valid
                 */

                if ( connection -> driver_act_ver == 3 &&
                        CHECK_SQLGETSTMTATTR( connection ))
                {
                    DRV_SQLHDESC desc;

                    /*
                     * ARD
                     */

                    ret1 = SQLGETSTMTATTR( connection,
                            statement -> driver_stmt,
                            SQL_ATTR_APP_ROW_DESC,
                            &desc,
                            sizeof( desc ),
                            NULL );

                    if ( SQL_SUCCEEDED( ret1 ))
                    {
                        /*
                         * allocate one of our descriptors
                         * to wrap around this
                         */
                        statement -> ard = __alloc_desc();
                        if ( !statement -> ard )
                        {
                            dm_log_write( __FILE__, 
                                __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY013" );

                            __post_internal_error( &connection -> error,
                                    ERROR_HY013, NULL,
                                    connection -> environment -> requested_version );

                            __release_stmt( statement );

                            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R1 );
                        }
                        statement -> implicit_ard = statement -> ard;
                        statement -> ard -> implicit = 1;
                        statement -> ard -> associated_with = statement;
                        statement -> ard -> state = STATE_D1i;
                        statement -> ard -> driver_desc = desc;
                        statement -> ard -> connection = connection;
                    }

                    /*
                     * APD
                     */

                    ret1 = SQLGETSTMTATTR( connection,
                            statement -> driver_stmt,
                            SQL_ATTR_APP_PARAM_DESC,
                            &desc,
                            sizeof( desc ),
                            NULL );

                    if ( SQL_SUCCEEDED( ret1 ))
                    {
                        /*
                         * allocate one of our descriptors
                         * to wrap around this
                         */
                        statement -> apd = __alloc_desc();
                        if ( !statement -> apd )
                        {
                            dm_log_write( __FILE__, 
                                __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY013" );

                            __post_internal_error( &connection -> error,
                                    ERROR_HY013, NULL,
                                    connection -> environment -> requested_version );
        
                            __release_stmt( statement );

                            *output_handle = SQL_NULL_HSTMT;

                            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R1 );
                        }
                        statement -> implicit_apd = statement -> apd;
                        statement -> apd -> implicit = 1;
                        statement -> apd -> associated_with = statement;
                        statement -> apd -> state = STATE_D1i;
                        statement -> apd -> driver_desc = desc;
                        statement -> apd -> connection = connection;
                    }

                    /*
                     * IRD
                     */

                    ret1 = SQLGETSTMTATTR( connection,
                            statement -> driver_stmt,
                            SQL_ATTR_IMP_ROW_DESC,
                            &desc,
                            sizeof( desc ),
                            NULL );

                    if ( SQL_SUCCEEDED( ret1 ))
                    {
                        /*
                         * allocate one of our descriptors
                         * to wrap around this
                         */
                        statement -> ird = __alloc_desc();
                        if ( !statement -> ird )
                        {
                            dm_log_write( __FILE__, 
                                __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY013" );

                            __post_internal_error( &connection -> error,
                                    ERROR_HY013, NULL, 
                                    connection -> environment -> requested_version );

                            __release_stmt( statement );

                            *output_handle = SQL_NULL_HSTMT;

                            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R1 );
                        }
                        statement -> implicit_ird = statement -> ird;
                        statement -> ird -> implicit = 1;
                        statement -> ird -> associated_with = statement;
                        statement -> ird -> state = STATE_D1i;
                        statement -> ird -> driver_desc = desc;
                        statement -> ird -> connection = connection;
                    }

                    /*
                     * IPD
                     */

                    ret1 = SQLGETSTMTATTR( connection,
                            statement -> driver_stmt,
                            SQL_ATTR_IMP_PARAM_DESC,
                            &desc,
                            sizeof( desc ),
                            NULL );

                    if ( SQL_SUCCEEDED( ret1 ))
                    {
                        /*
                         * allocate one of our descriptors
                         * to wrap around this
                         */
                        statement -> ipd = __alloc_desc();
                        if ( !statement -> ipd )
                        {
                            dm_log_write( __FILE__, 
                                __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY013" );

                            __post_internal_error( &connection -> error,
                                    ERROR_HY013, NULL,
                                    connection -> environment -> requested_version );

                            __release_stmt( statement );

                            *output_handle = SQL_NULL_HSTMT;

                            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R1 );
                        }
                        statement -> implicit_ipd = statement -> ipd;
                        statement -> ipd -> implicit = 1;
                        statement -> ipd -> associated_with = statement;
                        statement -> ipd -> state = STATE_D1i;
                        statement -> ipd -> driver_desc = desc;
                        statement -> ipd -> connection = connection;
                    }
                }
                /* Driver may only have unicode API's */
                else if ( CHECK_SQLGETSTMTATTRW( connection ))
                {
                    DRV_SQLHDESC desc;

                    /*
                     * ARD
                     */

                    ret1 = SQLGETSTMTATTRW( connection,
                            statement -> driver_stmt,
                            SQL_ATTR_APP_ROW_DESC,
                            &desc,
                            sizeof( desc ),
                            NULL );

                    if ( SQL_SUCCEEDED( ret1 ))
                    {
                        /*
                         * allocate one of our descriptors
                         * to wrap around this
                         */
                        statement -> ard = __alloc_desc();
                        if ( !statement -> ard )
                        {
                            dm_log_write( __FILE__, 
                                __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY013" );

                            __post_internal_error( &connection -> error,
                                    ERROR_HY013, NULL,
                                    connection -> environment -> requested_version );

                            __release_stmt( statement );

                            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R1 );
                        }
                        statement -> implicit_ard = statement -> ard;
                        statement -> ard -> implicit = 1;
                        statement -> ard -> associated_with = statement;
                        statement -> ard -> state = STATE_D1i;
                        statement -> ard -> driver_desc = desc;
                        statement -> ard -> connection = connection;
                    }

                    /*
                     * APD
                     */

                    ret1 = SQLGETSTMTATTRW( connection,
                            statement -> driver_stmt,
                            SQL_ATTR_APP_PARAM_DESC,
                            &desc,
                            sizeof( desc ),
                            NULL );

                    if ( SQL_SUCCEEDED( ret1 ))
                    {
                        /*
                         * allocate one of our descriptors
                         * to wrap around this
                         */
                        statement -> apd = __alloc_desc();
                        if ( !statement -> apd )
                        {
                            dm_log_write( __FILE__, 
                                __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY013" );

                            __post_internal_error( &connection -> error,
                                    ERROR_HY013, NULL,
                                    connection -> environment -> requested_version );
        
                            __release_stmt( statement );

                            *output_handle = SQL_NULL_HSTMT;

                            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R1 );
                        }
                        statement -> implicit_apd = statement -> apd;
                        statement -> apd -> implicit = 1;
                        statement -> apd -> associated_with = statement;
                        statement -> apd -> state = STATE_D1i;
                        statement -> apd -> driver_desc = desc;
                        statement -> apd -> connection = connection;
                    }

                    /*
                     * IRD
                     */

                    ret1 = SQLGETSTMTATTRW( connection,
                            statement -> driver_stmt,
                            SQL_ATTR_IMP_ROW_DESC,
                            &desc,
                            sizeof( desc ),
                            NULL );

                    if ( SQL_SUCCEEDED( ret1 ))
                    {
                        /*
                         * allocate one of our descriptors
                         * to wrap around this
                         */
                        statement -> ird = __alloc_desc();
                        if ( !statement -> ird )
                        {
                            dm_log_write( __FILE__, 
                                __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY013" );

                            __post_internal_error( &connection -> error,
                                    ERROR_HY013, NULL, 
                                    connection -> environment -> requested_version );

                            __release_stmt( statement );

                            *output_handle = SQL_NULL_HSTMT;

                            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R1 );
                        }
                        statement -> implicit_ird = statement -> ird;
                        statement -> ird -> implicit = 1;
                        statement -> ird -> associated_with = statement;
                        statement -> ird -> state = STATE_D1i;
                        statement -> ird -> driver_desc = desc;
                        statement -> ird -> connection = connection;
                    }

                    /*
                     * IPD
                     */

                    ret1 = SQLGETSTMTATTRW( connection,
                            statement -> driver_stmt,
                            SQL_ATTR_IMP_PARAM_DESC,
                            &desc,
                            sizeof( desc ),
                            NULL );

                    if ( SQL_SUCCEEDED( ret1 ))
                    {
                        /*
                         * allocate one of our descriptors
                         * to wrap around this
                         */
                        statement -> ipd = __alloc_desc();
                        if ( !statement -> ipd )
                        {
                            dm_log_write( __FILE__, 
                                __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY013" );

                            __post_internal_error( &connection -> error,
                                    ERROR_HY013, NULL,
                                    connection -> environment -> requested_version );

                            __release_stmt( statement );

                            *output_handle = SQL_NULL_HSTMT;

                            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R1 );
                        }
                        statement -> implicit_ipd = statement -> ipd;
                        statement -> ipd -> implicit = 1;
                        statement -> ipd -> associated_with = statement;
                        statement -> ipd -> state = STATE_D1i;
                        statement -> ipd -> driver_desc = desc;
                        statement -> ipd -> connection = connection;
                    }
                } 
            }

            /*
             * set any preset statement attributes
             */

            if ( SQL_SUCCEEDED( ret ))
            {
                __set_attributes( statement, SQL_HANDLE_STMT );
            }

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg, 
                        "\n\t\tExit:[SQL_SUCCESS]\n\t\t\tOutput Handle = %p",
                        statement );

                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        connection -> msg );
            }
#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
            uodbc_update_stats(connection->environment->sh,
                               UODBC_STATS_TYPE_HSTMT, (void *)1);
#endif

            return function_return( SQL_HANDLE_DBC, connection, ret, DEFER_R1 );
        }
        break;

      case SQL_HANDLE_DESC:
        {
        SQLRETURN ret;
        DMHDBC connection = (DMHDBC) input_handle;
        DMHDESC descriptor;

        if ( !__validate_dbc( connection ))
        {
            dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: SQL_INVALID_HANDLE" );

            return SQL_INVALID_HANDLE;
        }

        if ( output_handle )
            *output_handle = SQL_NULL_HDESC;

        thread_protect( SQL_HANDLE_DBC, connection );

        function_entry(( void * ) input_handle );

        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, 
                "\n\t\tEntry:\n\t\t\tHandle Type = %d\n\t\t\tInput Handle = %p",
                handle_type,
                (void*)input_handle );

            dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
        }

        if ( !output_handle )
        {
            dm_log_write( __FILE__, 
                   __LINE__, 
                   LOG_INFO, 
                   LOG_INFO, 
                   "Error: HY009" );

            __post_internal_error( &connection -> error,
                    ERROR_HY009, NULL, 
                    connection -> environment -> requested_version  );

            return function_return_nodrv( SQL_HANDLE_DBC, connection , SQL_ERROR );
        }

        if ( connection -> state == STATE_C1 ||
                connection -> state == STATE_C2 ||
                connection -> state == STATE_C3 )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: 08003" );

            __post_internal_error( &connection -> error,
                    ERROR_08003, NULL,
                    connection -> environment -> requested_version );

            *output_handle = SQL_NULL_HDESC;

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }

        descriptor = __alloc_desc();
        if ( !descriptor )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY013" );

            __post_internal_error( &connection -> error,
                    ERROR_HY013, NULL,
                    connection -> environment -> requested_version );

            *output_handle = SQL_NULL_HDESC;

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }

        /*
         * pass the call on
         */

        if ( CHECK_SQLALLOCHANDLE( connection ))
        {
            ret = SQLALLOCHANDLE( connection,
                    SQL_HANDLE_DESC,
                    connection -> driver_dbc,
                    &descriptor -> driver_desc,
                    NULL );

            if ( !SQL_SUCCEEDED( ret ))
                __release_desc( descriptor );
        }
        else
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM003" );

            __post_internal_error( &connection -> error,
                    ERROR_IM003, NULL,
                    connection -> environment -> requested_version );

            __release_desc( descriptor );

            *output_handle = SQL_NULL_HDESC;

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }

        if ( SQL_SUCCEEDED( ret ))
        {
            /*
             * sort out the states
             */

            descriptor -> state = STATE_D1e;
            descriptor -> implicit = 0;
            descriptor -> associated_with = NULL;

            connection -> statement_count ++;

            descriptor -> connection = connection;

            *output_handle = (SQLHANDLE) descriptor;
        }

        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, 
                    "\n\t\tExit:[SQL_SUCCESS]\n\t\t\tOutput Handle = %p",
                    descriptor );

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    connection -> msg );
        }
#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
        uodbc_update_stats(connection->environment->sh, UODBC_STATS_TYPE_HDESC,
                           (void *)1);
#endif

        return function_return( SQL_HANDLE_DBC, connection, ret, DEFER_R1 );
        }
        break;

      default:
		if ( __validate_env( (DMHENV) input_handle ))
		{
			DMHENV environment = (DMHENV) input_handle;

            thread_protect( SQL_HANDLE_ENV, environment );

			__post_internal_error( &environment -> error,
						ERROR_HY092, NULL,
						environment -> requested_version );

			return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
		}
		else if ( __validate_dbc( (DMHDBC) input_handle ))
		{
			DMHDBC connection = (DMHDBC) input_handle;

            thread_protect( SQL_HANDLE_DBC, connection );

			__post_internal_error( &connection -> error,
					ERROR_HY092, NULL,
					connection -> environment -> requested_version );
	
			return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
		}
		else
		{
			return SQL_INVALID_HANDLE;
		}
		break;
    }
}

SQLRETURN SQLAllocHandle( SQLSMALLINT handle_type,
           SQLHANDLE input_handle,
           SQLHANDLE *output_handle )
{
    /*
     * setting a requested version to 0
     * indicates that we are ODBC3 and the application must
     * select ODBC2 or 3 explicitly
     */

    return __SQLAllocHandle( handle_type,
            input_handle,
            output_handle,
            0 );
}
