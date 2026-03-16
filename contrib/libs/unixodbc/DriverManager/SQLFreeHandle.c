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
 * $Id: SQLFreeHandle.c,v 1.12 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLFreeHandle.c,v $
 * Revision 1.12  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.11  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.10  2007/12/17 13:13:03  lurcher
 * Fix a couple of descriptor typo's
 *
 * Revision 1.9  2007/01/02 10:27:50  lurcher
 * Fix descriptor leak with unicode only driver
 *
 * Revision 1.8  2006/04/18 10:24:47  lurcher
 * Add a couple of changes from Mark Vanderwiel
 *
 * Revision 1.7  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.6  2003/05/14 09:42:25  lurcher
 *
 * Fix bug in stats collection
 *
 * Revision 1.5  2003/04/09 08:42:18  lurcher
 *
 * Allow setting of odbcinstQ lib from odbcinst.ini and Environment
 *
 * Revision 1.4  2002/09/18 14:49:32  lurcher
 *
 * DataManagerII additions and some more threading fixes
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.7  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 * Revision 1.6  2001/06/04 15:24:49  nick
 *
 * Add port to MAC OSX and QT3 changes
 *
 * Revision 1.5  2001/05/15 13:33:44  jason
 *
 * Wrapped calls to stats with COLLECT_STATS
 *
 * Revision 1.4  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.3  2000/12/19 10:28:29  martin
 *
 * Return "not built with stats" in uodbc_error() if stats function called
 * 	when stats not built.
 * Add uodbc_update_stats() calls to SQLFreeHandle.
 *
 * Revision 1.2  2000/11/23 09:43:29  nick
 *
 * Fix deadlock posibility
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.12  2000/06/27 17:34:10  ngorham
 *
 * Fix a problem when the second part of the connect failed a seg fault
 * was generated in the error reporting
 *
 * Revision 1.11  2000/05/21 21:49:19  ngorham
 *
 * Assorted fixes
 *
 * Revision 1.10  1999/11/17 21:11:59  ngorham
 *
 * Fix bug where the check for a valid handle was after the code had
 * used it.
 *
 * Revision 1.9  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.8  1999/10/24 23:54:18  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.7  1999/10/09 00:15:58  ngorham
 *
 * Add mapping from SQL_TYPE_X to SQL_X and SQL_C_TYPE_X to SQL_C_X
 * when the driver is a ODBC 2 one
 *
 * Revision 1.6  1999/09/21 22:34:24  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.5  1999/08/03 21:47:39  shandyb
 * Moving to automake: changed files in DriverManager
 *
 * Revision 1.4  1999/07/10 21:10:16  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.3  1999/07/04 21:05:07  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/30 23:56:55  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:07  sShandyb
 * first go at it
 *
 * Revision 1.2  1999/06/07 01:29:31  pharvey
 * *** empty log message ***
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.5  1999/05/09 23:27:11  nick
 * All the API done now
 *
 * Revision 1.4  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.3  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.2  1999/04/29 20:47:37  nick
 * Another checkpoint
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
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

static char const rcsid[]= "$RCSfile: SQLFreeHandle.c,v $ $Revision: 1.12 $";

extern int pooling_enabled;

SQLRETURN __SQLFreeHandle( SQLSMALLINT handle_type,
        SQLHANDLE handle )
{
    switch( handle_type )
    {
      case SQL_HANDLE_ENV:
      case SQL_HANDLE_SENV:
        {
            DMHENV environment = (DMHENV)handle;

            /*
             * check environment, the mark_released addition is to catch what seems to be a 
             * race error in SQLAPI where it uses a env handle in one thread while its being released
             * in another. releasing the handle at the end of this function is not fast enough for 
             * the normal validation process to catch it.
             */

            if ( !__validate_env_mark_released( environment ))
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: SQL_INVALID_HANDLE" );

                return SQL_INVALID_HANDLE;
            }
            
            function_entry( environment );

            if ( log_info.log_flag )
            {
                sprintf( environment -> msg,
                        "\n\t\tEntry:\n\t\t\tHandle Type = %d\n\t\t\tInput Handle = %p",
                        handle_type,
                        (void*)handle );

                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        environment -> msg );
            }

            thread_protect( SQL_HANDLE_ENV, environment );

#ifdef WITH_SHARDENV
            if ( pooling_enabled == 0 ) {
                /*
                 * check states
                 */
                if ( environment -> state != STATE_E1 )
                {
                    dm_log_write( __FILE__,
                            __LINE__,
                            LOG_INFO,
                            LOG_INFO,
                            "Error: HY010" );
    
                    __post_internal_error( &environment -> error,
                            ERROR_HY010, NULL,
                            environment -> requested_version );
    
                    return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
                }
            }
#else
            /*
             * check states
             */
            if ( environment -> state != STATE_E1 )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: HY010" );

                __post_internal_error( &environment -> error,
                        ERROR_HY010, NULL,
                        environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
            }
#endif

            thread_release( SQL_HANDLE_ENV, environment );

#ifdef WITH_SHARDENV
            if ( pooling_enabled == 0 ) {
                /*
                 * release any pooled connections that are using this environment
                 */
                __strip_from_pool( environment );
            }
#else
            __strip_from_pool( environment );
#endif

            __release_env( environment );

            return SQL_SUCCESS;
        }
        break;

      case SQL_HANDLE_DBC:
        {
            DMHDBC connection = (DMHDBC)handle;
            DMHENV environment;

            /*
             * check connection
             */

            if ( !__validate_dbc( connection ))
            {
                dm_log_write( __FILE__, 
                            __LINE__, 
                            LOG_INFO, 
                            LOG_INFO, 
                            "Error: SQL_INVALID_HANDLE" );

                return SQL_INVALID_HANDLE;
            }

            function_entry( connection );

            environment = connection -> environment;

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg,
                        "\n\t\tEntry:\n\t\t\tHandle Type = %d\n\t\t\tInput Handle = %p",
                        handle_type,
                        (void*)handle );

                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        connection -> msg );
            }

            thread_protect( SQL_HANDLE_ENV, environment );

            /*
             * check states
             */
            if ( connection -> state != STATE_C2 )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: HY010" );

                __post_internal_error( &connection -> error,
                        ERROR_HY010, NULL,
                        connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
            }

            environment -> connection_count --;

            if ( environment -> connection_count == 0 )
            {
                environment -> state = STATE_E1;
            }

            environment = connection -> environment;

            __release_attr_str( &connection -> env_attribute );
            __release_attr_str( &connection -> dbc_attribute );
            __release_attr_str( &connection -> stmt_attribute );

            __disconnect_part_one( connection );

            __release_dbc( connection );

            if ( log_info.log_flag )
            {
                sprintf( environment -> msg,
                        "\n\t\tExit:[SQL_SUCCESS]" );

                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        environment -> msg );
            }
#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
            uodbc_update_stats(environment->sh, UODBC_STATS_TYPE_HDBC,
                               (void *)-1);
#endif

            thread_release( SQL_HANDLE_ENV, environment );

            return SQL_SUCCESS;
        }
        break;

      case SQL_HANDLE_STMT:
        {
            DMHSTMT statement = (DMHSTMT)handle;
            DMHDBC connection;
            SQLRETURN ret;

            /*
             * check statement
             */
            if ( !__validate_stmt( statement ))
            {
                dm_log_write( __FILE__, 
                            __LINE__, 
                            LOG_INFO, 
                            LOG_INFO, 
                            "Error: SQL_INVALID_HANDLE" );

                return SQL_INVALID_HANDLE;
            }

            function_entry( statement );

            connection = statement -> connection;

            if ( log_info.log_flag )
            {
                sprintf( statement -> msg,
                        "\n\t\tEntry:\n\t\t\tHandle Type = %d\n\t\t\tInput Handle = %p",
                        handle_type,
                        (void*)handle );

                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        statement -> msg );
            }

            thread_protect( SQL_HANDLE_STMT, statement );

            /*
             * check states
             */
            if ( statement -> state == STATE_S8 ||
                    statement -> state == STATE_S9 ||
                    statement -> state == STATE_S10 ||
                    statement -> state == STATE_S11 ||
                    statement -> state == STATE_S12 ||
                    statement -> state == STATE_S13 ||
                    statement -> state == STATE_S14 ||
                    statement -> state == STATE_S15 )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: HY010" );

                __post_internal_error( &statement -> error,
                          ERROR_HY010, NULL,
                          statement -> connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
            }

            if ( !CHECK_SQLFREEHANDLE( statement -> connection ))
            {
                if ( !CHECK_SQLFREESTMT( statement -> connection ))
                {
                    dm_log_write( __FILE__,
                            __LINE__,
                            LOG_INFO,
                            LOG_INFO,
                            "Error: IM001" );

                    __post_internal_error( &statement -> error,
                            ERROR_IM001, NULL,
                            statement -> connection -> environment -> requested_version );

                    return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
                }
                else
                {
                    ret = SQLFREESTMT( statement -> connection,
                        statement -> driver_stmt,
                        SQL_DROP );
                }
            }
            else
            {
                ret = SQLFREEHANDLE( statement -> connection,
                        handle_type,
                        statement -> driver_stmt );
            }

            if ( SQL_SUCCEEDED( ret ))
            {
                /*
                 * release the implicit descriptors, 
				 * this matches the tests in SQLAllocHandle
                 */
                if (( statement -> connection -> driver_act_ver == 3 &&
						CHECK_SQLGETSTMTATTR( connection )) ||
						CHECK_SQLGETSTMTATTRW( connection ))
                {
                    if ( statement -> implicit_ard )
                        __release_desc( statement -> implicit_ard );
                    if ( statement -> implicit_apd )
                        __release_desc( statement -> implicit_apd );
                    if ( statement -> implicit_ird )
                        __release_desc( statement -> implicit_ird );
                    if ( statement -> implicit_ipd )
                        __release_desc( statement -> implicit_ipd );
                }
                statement -> connection -> statement_count --;

                thread_release( SQL_HANDLE_STMT, statement );
#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
                uodbc_update_stats(connection->environment->sh,
                                   UODBC_STATS_TYPE_HSTMT, (void *)-1);
#endif

                __release_stmt( statement );
            }
            else
            {
                thread_release( SQL_HANDLE_STMT, statement );
            }

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg,
                        "\n\t\tExit:[SQL_SUCCESS]" );

                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        connection -> msg );
            }

            return function_return( IGNORE_THREAD, connection, ret, DEFER_R0 );
        }
        break;

      case SQL_HANDLE_DESC:
        {
            DMHDESC descriptor = (DMHDESC)handle;
            DMHDBC connection;
            SQLRETURN ret;

            /*
             * check descriptor
             */
            if ( !__validate_desc( descriptor ))
            {
                return SQL_INVALID_HANDLE;
            }

            function_entry( descriptor );

            connection = descriptor -> connection;

            if ( log_info.log_flag )
            {
                sprintf( descriptor -> msg,
                        "\n\t\tEntry:\n\t\t\tHandle Type = %d\n\t\t\tInput Handle = %p",
                        handle_type,
                        (void*)handle );

                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        descriptor -> msg );
            }

			if ( descriptor -> implicit )
			{
				dm_log_write( __FILE__,
						__LINE__,
						LOG_INFO,
						LOG_INFO,
						"Error: HY017" );
		
				__post_internal_error( &descriptor -> error,
						ERROR_HY017, NULL,
						connection -> environment -> requested_version );
		
				return function_return_nodrv( IGNORE_THREAD, descriptor, SQL_ERROR );
			}
		
            thread_protect( SQL_HANDLE_DESC, descriptor );

            if ( !CHECK_SQLFREEHANDLE( connection ))
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: IM001" );

                __post_internal_error( &descriptor -> error,
                        ERROR_IM001, NULL,
                        connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_DESC, descriptor, SQL_ERROR );
            }
            else
            {
                ret = SQLFREEHANDLE( connection,
                        handle_type,
                        descriptor -> driver_desc );
            }

            /*
             * check status of statements associated with this descriptor
             */

            if( __check_stmt_from_desc( descriptor, STATE_S8 ) ||
                __check_stmt_from_desc( descriptor, STATE_S9 ) ||
                __check_stmt_from_desc( descriptor, STATE_S10 ) ||
                __check_stmt_from_desc( descriptor, STATE_S11 ) ||
                __check_stmt_from_desc( descriptor, STATE_S12 )) {

                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY010" );

                __post_internal_error( &descriptor -> error,
                        ERROR_HY010, NULL,
                        descriptor -> connection -> environment -> requested_version );

                return function_return( SQL_HANDLE_DESC, descriptor, SQL_ERROR, DEFER_R0 );
            }

            thread_release( SQL_HANDLE_DESC, descriptor );

            __release_desc( descriptor );

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg,
                        "\n\t\tExit:[SQL_SUCCESS]" );

                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        connection -> msg );
            }
#if defined ( COLLECT_STATS ) && defined( HAVE_SYS_SEM_H )
            uodbc_update_stats(connection->environment->sh,
                               UODBC_STATS_TYPE_HDESC, (void *)-1);
#endif

            return function_return( IGNORE_THREAD, connection, SQL_SUCCESS, DEFER_R0 );
        }
        break;

      default:
        /*
         * there is nothing to report a error on
         */
        return SQL_INVALID_HANDLE;
    }
}

SQLRETURN SQLFreeHandle( SQLSMALLINT handle_type,
        SQLHANDLE handle )
{
    return __SQLFreeHandle( handle_type,
            handle );
}

