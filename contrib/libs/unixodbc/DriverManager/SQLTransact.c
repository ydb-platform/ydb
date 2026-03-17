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
 * $Id: SQLTransact.c,v 1.11 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLTransact.c,v $
 * Revision 1.11  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.10  2007/08/30 12:54:17  lurcher
 * Add -3 option to isql to use ODBC3 calls
 *
 * Revision 1.9  2006/05/31 17:35:34  lurcher
 * Add unicode ODBCINST entry points
 *
 * Revision 1.8  2004/06/16 14:42:03  lurcher
 *
 *
 * Fix potential corruption with threaded use and SQLEndTran
 *
 * Revision 1.7  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.6  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.5  2002/09/18 14:49:32  lurcher
 *
 * DataManagerII additions and some more threading fixes
 *
 * Revision 1.3  2002/08/20 12:41:07  lurcher
 *
 * Fix incorrect return state from SQLEndTran/SQLTransact
 *
 * Revision 1.2  2002/08/12 16:20:44  lurcher
 *
 * Make it try and find a working iconv set of encodings
 *
 * Revision 1.1.1.1  2001/10/17 16:40:07  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.10  2000/08/16 15:57:51  ngorham
 *
 * Fix bug where it falled if called in state C4
 *
 * Revision 1.9  1999/11/13 23:41:01  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.8  1999/10/24 23:54:19  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.7  1999/10/20 19:45:15  ngorham
 *
 * Added fix to SQLTransact in the DM
 *
 * Revision 1.6  1999/09/21 22:34:26  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.5  1999/07/10 21:10:17  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.4  1999/07/04 21:05:08  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.3  1999/06/30 23:56:56  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.2  1999/06/19 17:51:40  ngorham
 *
 * Applied assorted minor bug fixes
 *
 * Revision 1.1.1.1  1999/05/29 13:41:09  sShandyb
 * first go at it
 *
 * Revision 1.3  1999/06/02 20:12:10  ngorham
 *
 * Fixed botched log entry, and removed the dos \r from the sql header files.
 *
 * Revision 1.2  1999/06/02 19:57:21  ngorham
 *
 * Added code to check if a attempt is being made to compile with a C++
 * Compiler, and issue a message.
 * Start work on the ODBC2-3 conversions.
 *
 * Revision 1.1.1.1  1999/05/27 18:23:18  pharvey
 * Imported sources
 *
 * Revision 1.2  1999/05/09 23:27:11  nick
 * All the API done now
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLTransact.c,v $ $Revision: 1.11 $";

SQLRETURN SQLTransact( SQLHENV environment_handle,
           SQLHDBC connection_handle,
           SQLUSMALLINT completion_type )
{
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

    /*
     * check either handle first
     */

    if ( connection_handle != SQL_NULL_HDBC )
    {
        DMHDBC connection = (DMHDBC) connection_handle;

        if ( !__validate_dbc( connection ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

            return SQL_INVALID_HANDLE;
        }
    }

    if ( environment_handle != SQL_NULL_HENV )
    {
        DMHENV environment = (DMHENV) environment_handle;

        if ( !__validate_env( environment ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

            return SQL_INVALID_HANDLE;
        }
    }

    if ( connection_handle != SQL_NULL_HDBC )
    {
        DMHDBC connection = (DMHDBC) connection_handle;
        SQLRETURN ret;

        function_entry( connection );

        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, "\n\t\tEntry:\
\n\t\t\tEnvironment = %p\
\n\t\t\tConnection = %p\
\n\t\t\tCompletion Type = %d",
                    (void*)environment_handle,
                    (void*)connection_handle,
                    (int)completion_type );

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    connection -> msg );
        }

        thread_protect( SQL_HANDLE_DBC, connection );

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

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }

        /*
         * check status of statements belonging to this connection
         */

        if( __check_stmt_from_dbc_v( connection, 8, STATE_S8, STATE_S9, STATE_S10, STATE_S11, STATE_S12, STATE_S13, STATE_S14, STATE_S15 )) {

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY010" );

            __post_internal_error( &connection -> error,
                    ERROR_HY010, NULL,
                    connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }

        if ( completion_type != SQL_COMMIT &&
                completion_type != SQL_ROLLBACK )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY012" );

            __post_internal_error( &connection -> error,
                    ERROR_HY012, NULL,
                    connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }

        if ( CHECK_SQLTRANSACT( connection ))
        {
            ret = SQLTRANSACT( connection,
                    SQL_NULL_HENV,
                    connection -> driver_dbc,
                    completion_type );
        }
        else if ( CHECK_SQLENDTRAN( connection ))
        {
            ret = SQLENDTRAN( connection,
                    SQL_HANDLE_DBC,
                    connection -> driver_dbc,
                    completion_type );
        }
        else
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM001" );

            __post_internal_error( &connection -> error,
                    ERROR_IM001, NULL,
                    connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }

        if( SQL_SUCCEEDED(ret) && connection -> auto_commit == SQL_AUTOCOMMIT_OFF )
        {
            SQLSMALLINT cb_value;
            SQLSMALLINT cb_value_length = sizeof(SQLSMALLINT);
            SQLRETURN ret1;
	    
            /*
             * for each statement belonging to this connection set its state 
             * relative to the commit or rollback behavior
             */

			if ( connection -> cbs_found == 0 ) 
			{
            	/* release thread so we can get the info */
            	thread_release( SQL_HANDLE_DBC, connection );
            
				ret1 = SQLGetInfo(connection, 
                      	SQL_CURSOR_COMMIT_BEHAVIOR, 
                      	&connection -> ccb_value, 
                      	sizeof( SQLSMALLINT ),
                      	&cb_value_length);

				if ( SQL_SUCCEEDED( ret1 ))
				{
					ret1 = SQLGetInfo(connection, 
                      	SQL_CURSOR_ROLLBACK_BEHAVIOR,
                      	&connection -> crb_value, 
                      	sizeof( SQLSMALLINT ), 
                      	&cb_value_length);
				}

            	/* protect thread again */
            	thread_protect( SQL_HANDLE_DBC, connection );
				if ( SQL_SUCCEEDED( ret1 ))
				{
					connection -> cbs_found = 1;
				}
			}

            if( completion_type == SQL_COMMIT )
			{
				cb_value = connection -> ccb_value;
			}
			else
			{
				cb_value = connection -> crb_value;
			}

            if( connection -> cbs_found )
            {
                __set_stmt_state( connection, cb_value );
            }
        }

        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, 
                    "\n\t\tExit:[%s]",
                        __get_return_status( ret, s1 ));

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    connection -> msg );
        }

        return function_return( SQL_HANDLE_DBC, connection, ret, DEFER_R0 );
    }
    else if ( environment_handle != SQL_NULL_HENV )
    {
        DMHENV environment = (DMHENV) environment_handle;
        DMHDBC connection;
        SQLRETURN ret;

        function_entry( environment );

        if ( log_info.log_flag )
        {
            sprintf( environment -> msg, "\n\t\tEntry:\
\n\t\t\tEnvironment = %p\
\n\t\t\tConnection = %p\
\n\t\t\tCompletion Type = %d",
                    (void*)environment_handle,
                    (void*)connection_handle,
                    (int)completion_type );

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    environment -> msg );
        }

        thread_protect( SQL_HANDLE_ENV, environment );

        if ( completion_type != SQL_COMMIT &&
                completion_type != SQL_ROLLBACK )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY012" );

            __post_internal_error( &environment -> error,
                    ERROR_HY012, NULL,
                    environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
        }

        if ( environment -> state == STATE_E2 )
        {
            /*
             * check that none of the connections are in a need data state
             */

            connection = __get_dbc_root();

            while( connection )
            {
                if ( connection -> environment == environment &&
                        connection -> state > STATE_C4 )
                {
                    if( __check_stmt_from_dbc_v( connection, 8, STATE_S8, STATE_S9, STATE_S10, STATE_S11, STATE_S12, STATE_S13, STATE_S14, STATE_S15 )) {

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

                connection = connection -> next_class_list;
            }

            /*
             * for each connection on this env
             */

            connection = __get_dbc_root();

            while( connection )
            {
                if ( connection -> environment == environment &&
                        connection -> state > STATE_C4 )
                {
                    if ( CHECK_SQLTRANSACT( connection ))
                    {
                        ret = SQLTRANSACT( connection,
                            SQL_NULL_HENV,
                            connection -> driver_dbc,
                            completion_type );

                        if ( !SQL_SUCCEEDED( ret ))
                        {
                            dm_log_write( __FILE__, 
                                    __LINE__, 
                                    LOG_INFO, 
                                    LOG_INFO, 
                                    "Error: 24S01" );

                            __post_internal_error( &environment -> error,
                                    ERROR_25S01, NULL,
                                    environment -> requested_version );

                            thread_release( SQL_HANDLE_ENV, environment );

                            return function_return( SQL_HANDLE_ENV, environment, SQL_ERROR, DEFER_R0 );
                        }
                    }
                    else if ( CHECK_SQLENDTRAN( connection ))
                    {
                        ret = SQLENDTRAN( connection,
                            SQL_HANDLE_DBC,
                            connection -> driver_dbc,
                            completion_type );

                        if ( !SQL_SUCCEEDED( ret ))
                        {
                            dm_log_write( __FILE__, 
                                    __LINE__, 
                                    LOG_INFO, 
                                    LOG_INFO, 
                                    "Error: 24S01" );

                            __post_internal_error( &environment -> error,
                                    ERROR_25S01, NULL,
                                    environment -> requested_version );

                            return function_return( SQL_HANDLE_ENV, environment, SQL_ERROR, DEFER_R0 );
                        }
                    }
                    else
                    {
                        dm_log_write( __FILE__, 
                            __LINE__, 
                            LOG_INFO, 
                            LOG_INFO, 
                            "Error: IM001" );

                        __post_internal_error( &environment -> error,
                            ERROR_IM001, NULL,
                            environment -> requested_version );

                        return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
                    }
                }

                connection = connection -> next_class_list;
            }
        }

        if ( log_info.log_flag )
        {
            sprintf( environment -> msg, 
                    "\n\t\tExit:[%s]",
                        __get_return_status( SQL_SUCCESS, s1 ));

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    environment -> msg );
        }

        thread_release( SQL_HANDLE_ENV, environment );

        return SQL_SUCCESS;
    }
    else
    {
        dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

        return SQL_INVALID_HANDLE;
    }
}
