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
 * $Id: SQLSetConnectOptionW.c,v 1.10 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLSetConnectOptionW.c,v $
 * Revision 1.10  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.9  2007/02/28 15:37:48  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.8  2006/04/18 10:24:47  lurcher
 * Add a couple of changes from Mark Vanderwiel
 *
 * Revision 1.7  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.6  2003/03/05 09:48:45  lurcher
 *
 * Add some 64 bit fixes
 *
 * Revision 1.5  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.4  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.3  2002/01/30 12:20:02  lurcher
 *
 * Add MyODBC 3 driver source
 *
 * Revision 1.2  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.1.1.1  2001/10/17 16:40:07  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.4  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 * Revision 1.3  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLSetConnectOptionW.c,v $";

SQLRETURN SQLSetConnectOptionW( SQLHDBC connection_handle,
           SQLUSMALLINT option,
           SQLULEN value )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];
	SQLWCHAR buffer[ 512 ];

    /*
     * doesn't require a handle
     */

    if ( option == SQL_ATTR_TRACE )
    {
        if ((SQLLEN) value != SQL_OPT_TRACE_OFF && 
            (SQLLEN) value != SQL_OPT_TRACE_ON ) 
        {
            if ( __validate_dbc( connection ))
            {
                thread_protect( SQL_HANDLE_DBC, connection );
                function_entry( connection );
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY024" );
        
                __post_internal_error( &connection -> error,
                    ERROR_HY024, NULL,
                    connection -> environment -> requested_version );
        
                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }
            else 
            {
                return SQL_INVALID_HANDLE;
            }
        }

        if ( value == SQL_OPT_TRACE_OFF )
        {
            log_info.log_flag = 0;
        }
        else
        {
            log_info.log_flag = 1;
        }

        return SQL_SUCCESS;
    }
    else if ( option == SQL_ATTR_TRACEFILE )
    {
        if ( value )
        {
            if (((SQLWCHAR*)value)[ 0 ] == 0 ) 
            {
                if ( __validate_dbc( connection ))
                {
                    thread_protect( SQL_HANDLE_DBC, connection );
                    function_entry( connection );
                    dm_log_write( __FILE__, 
                            __LINE__, 
                            LOG_INFO, 
                            LOG_INFO, 
                            "Error: HY024" );
            
                    __post_internal_error( &connection -> error,
                        ERROR_HY024, NULL,
                        connection -> environment -> requested_version );
            
                    return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
                }
                else 
                {
                    return SQL_INVALID_HANDLE;
                }
            }
            else 
            {
                if ( log_info.log_file_name )
                {
                    free( log_info.log_file_name );
                }
                log_info.log_file_name = unicode_to_ansi_alloc((SQLWCHAR *) value, SQL_NTS, connection, NULL );
            }
        }
        else 
        {
            if ( __validate_dbc( connection ))
            {
                thread_protect( SQL_HANDLE_DBC, connection );
                function_entry( connection );
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY024" );
        
                __post_internal_error( &connection -> error,
                    ERROR_HY024, NULL,
                    connection -> environment -> requested_version );
        
                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }
            else 
            {
                return SQL_INVALID_HANDLE;
            }
        }
        return SQL_SUCCESS;
    }

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

#ifdef WITH_HANDLE_REDIRECT
		{
			DMHDBC parent_connection;

			parent_connection = find_parent_handle( connection, SQL_HANDLE_DBC );

			if ( parent_connection ) {
        		dm_log_write( __FILE__, 
                	__LINE__, 
                    	LOG_INFO, 
                    	LOG_INFO, 
                    	"Info: found parent handle" );

				if ( CHECK_SQLSETCONNECTOPTIONW( parent_connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

					return SQLSETCONNECTOPTIONW( parent_connection, 
							connection_handle, 
							option,
							value );
				}
			}
		}
#endif
        return SQL_INVALID_HANDLE;
    }

    function_entry( connection );

    if ( log_info.log_flag )
    {
        sprintf( connection -> msg, "\n\t\tEntry:\
\n\t\t\tConnection = %p\
\n\t\t\tOption = %s\
\n\t\t\tValue = %d",
                connection,
                __con_attr_as_string( s1, option ),
                (int)value );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    if ( connection -> state == STATE_C2 )
    {
        if ( option == SQL_TRANSLATE_OPTION ||
                option == SQL_TRANSLATE_DLL )
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
    }
    else if ( connection -> state == STATE_C3 )
    {
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
    else if ( connection -> state == STATE_C4 ||
            connection -> state == STATE_C5 )
    {
        if ( option == SQL_ODBC_CURSORS )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: 08002" );

            __post_internal_error( &connection -> error,
                ERROR_08002, NULL,
                connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }
    }
    else if ( connection -> state == STATE_C6 )
    {
        if ( option == SQL_ODBC_CURSORS )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: 08002" );

            __post_internal_error( &connection -> error,
                ERROR_08002, NULL,
                connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }
        else if ( option == SQL_TXN_ISOLATION )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: S1011" );

            __post_internal_error( &connection -> error,
                ERROR_S1011, NULL,
                connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }
    }

    /*
     * is it a legitimate value
     */
    ret = dm_check_connection_attrs( connection, option, (SQLPOINTER)value );

    if ( ret != SQL_SUCCESS ) 
    {
        dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY024" );

        __post_internal_error( &connection -> error,
                ERROR_HY024, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    /*
     * is it something overridden
     */

    value = (SQLULEN) __attr_override_wide( connection, SQL_HANDLE_DBC, option, (void*) value, NULL, buffer );

    if ( option == SQL_LOGIN_TIMEOUT )
    {
        connection -> login_timeout_set = 1;
        connection -> login_timeout = value;
        ret = SQL_SUCCESS;
    }
    else if ( option == SQL_ATTR_ACCESS_MODE )
    {
        connection -> access_mode = ( SQLLEN ) value;
        connection -> access_mode_set = 1;
    }
    else if ( option == SQL_AUTOCOMMIT )
    {
        connection -> auto_commit = ( SQLINTEGER ) value;
        connection -> auto_commit_set = 1;
    }

    if ( option == SQL_ODBC_CURSORS )
    {
        connection -> cursors = value;
        ret = SQL_SUCCESS;
    }
    else if ( connection -> state == STATE_C2 )
    {
        if ( option == SQL_AUTOCOMMIT )
        {
            connection -> auto_commit = ( SQLINTEGER ) value;
            connection -> auto_commit_set = 1;
        }
        else if ( option == SQL_ATTR_QUIET_MODE )
        {
            connection -> quite_mode = ( SQLLEN ) value;
            connection -> quite_mode_set = 1;
        }
        else if ( option == SQL_ATTR_ACCESS_MODE )
        {
            connection -> access_mode = ( SQLLEN ) value;
            connection -> access_mode_set = 1;
        }
        else
        {
            /*
             * save any unknown attributes untill connect
             */

            struct save_attr sa, *sap;
            
            memset( &sa, 0, sizeof( sa ));
            
            sa.attr_type = option;
            sa.intptr_attr = value;
            
            sap = connection -> save_attr;
            
            while ( sap )
            {
                if ( sap -> attr_type == option )
                {
                    free( sap -> str_attr );
                    break;
                }
                sap = sap -> next;
            }

            if ( sap )
            {
                *sap = sa;
            }
            else
            {
                sap = malloc( sizeof( struct save_attr ));
                *sap = sa;

                sap -> next = connection -> save_attr;
                connection -> save_attr = sap;
            }
        }

        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, 
                    "\n\t\tExit:[%s]",
                        __get_return_status( SQL_SUCCESS, s1 ));

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    connection -> msg );
        }

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_SUCCESS );
    }
    else
    {
        /*
         * call the driver
         */
        if ( CHECK_SQLSETCONNECTOPTIONW( connection ))
        {
            ret = SQLSETCONNECTOPTIONW( connection,
                    connection -> driver_dbc,
                    option,
                    value );
        }
        else if ( CHECK_SQLSETCONNECTATTRW( connection ))
        {
            SQLINTEGER  string_length;

            switch( option )
            {
              case SQL_ATTR_CURRENT_CATALOG:
              case SQL_ATTR_TRACEFILE:
              case SQL_ATTR_TRANSLATE_LIB:
                string_length = SQL_NTS;
                break;

              default:
                string_length = 0;
                break;
            }
            ret = SQLSETCONNECTATTRW( connection,
                    connection -> driver_dbc,
                    option,
                    (SQLPOINTER)(intptr_t) value,
                    string_length );
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
    }

    /*
     * catch this 
     */

    if ( option == SQL_ATTR_USE_BOOKMARKS && SQL_SUCCEEDED( ret ))
    {
        connection -> bookmarks_on = (SQLUINTEGER) value;
    }

    return function_return( SQL_HANDLE_DBC, connection, ret, DEFER_R3 );
}
