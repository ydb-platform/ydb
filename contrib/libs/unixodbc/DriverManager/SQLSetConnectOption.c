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
 * $Id: SQLSetConnectOption.c,v 1.12 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLSetConnectOption.c,v $
 * Revision 1.12  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.11  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.10  2003/03/05 09:48:45  lurcher
 *
 * Add some 64 bit fixes
 *
 * Revision 1.9  2003/02/27 12:19:40  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.8  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.7  2002/07/25 09:30:26  lurcher
 *
 * Additional unicode and iconv changes
 *
 * Revision 1.6  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.5  2002/07/04 17:27:56  lurcher
 *
 * Small bug fixes
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
 * Revision 1.8  2001/09/27 17:05:48  nick
 *
 * Assorted fixes and tweeks
 *
 * Revision 1.7  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 * Revision 1.6  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.5  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.4  2001/02/07 11:20:23  nick
 *
 * Remove some compile warnings
 *
 * Revision 1.3  2001/02/06 18:46:55  nick
 *
 * More UNICODE ommissions
 *
 * Revision 1.2  2000/11/14 10:15:27  nick
 *
 * Add test for localtime_r
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.11  2000/06/20 13:30:10  ngorham
 *
 * Fix problems when using bookmarks
 *
 * Revision 1.10  2000/05/21 21:49:19  ngorham
 *
 * Assorted fixes
 *
 * Revision 1.9  1999/11/13 23:41:00  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.8  1999/11/10 03:51:34  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.7  1999/10/24 23:54:18  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.6  1999/09/21 22:34:25  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.5  1999/09/19 22:24:34  ngorham
 *
 * Added support for the cursor library
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
 * Revision 1.2  1999/06/30 23:56:55  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:08  sShandyb
 * first go at it
 *
 * Revision 1.4  1999/06/03 22:20:25  ngorham
 *
 * Finished off the ODBC3-2 mapping
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
#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLSetConnectOption.c,v $ $Revision: 1.12 $";

SQLRETURN SQLSetConnectOptionA( SQLHDBC connection_handle,
           SQLUSMALLINT option,
           SQLULEN value )
{
    return SQLSetConnectOption( connection_handle,
           option,
           value );
}

SQLRETURN SQLSetConnectOption( SQLHDBC connection_handle,
           SQLUSMALLINT option,
           SQLULEN value )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

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
            if (((SQLCHAR*)value)[ 0 ] == '\0' ) 
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
                log_info.log_file_name = strdup((char*) value );
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

    value = (SQLULEN) __attr_override( connection, SQL_HANDLE_DBC, option, (void*) value, NULL );

    /*
     * we need to save this even if connected so we can use it for the next connect
     */
    if ( option == SQL_LOGIN_TIMEOUT )
    {
        connection -> login_timeout_set = 1;
        connection -> login_timeout = value;
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
             * save any unknown attributes until connect
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
        if ( connection -> unicode_driver )
        {
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
                void *ptr = (void *) value;

                switch( option )
                {
                  case SQL_ATTR_CURRENT_CATALOG:
                  case SQL_ATTR_TRACEFILE:
                  case SQL_ATTR_TRANSLATE_LIB:
                    string_length = SQL_NTS;
                    ptr = (void *) ansi_to_unicode_alloc(( SQLCHAR * ) value, SQL_NTS, connection, NULL );
                    break;

                  default:
                    string_length = 0;
                    break;
                }
                ret = SQLSETCONNECTATTRW( connection,
                        connection -> driver_dbc,
                        option,
                        ptr,
                        string_length );

                if ( ptr != (void*) value )
                {
                    free( ptr );
                }
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
        }
        else
        {
            if ( CHECK_SQLSETCONNECTOPTION( connection ))
            {
                ret = SQLSETCONNECTOPTION( connection,
                        connection -> driver_dbc,
                        option,
                        value );
            }
            else if ( CHECK_SQLSETCONNECTATTR( connection ))
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
                ret = SQLSETCONNECTATTR( connection,
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
