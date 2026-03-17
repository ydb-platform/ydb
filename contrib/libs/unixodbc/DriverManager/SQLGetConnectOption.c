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
 * $Id: SQLGetConnectOption.c,v 1.9 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetConnectOption.c,v $
 * Revision 1.9  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.8  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.7  2008/09/29 14:02:45  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.6  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.5  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.4  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.3  2002/11/11 17:10:10  lurcher
 *
 * VMS changes
 *
 * Revision 1.2  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.5  2001/08/03 15:19:00  nick
 *
 * Add changes to set values before connect
 *
 * Revision 1.4  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.8  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
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

static char const rcsid[]= "$RCSfile: SQLGetConnectOption.c,v $ $Revision: 1.9 $";

SQLRETURN SQLGetConnectOptionA( SQLHDBC connection_handle,
           SQLUSMALLINT option,
           SQLPOINTER value )
{
    return SQLGetConnectOption( connection_handle,
                        option,
                        value );
}

SQLRETURN SQLGetConnectOption( SQLHDBC connection_handle,
           SQLUSMALLINT option,
           SQLPOINTER value )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    int type = 0;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

    /*
     * doesn't require a handle
     */

    if ( option == SQL_ATTR_TRACE )
    {
        if ( value )
        {
            *((SQLINTEGER*)value) = SQL_OPT_TRACE_ON;
        }

        return SQL_SUCCESS;
    }
    else if ( option == SQL_ATTR_TRACEFILE )
    {
        SQLRETURN ret =  SQL_SUCCESS;

        if ( log_info.log_file_name )
        {
            strcpy( value, log_info.log_file_name );
        }
        else
        {
            strcpy( value, "" );
        }
        return ret;
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
\n\t\t\tValue = %p",
                connection,
                __con_attr_as_string( s1, option ),
                value );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    if ( connection -> state == STATE_C3 )
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

    if ( connection -> state == STATE_C2 )
    {
        switch ( option )
        {
          case SQL_ACCESS_MODE:
          case SQL_AUTOCOMMIT:
          case SQL_LOGIN_TIMEOUT:
          case SQL_ODBC_CURSORS:
            break;

          default:
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

    switch ( option )
    {
      case SQL_ACCESS_MODE:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLINTEGER*)value) = connection -> access_mode;
            type = 1;
        }
        break;

      case SQL_AUTOCOMMIT:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLINTEGER*)value) = connection -> auto_commit;
            type = 1;
        }
        break;

      case SQL_ODBC_CURSORS:
        *((SQLINTEGER*)value) = connection -> cursors;
        type = 1;
        break;

      case SQL_ATTR_LOGIN_TIMEOUT:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLINTEGER*)value) = connection -> login_timeout;
            type = 1;
        }
        break;

      default:
        break;
    }

    /*
     * if type has been set we have already set the value,
     * so just return
     */
    if ( type )
    {
        sprintf( connection -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( SQL_SUCCESS, s1 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_SUCCESS );
    }
    else
    {
        SQLRETURN ret = 0;

        /*
         * call the driver
         */

        if ( connection -> unicode_driver )
        {
            SQLWCHAR *s1 = NULL;

            if (( ret = CHECK_SQLGETCONNECTOPTIONW( connection )))
            {
                switch( option )
                {
                  case SQL_ATTR_CURRENT_CATALOG:
                  case SQL_ATTR_TRACEFILE:
                  case SQL_ATTR_TRANSLATE_LIB:
                    if ( SQL_SUCCEEDED( ret ) && value )
                         s1 = malloc( sizeof( SQLWCHAR ) * 1024 ); /* guess a length */
                    break;
                }

                ret = SQLGETCONNECTOPTIONW( connection,
                        connection -> driver_dbc,
                        option,
                        s1 ? s1 : value );

                switch( option )
                {
                  case SQL_ATTR_CURRENT_CATALOG:
                  case SQL_ATTR_TRACEFILE:
                  case SQL_ATTR_TRANSLATE_LIB:
                    if ( SQL_SUCCEEDED( ret ) && value && s1 )
                    {
                        unicode_to_ansi_copy( value, 1024, s1, SQL_NTS, connection, NULL );
                    }
                    break;
                }

                if ( s1 )
                {
                    free( s1 );
                }
            }
            else if ( CHECK_SQLGETCONNECTATTRW( connection ))
            {
                SQLINTEGER length, len;
                void * ptr;
                SQLWCHAR txt[ 1024 ];

                switch( option )
                {
                  case SQL_ATTR_CURRENT_CATALOG:
                  case SQL_ATTR_TRACEFILE:
                  case SQL_ATTR_TRANSLATE_LIB:
                    length = sizeof( txt );
                    ptr = txt;
                    break;

                  default:
                    length = sizeof( SQLINTEGER );
                    ptr = value;
                    break;

                }

                ret = SQLGETCONNECTATTRW( connection,
                        connection -> driver_dbc,
                        option,
                        ptr,
                        length,
                        &len );

                /*
                 * not much else we can do here, lets assume that 
                 * there is enough space
                 */

                if ( ptr != value && SQL_SUCCEEDED( ret ))
                {
                    unicode_to_ansi_copy( value, 1024, ptr, SQL_NTS, connection, NULL );

                    /*
                     * are we still here ? good
                     */
                }
            }
            else
            {
                __post_internal_error( &connection -> error,
                        ERROR_IM001, NULL,
                        connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }
        }
        else
        {
            if ( CHECK_SQLGETCONNECTOPTION( connection ))
            {
                ret = SQLGETCONNECTOPTION( connection,
                        connection -> driver_dbc,
                        option,
                        value );
            }
            else if ( CHECK_SQLGETCONNECTATTR( connection ))
            {
                SQLINTEGER length, len;
                void * ptr;
                char txt[ 1024 ];

                switch( option )
                {
                  case SQL_ATTR_CURRENT_CATALOG:
                  case SQL_ATTR_TRACEFILE:
                  case SQL_ATTR_TRANSLATE_LIB:
                    length = sizeof( txt );
                    ptr = txt;
                    break;

                  default:
                    length = sizeof( SQLINTEGER );
                    ptr = value;
                    break;

                }

                ret = SQLGETCONNECTATTR( connection,
                        connection -> driver_dbc,
                        option,
                        ptr,
                        length,
                        &len );

                /*
                 * not much else we can do here, lets assume that 
                 * there is enough space
                 */

                if ( ptr != value )
                {
                    strcpy( value, ptr );

                    /*
                     * are we still here ? good
                     */
                }
            }
            else
            {
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

        return function_return( SQL_HANDLE_DBC, connection, ret, DEFER_R3 );
    }
}
