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
 * $Id: SQLGetConnectOptionW.c,v 1.10 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetConnectOptionW.c,v $
 * Revision 1.10  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.9  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.8  2008/08/29 08:01:38  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.7  2007/02/28 15:37:48  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.6  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.5  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.4  2002/11/11 17:10:12  lurcher
 *
 * VMS changes
 *
 * Revision 1.3  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.2  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.4  2001/08/03 15:19:00  nick
 *
 * Add changes to set values before connect
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

static char const rcsid[]= "$RCSfile: SQLGetConnectOptionW.c,v $";

SQLRETURN SQLGetConnectOptionW( SQLHDBC connection_handle,
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
            ansi_to_unicode_copy( value, log_info.log_file_name, SQL_NTS, connection, NULL );
        }
        else
        {
            ansi_to_unicode_copy( value, "", SQL_NTS, connection, NULL );
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

				if ( CHECK_SQLGETCONNECTOPTIONW( parent_connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

					return SQLGETCONNECTOPTIONW( parent_connection, 
							connection,
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
        if ( connection -> unicode_driver ||
			CHECK_SQLGETCONNECTOPTIONW( connection ) ||
			CHECK_SQLGETCONNECTATTRW( connection ))
        {
            if ( CHECK_SQLGETCONNECTOPTIONW( connection ))
            {
                ret = SQLGETCONNECTOPTIONW( connection,
                        connection -> driver_dbc,
                        option,
                        value );
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

                if ( ptr != value )
                {
                    wide_strcpy( value, ptr );

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
            if (( ret = CHECK_SQLGETCONNECTOPTION( connection )))
            {
                SQLCHAR *as1 = NULL;

                switch( option )
                {
                  case SQL_ATTR_CURRENT_CATALOG:
                  case SQL_ATTR_TRACEFILE:
                  case SQL_ATTR_TRANSLATE_LIB:
                    if ( SQL_SUCCEEDED( ret ) && value )
                    {
                        /* 
                         * we need to chance out arm here, as we dont know
                         */

                        as1 = malloc( 1024 );
                    }
                    break;
                }

                ret = SQLGETCONNECTOPTION( connection,
                        connection -> driver_dbc,
                        option,
                        as1 ? as1 : value );

                switch( option )
                {
                  case SQL_ATTR_CURRENT_CATALOG:
                  case SQL_ATTR_TRACEFILE:
                  case SQL_ATTR_TRANSLATE_LIB:
                    if ( SQL_SUCCEEDED( ret ) && value && as1 )
                    {
                        ansi_to_unicode_copy( value, (char*) as1, SQL_NTS, connection, NULL );
                    }

                    if ( as1 )
                    {
                        free( as1 );
                    }
                    break;
                }
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
                    SQLWCHAR *s1;

                    s1 = ansi_to_unicode_alloc( value, SQL_NTS, connection, NULL );

                    if ( s1 )
                    {
                        wide_strcpy( value, s1 );
                        free( s1 );
                    }

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
