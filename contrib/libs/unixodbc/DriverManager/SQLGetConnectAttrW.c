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
 * $Id: SQLGetConnectAttrW.c,v 1.14 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetConnectAttrW.c,v $
 * Revision 1.14  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.13  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.12  2008/08/29 08:01:38  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.11  2004/11/22 17:02:48  lurcher
 * Fix unicode/ansi conversion in the SQLGet functions
 *
 * Revision 1.10  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.9  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.8  2002/11/11 17:10:10  lurcher
 *
 * VMS changes
 *
 * Revision 1.7  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.6  2002/08/12 13:17:52  lurcher
 *
 * Replicate the way the MS DM handles loading of driver libs, and allocating
 * handles in the driver. usage counting in the driver means that dlopen is
 * only called for the first use, and dlclose for the last. AllocHandle for
 * the driver environment is only called for the first time per driver
 * per application environment.
 *
 * Revision 1.5  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.4  2002/07/16 13:08:18  lurcher
 *
 * Filter attribute values from SQLSetStmtAttr to SQLSetStmtOption to fit
 * within ODBC 2
 * Make DSN's double clickable in ODBCConfig
 *
 * Revision 1.3  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.2  2001/12/04 16:46:19  lurcher
 *
 * Allow the Unix Domain Socket to be set from the ini file (DSN)
 * Make the DataManager browser work with drivers that don't support
 * SQLRowCount
 * Make the directory selection from odbctest work simplier
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

static char const rcsid[]= "$RCSfile: SQLGetConnectAttrW.c,v $";

SQLRETURN SQLGetConnectAttrW( SQLHDBC connection_handle,
           SQLINTEGER attribute,
           SQLPOINTER value,
           SQLINTEGER buffer_length,
           SQLINTEGER *string_length )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    int type = 0;
    char *ptr;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

    /*
     * doesn't require a handle
     */

    if ( attribute == SQL_ATTR_TRACE )
    {
        if ( value )
        {
            if ( log_info.log_flag )
            {
                *((SQLINTEGER*)value) = SQL_OPT_TRACE_ON;
            }
            else
            {
                *((SQLINTEGER*)value) = SQL_OPT_TRACE_OFF;
            }
        }

        return SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_TRACEFILE )
    {
        SQLRETURN ret =  SQL_SUCCESS;

        ptr = log_info.log_file_name;

        if ( ptr )
        {
            int len = strlen( ptr ) * sizeof( SQLWCHAR );
            if ( string_length )
            {
                *string_length = len;
            }
            if ( value )
            {
                if ( buffer_length > len + sizeof( SQLWCHAR ))
                {
                    ansi_to_unicode_copy( value, ptr, SQL_NTS, connection, NULL );
                }
                else
                {
                    ansi_to_unicode_copy( value, ptr, buffer_length - 1, connection, NULL );
                    ((SQLWCHAR*)value)[( buffer_length - 1 ) / sizeof( SQLWCHAR )] = 0;
                    ret = SQL_SUCCESS_WITH_INFO;
                }
            }
        }
        else
        {
            if ( string_length )
            {
                *string_length = 0;
            }
            if ( value )
            {
                if ( buffer_length > 0 )
                {
                    ((SQLWCHAR*)value)[ 0 ] = 0;
                }
                else
                {
                    ret = SQL_SUCCESS_WITH_INFO;
                }
            }
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
\n\t\t\tAttribute = %s\
\n\t\t\tValue = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tStrLen = %p",
                connection,
                __con_attr_as_string( s1, attribute ),
                value, 
                (int)buffer_length,
                (void*)string_length );

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
        switch ( attribute )
        {
          case SQL_ATTR_ACCESS_MODE:
          case SQL_ATTR_AUTOCOMMIT:
          case SQL_ATTR_LOGIN_TIMEOUT:
          case SQL_ATTR_ODBC_CURSORS:
          case SQL_ATTR_TRACE:
          case SQL_ATTR_TRACEFILE:
          case SQL_ATTR_ASYNC_ENABLE:
            break;

          case SQL_ATTR_PACKET_SIZE:
            if ( connection -> packet_size_set )
                break;
          case SQL_ATTR_QUIET_MODE:
            if ( connection -> quite_mode_set )
                break;

          default:
            {
                struct save_attr *sa = connection -> save_attr;
                while (sa)
                {
                    if (sa -> attr_type == attribute)
                    {
                        SQLRETURN rc = SQL_SUCCESS;
                        if (sa -> str_len == SQL_NTS || sa -> str_len > 0)
                        {
                            SQLLEN realLen = sa->str_attr ? strlen(sa->str_attr) : 0;
                            if(value && sa->str_attr && buffer_length)
                            {
                                ansi_to_unicode_copy( value, sa->str_attr, buffer_length/sizeof(SQLWCHAR), connection, NULL );
                                ((SQLCHAR*)value)[buffer_length - 1] = 0;
                            }
                            if(string_length)
                            {
                                *string_length = realLen * sizeof(SQLWCHAR);
                            }
                            if(realLen * sizeof(SQLWCHAR) > buffer_length - 1)
                            {
                                __post_internal_error( &connection -> error,
                                ERROR_01004, NULL,
                                connection -> environment -> requested_version );
                                rc = SQL_SUCCESS_WITH_INFO;
                            }
                        }
                        else if(buffer_length >= sizeof(SQLLEN))
                        {
                            *(SQLLEN*)value = sa -> intptr_attr;
                            if(string_length)
                            {
                                *string_length = sizeof(SQLLEN);
                            }
                        }
                        else if(sa -> str_len >= SQL_IS_SMALLINT && sa -> str_len <= SQL_IS_POINTER)
                        {
                            SQLLEN length = 0;
                            switch (sa -> str_len)
                            {
                            case SQL_IS_SMALLINT:
                                *(SQLSMALLINT*)value = sa->intptr_attr;
                                length = sizeof(SQLSMALLINT);
                                break;
                            case SQL_IS_USMALLINT:
                                *(SQLUSMALLINT*)value = sa->intptr_attr;
                                length = sizeof(SQLUSMALLINT);
                                break;
                            case SQL_IS_INTEGER:
                                *(SQLINTEGER*)value = sa->intptr_attr;
                                length = sizeof(SQLINTEGER);
                                break;
                            case SQL_IS_UINTEGER:
                                *(SQLUINTEGER*)value = sa->intptr_attr;
                                length = sizeof(SQLUINTEGER);
                                break;
                            case SQL_IS_POINTER:
                                *(SQLPOINTER**)value = (SQLPOINTER) sa->intptr_attr;
                                length = sizeof(SQLPOINTER);
                                break;
                            }
                            if(string_length)
                            {
                                *string_length = length;
                            }
                        }
                        else
                        {
                            memcpy(value, &sa->intptr_attr, buffer_length);
                        }
                        return function_return_nodrv( SQL_HANDLE_DBC, connection, rc );
                    }
                    sa = sa -> next;
                }
            }
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

    switch ( attribute )
    {
      case SQL_ATTR_ACCESS_MODE:
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

      case SQL_ATTR_AUTOCOMMIT:
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

      case SQL_ATTR_ODBC_CURSORS:
        *((SQLULEN*)value) = connection -> cursors;
        type = 1;
        break;

      case SQL_ATTR_TRACE:
        *((SQLINTEGER*)value) = connection -> trace;
        type = 1;
        break;

      case SQL_ATTR_TRACEFILE:
        ptr = connection -> tracefile;
        type = 2;
        break;

      case SQL_ATTR_ASYNC_ENABLE:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLULEN*)value) = connection -> async_enable;
            type = 1;
        }
        break;

      case SQL_ATTR_AUTO_IPD:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLINTEGER*)value) = connection -> auto_ipd;
            type = 1;
        }
        break;

      case SQL_ATTR_CONNECTION_TIMEOUT:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLINTEGER*)value) = connection -> connection_timeout;
            type = 1;
        }
        break;

      case SQL_ATTR_METADATA_ID:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLINTEGER*)value) = connection -> metadata_id;
            type = 1;
        }
        break;

      case SQL_ATTR_PACKET_SIZE:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLINTEGER*)value) = connection -> packet_size;
            type = 1;
        }
        break;

      case SQL_ATTR_QUIET_MODE:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLINTEGER*)value) = connection -> quite_mode;
            type = 1;
        }
        break;

      case SQL_ATTR_TXN_ISOLATION:
        /*
         * if connected, call the driver
         */
        if ( connection -> state != STATE_C2 )
        {
            type = 0;
        }
        else
        {
            *((SQLINTEGER*)value) = connection -> txn_isolation;
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
        SQLRETURN ret = SQL_SUCCESS;
        if ( type == 1 )
        {
            if ( string_length )
            {
                *string_length = sizeof( SQLUINTEGER );
            }
        }
        else
        {
            if ( string_length )
            {
                *string_length = strlen( ptr );
            }
            if ( value )
            {
                if ( buffer_length > strlen( ptr ) + 1 )
                {
                    strcpy( value, ptr );
                }
                else
                {
                    memcpy( value, ptr, buffer_length - 1 );
                    ((char*)value)[ buffer_length - 1 ] = '\0';
                    ret = SQL_SUCCESS_WITH_INFO;
                }
            }
        }

        sprintf( connection -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, s1 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, ret );
    }
    else
    {
        SQLRETURN ret = 0;

        /*
         * call the driver
         */
        if ( connection -> unicode_driver ||
			CHECK_SQLGETCONNECTATTRW( connection ) ||
			CHECK_SQLGETCONNECTOPTIONW( connection ))
        {
            if ( !CHECK_SQLGETCONNECTATTRW( connection ))
            {
                if ( CHECK_SQLGETCONNECTOPTIONW( connection ))
                {
                    /*
                     * Is it in the legal range of values
                     */

                    if ( attribute < SQL_CONN_DRIVER_MIN && 
                            ( attribute > SQL_PACKET_SIZE || attribute < SQL_ACCESS_MODE ))
                    {
                        dm_log_write( __FILE__, 
                                    __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY092" );

                        __post_internal_error( &connection -> error,
                                ERROR_HY092, NULL,
                                connection -> environment -> requested_version );

                        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
                    }

                    ret = SQLGETCONNECTOPTIONW( connection,
                        connection -> driver_dbc,
                        attribute,
                        value );
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
                ret = SQLGETCONNECTATTRW( connection,
                    connection -> driver_dbc,
                    attribute,
                    value,
                    buffer_length,
                    string_length );
            }
        }
        else
        {
            if ( !CHECK_SQLGETCONNECTATTR( connection ))
            {
                if (( ret = CHECK_SQLGETCONNECTOPTION( connection )))
                {
                    SQLCHAR *as1 = NULL;

                    /*
                     * Is it in the legal range of values
                     */

                    if ( attribute < SQL_CONN_DRIVER_MIN && 
                            ( attribute > SQL_PACKET_SIZE || attribute < SQL_ACCESS_MODE ))
                    {
                        dm_log_write( __FILE__, 
                                    __LINE__, 
                                LOG_INFO, 
                                LOG_INFO, 
                                "Error: HY092" );

                        __post_internal_error( &connection -> error,
                                ERROR_HY092, NULL,
                                connection -> environment -> requested_version );

                        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
                    }

                    switch( attribute )
                    {
                      case SQL_ATTR_CURRENT_CATALOG:
                      case SQL_ATTR_TRACEFILE:
                      case SQL_ATTR_TRANSLATE_LIB:
                        if ( SQL_SUCCEEDED( ret ) && value && buffer_length > 0 )
                        {
                            as1 = malloc( buffer_length + 1 );
                        }
                        break;
                    }

                    ret = SQLGETCONNECTOPTION( connection,
                        connection -> driver_dbc,
                        attribute,
                        as1 ? as1 : value );

                    switch( attribute )
                    {
                      case SQL_ATTR_CURRENT_CATALOG:
                      case SQL_ATTR_TRACEFILE:
                      case SQL_ATTR_TRANSLATE_LIB:
                        if ( SQL_SUCCEEDED( ret ) && value && buffer_length > 0 && as1 )
                        {
                            ansi_to_unicode_copy( value, (char*) as1, SQL_NTS, connection, NULL );
                        }
                        if ( as1 )
                        {
                            free( as1 );
                        }
						if ( SQL_SUCCEEDED( ret ) && string_length )
						{
							*string_length *= sizeof( SQLWCHAR );
						}
                        break;
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
                SQLCHAR *as1 = NULL;

                switch( attribute )
                {
                  case SQL_ATTR_CURRENT_CATALOG:
                  case SQL_ATTR_TRACEFILE:
                  case SQL_ATTR_TRANSLATE_LIB:
                    buffer_length = buffer_length / 2;
                    if ( buffer_length > 0 )
                    {
                        as1 = malloc( buffer_length + 1 );
                    }
                    break;
                }

                ret = SQLGETCONNECTATTR( connection,
                    connection -> driver_dbc,
                    attribute,
                    as1 ? as1 : value,
                    buffer_length,
                    string_length );

                switch( attribute )
                {
                  case SQL_ATTR_CURRENT_CATALOG:
                  case SQL_ATTR_TRACEFILE:
                  case SQL_ATTR_TRANSLATE_LIB:
                    if ( SQL_SUCCEEDED( ret ) && value && buffer_length > 0 && as1 )
                    {
                        ansi_to_unicode_copy( value, (char*)as1, SQL_NTS, connection, NULL );
                    }
                    if ( as1 )
                    {
                        free( as1 );
                    }		
					if ( SQL_SUCCEEDED( ret ) && string_length )
					{
						*string_length *= sizeof( SQLWCHAR );
					}
                    break;
                }
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
