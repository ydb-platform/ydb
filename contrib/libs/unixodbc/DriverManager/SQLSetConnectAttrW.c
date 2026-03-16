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
 * $Id: SQLSetConnectAttrW.c,v 1.14 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLSetConnectAttrW.c,v $
 * Revision 1.14  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.13  2008/08/29 08:01:39  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.12  2007/02/28 15:37:48  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.11  2006/04/18 10:24:47  lurcher
 * Add a couple of changes from Mark Vanderwiel
 *
 * Revision 1.10  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.9  2003/03/05 09:48:44  lurcher
 *
 * Add some 64 bit fixes
 *
 * Revision 1.8  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.7  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.6  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.5  2002/07/16 13:08:18  lurcher
 *
 * Filter attribute values from SQLSetStmtAttr to SQLSetStmtOption to fit
 * within ODBC 2
 * Make DSN's double clickable in ODBCConfig
 *
 * Revision 1.4  2002/07/04 17:27:56  lurcher
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
 * Revision 1.7  2001/09/27 17:05:48  nick
 *
 * Assorted fixes and tweeks
 *
 * Revision 1.6  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 * Revision 1.5  2001/08/03 15:19:00  nick
 *
 * Add changes to set values before connect
 *
 * Revision 1.4  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.3  2001/05/23 13:48:37  nick
 *
 * Remove unwanted include
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

static char const rcsid[]= "$RCSfile: SQLSetConnectAttrW.c,v $";

SQLRETURN SQLSetConnectAttrW( SQLHDBC connection_handle,
           SQLINTEGER attribute,
           SQLPOINTER value,
           SQLINTEGER string_length )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];
	SQLWCHAR buffer[ 512 ];

    /*
     * doesn't require a handle
     */

    if ( attribute == SQL_ATTR_TRACE )
    {
        if ((SQLLEN) value != SQL_OPT_TRACE_OFF && 
            (SQLLEN) value != SQL_OPT_TRACE_ON ) 
        {
            if ( __validate_dbc( connection ))
            {
                thread_protect( SQL_HANDLE_DBC, connection );
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY024" );
        
                function_entry( connection );
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

        if ((SQLLEN) value == SQL_OPT_TRACE_OFF )
        {
            char force_string[ 30 ];

            SQLGetPrivateProfileString( "ODBC", "ForceTrace", "0",
				force_string, sizeof( force_string ), 
                "ODBCINST.INI" );

            if ( force_string[ 0 ] == '1' ||
                toupper( force_string[ 0 ] ) == 'Y' ||
                ( toupper( force_string[ 0 ] ) == 'O' &&
                    toupper( force_string[ 1 ] ) == 'N' ))
            {
                if ( log_info.log_flag )
                {
                    dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Application tried to turn logging off" );
                }
            }
            else
            {
                if ( log_info.log_flag )
                {
                    dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Application turning logging off" );
                }
                log_info.log_flag = 0;
            }
        }
        else
        {
            log_info.log_flag = 1;
        }

        return SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_TRACEFILE )
    {
        if ( value )
        {
            if (((SQLWCHAR*)value)[ 0 ] == 0 ) 
            {
                if ( __validate_dbc( connection ))
                {
                    thread_protect( SQL_HANDLE_DBC, connection );
                    dm_log_write( __FILE__, 
                            __LINE__, 
                            LOG_INFO, 
                            LOG_INFO, 
                            "Error: HY024" );

                    function_entry( connection );
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
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY024" );

                function_entry( connection );
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

				if ( CHECK_SQLSETCONNECTATTRW( parent_connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

					return SQLSETCONNECTATTRW( parent_connection, 
							connection_handle, 
           					attribute,
           					value,
           					string_length );
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
\n\t\t\tAttribute = %s\
\n\t\t\tValue = %p\
\n\t\t\tStrLen = %d",
                connection,
                __con_attr_as_string( s1, attribute ),
                value, 
                (int)string_length );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    if ( connection -> state == STATE_C2 )
    {
        if ( attribute == SQL_ATTR_TRANSLATE_OPTION ||
                attribute == SQL_ATTR_TRANSLATE_LIB )
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
            connection -> state == STATE_C5 || 
            connection -> state == STATE_C6 )
    {
        if ( attribute == SQL_ATTR_ODBC_CURSORS )
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
        else if ( attribute == SQL_ATTR_PACKET_SIZE )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY011" );

            __post_internal_error( &connection -> error,
                ERROR_HY011, NULL,
                connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }
    }

    /*
     * is it a legitimate value
     */
    ret = dm_check_connection_attrs( connection, attribute, value );

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
     * is it a connection attribute or statement, check state of any active connections
     */

    switch( attribute ) 
    {
        /* ODBC 3.x statement attributes are not settable at the connection level */
        case SQL_ATTR_APP_PARAM_DESC:
        case SQL_ATTR_APP_ROW_DESC:
        case SQL_ATTR_CURSOR_SCROLLABLE:
        case SQL_ATTR_CURSOR_SENSITIVITY:
        case SQL_ATTR_ENABLE_AUTO_IPD:
        case SQL_ATTR_FETCH_BOOKMARK_PTR:
        case SQL_ATTR_IMP_PARAM_DESC:
        case SQL_ATTR_IMP_ROW_DESC:
        case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
        case SQL_ATTR_PARAM_BIND_TYPE:
        case SQL_ATTR_PARAM_OPERATION_PTR:
        case SQL_ATTR_PARAM_STATUS_PTR:
        case SQL_ATTR_PARAMS_PROCESSED_PTR:
        case SQL_ATTR_PARAMSET_SIZE:
        case SQL_ATTR_ROW_ARRAY_SIZE:
        case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        case SQL_ATTR_ROW_OPERATION_PTR:
        case SQL_ATTR_ROW_STATUS_PTR:
        case SQL_ATTR_ROWS_FETCHED_PTR:
            __post_internal_error( &connection -> error,
                    ERROR_HY092, NULL,
                    connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );

      	case SQL_ATTR_CONCURRENCY:
	  	case SQL_BIND_TYPE:
      	case SQL_ATTR_CURSOR_TYPE:
      	case SQL_ATTR_MAX_LENGTH:
      	case SQL_MAX_ROWS:
      	case SQL_ATTR_KEYSET_SIZE:
      	case SQL_ROWSET_SIZE: 
      	case SQL_ATTR_NOSCAN:
      	case SQL_ATTR_QUERY_TIMEOUT:
      	case SQL_ATTR_RETRIEVE_DATA:
      	case SQL_ATTR_SIMULATE_CURSOR:
      	case SQL_ATTR_USE_BOOKMARKS:
            if( __check_stmt_from_dbc_v( connection, 8, STATE_S8, STATE_S9, STATE_S10, STATE_S11, STATE_S12, STATE_S13, STATE_S14, STATE_S15 )) {

                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: 24000" );

                __post_internal_error( &connection -> error,
                        ERROR_24000, NULL,
                        connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }
            break;

        default:

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
            break;
    }

    /*
     * is it something overridden
     */

    value = __attr_override_wide( connection, SQL_HANDLE_DBC, attribute, value, &string_length, buffer );

    /*
     * we need to save this even if connected so we can use it for the next connect
     */
    if ( attribute == SQL_ATTR_LOGIN_TIMEOUT )
    {
        connection -> login_timeout = ( SQLLEN ) value;
        connection -> login_timeout_set = 1;
    }

    /*
     * if connected, call the driver
     * otherwise we need to save the states and set them when we
     * do connect
     */
    if ( connection -> state == STATE_C2 )
    {
        /*
         * is it for us
         */

        if ( attribute == SQL_ATTR_ODBC_CURSORS )
        {
            connection -> cursors = ( SQLLEN ) value;
        }
        else if ( attribute == SQL_ATTR_ACCESS_MODE )
        {
            connection -> access_mode = ( SQLLEN ) value;
            connection -> access_mode_set = 1;
        }
        else if ( attribute == SQL_ATTR_ASYNC_ENABLE )
        {
            connection -> async_enable = ( SQLLEN ) value;
            connection -> async_enable_set = 1;
        }
        else if ( attribute == SQL_ATTR_AUTO_IPD )
        {
            connection -> auto_ipd = ( SQLLEN ) value;
            connection -> auto_ipd_set = 1;
        }
        else if ( attribute == SQL_ATTR_AUTOCOMMIT )
        {
            connection -> auto_commit = ( SQLLEN ) value;
            connection -> auto_commit_set = 1;
        }
        else if ( attribute == SQL_ATTR_CONNECTION_TIMEOUT )
        {
            connection -> connection_timeout = ( SQLLEN ) value;
            connection -> connection_timeout_set = 1;
        }
        else if ( attribute == SQL_ATTR_METADATA_ID )
        {
            connection -> metadata_id = ( SQLLEN ) value;
            connection -> metadata_id_set = 1;
        }
        else if ( attribute == SQL_ATTR_PACKET_SIZE )
        {
            connection -> packet_size = ( SQLLEN ) value;
            connection -> packet_size_set = 1;
        }
        else if ( attribute == SQL_ATTR_QUIET_MODE )
        {
            connection -> quite_mode = ( SQLLEN ) value;
            connection -> quite_mode_set = 1;
        }
        else if ( attribute == SQL_ATTR_TXN_ISOLATION )
        {
            connection -> txn_isolation = ( SQLLEN ) value;
            connection -> txn_isolation_set = 1;
        }
        else if ( attribute != SQL_ATTR_LOGIN_TIMEOUT )
        {
            /*
             * save any unknown attributes until connect
             */

            struct save_attr sa, *sap;

            memset( &sa, 0, sizeof ( sa ));

            sa.attr_type = attribute;
            if ( string_length > 0 )
            {
                sa.str_attr = malloc( string_length );
                memcpy( sa.str_attr, value, string_length );
                sa.str_len = string_length;
            }
            else if ( string_length == SQL_NTS )
            {
                if (!value)
                {
                    __post_internal_error( &connection -> error,
                        ERROR_HY024, "Invalid argument value",
                        connection -> environment -> requested_version );
                    return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
                }
                else
                {
                    sa.str_attr = unicode_to_ansi_alloc( value, string_length, connection, NULL );
                    sa.str_len = string_length;
                }
            }
            else
            {
                sa.intptr_attr = (intptr_t) value;
                sa.str_len = string_length;
            }
            
            sap = connection -> save_attr;
            
            while ( sap )
            {
                if ( sap -> attr_type == attribute )
                {
                    free ( sap -> str_attr );
                    break;
                }
                sap = sap -> next;
            }
            
            if ( sap ) /* replace existing attribute */
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
        if ( connection -> unicode_driver ||
			CHECK_SQLSETCONNECTATTRW( connection ))
        {
            if ( !CHECK_SQLSETCONNECTATTRW( connection ))
            {
                if ( CHECK_SQLSETCONNECTOPTIONW( connection ))
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

                    ret = SQLSETCONNECTOPTIONW( connection,
                            connection -> driver_dbc,
                            attribute,
                            (SQLULEN) value );
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
                ret = SQLSETCONNECTATTRW( connection,
                        connection -> driver_dbc,
                        attribute,
                        value,
                        string_length );
            }
        }
        else
        {
            if ( !CHECK_SQLSETCONNECTATTR( connection ))
            {
                if ( CHECK_SQLSETCONNECTOPTION( connection ))
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
                        if ( value )
                        {
                            as1 = (SQLCHAR*) unicode_to_ansi_alloc( value, SQL_NTS, connection, NULL );
                        }
                        break;
                    }

                    ret = SQLSETCONNECTOPTION( connection,
                            connection -> driver_dbc,
                            attribute,
                            (SQLULEN) (as1 ? as1 : value) );

                    if ( as1 ) free( as1 );
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
                    if ( value )
                    {
                        if ( string_length > 0 )
                        {
                            as1 = (SQLCHAR*) unicode_to_ansi_alloc( value, string_length, connection, NULL );
                        }
                        else if ( string_length == SQL_NTS )
                        {
                            as1 = (SQLCHAR*) unicode_to_ansi_alloc( value, SQL_NTS, connection, NULL );
                        }
                    }

                    ret = SQLSETCONNECTATTR( connection,
                            connection -> driver_dbc,
                            attribute,
                            as1 ? as1 : value,
                            string_length / sizeof( SQLWCHAR ));

                    if ( as1 ) 
                    {
                        free( as1 );
                    }
                    break;

                  default:
                    ret = SQLSETCONNECTATTR( connection,
                            connection -> driver_dbc,
                            attribute,
                            value,
                            string_length );
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
    }

    /*
     * catch this 
     */

    if ( attribute == SQL_ATTR_USE_BOOKMARKS && SQL_SUCCEEDED( ret ))
    {
        connection -> bookmarks_on = (SQLULEN) value;
    }

    return function_return( SQL_HANDLE_DBC, connection, ret, DEFER_R3 );
}
