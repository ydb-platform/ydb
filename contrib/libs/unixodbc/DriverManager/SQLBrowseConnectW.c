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
 * $Id: SQLBrowseConnectW.c,v 1.13 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLBrowseConnectW.c,v $
 * Revision 1.13  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.12  2007/10/19 10:14:05  lurcher
 * Pull errors from SQLBrowseConnect when it returns SQL_NEED_DATA
 *
 * Revision 1.11  2007/02/28 15:37:46  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
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
 * Revision 1.8  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.7  2002/08/15 08:10:33  lurcher
 *
 * Couple of small fixes from John L Miller
 *
 * Revision 1.6  2002/07/25 09:30:26  lurcher
 *
 * Additional unicode and iconv changes
 *
 * Revision 1.5  2002/07/24 08:49:51  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.4  2002/02/08 17:59:40  lurcher
 *
 * Fix threading problem in SQLBrowseConnect
 *
 * Revision 1.3  2002/01/21 18:00:51  lurcher
 *
 * Assorted fixed and changes, mainly UNICODE/bug fixes
 *
 * Revision 1.2  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.7  2001/07/20 12:35:09  nick
 *
 * Fix SQLBrowseConnect operation
 *
 * Revision 1.6  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.5  2001/05/15 10:57:44  nick
 *
 * Add initial support for VMS
 *
 * Revision 1.4  2001/04/16 15:41:24  nick
 *
 * Fix some problems calling non existing error funcs
 *
 * Revision 1.3  2001/04/12 17:43:35  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2001/01/02 09:55:04  nick
 *
 * More unicode bits
 *
 * Revision 1.1  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLBrowseConnectW.c,v $";

#define BUFFER_LEN      4095

SQLRETURN SQLBrowseConnectW(
    SQLHDBC            hdbc,
    SQLWCHAR           *conn_str_in,
    SQLSMALLINT        len_conn_str_in,
    SQLWCHAR           *conn_str_out,
    SQLSMALLINT        conn_str_out_max,
    SQLSMALLINT        *ptr_conn_str_out )
{
    DMHDBC connection = (DMHDBC) hdbc;
    struct con_struct con_struct;
    char *driver, *dsn;
    char lib_name[ INI_MAX_PROPERTY_VALUE + 1 ];
    char driver_name[ INI_MAX_PROPERTY_VALUE + 1 ];
    SQLWCHAR in_str_bufw[ BUFFER_LEN ];
    SQLWCHAR *in_str;
    SQLSMALLINT in_str_len;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ], s2[ 100 + LOG_MESSAGE_LEN ];
    int warnings = 0;

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

				if ( CHECK_SQLBROWSECONNECTW( parent_connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

					return SQLBROWSECONNECTW( parent_connection, 
							connection, 
							conn_str_in,
							len_conn_str_in,
							conn_str_out,
							conn_str_out_max,
							ptr_conn_str_out );
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
\n\t\t\tStr In = %s\
\n\t\t\tStr Out = %s\
\n\t\t\tPtr Conn Str Out = %p",
                connection,
                __wstring_with_length( s1, conn_str_in, len_conn_str_in ), 
                __wstring_with_length( s2, conn_str_out, conn_str_out_max ), 
                ptr_conn_str_out );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    /*
     * check the state of the connection
     */

    if ( connection -> state == STATE_C4 ||
        connection -> state == STATE_C5 ||
        connection -> state == STATE_C6 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 08002" );

        __post_internal_error( &connection -> error,
                ERROR_08002, NULL, 
                connection -> environment -> requested_version );

        return function_return_nodrv( IGNORE_THREAD, connection, SQL_ERROR );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    if ( len_conn_str_in < 0  &&  len_conn_str_in != SQL_NTS)
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY090" );

        __post_internal_error( &connection -> error,
                ERROR_HY090, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    /*
     * are we at the start of a connection
     */

    driver_name[ 0 ] = '\0';

    if ( connection -> state == STATE_C2 )
    {
        char in_str_buf[ BUFFER_LEN ];
        /*
         * parse the connection string
         */

        __parse_connection_string_w( &con_struct,
                conn_str_in, len_conn_str_in );

        /*
         * look for some keywords
         * have we got a DRIVER= attribute
         */

        driver = __get_attribute_value( &con_struct, "DRIVER" );
        if ( driver )
        {
            /*
             * look up the driver in the ini file
             */
            SQLGetPrivateProfileString( driver, "Driver", "",
                    lib_name, sizeof( lib_name ), "ODBCINST.INI" );

            if ( lib_name[ 0 ] == '\0' )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: IM002" );

                __post_internal_error( &connection -> error,
                        ERROR_IM002, NULL,
                        connection -> environment -> requested_version );

                __release_conn( &con_struct );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }

            strcpy( connection -> dsn, "" );
        }
        else
        {
            dsn = __get_attribute_value( &con_struct, "DSN" );
            if ( !dsn )
            {
                dsn = "DEFAULT";
                __append_pair( &con_struct, "DSN", "DEFAULT" );
            }

            if ( strlen( dsn ) > SQL_MAX_DSN_LENGTH )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: IM012" );

                __post_internal_error( &connection -> error,
                        ERROR_IM012, NULL,
                        connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }

            /*
             * look up the dsn in the ini file
             */

            if ( !__find_lib_name( dsn, lib_name, driver_name ))
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: IM002" );

                __post_internal_error( &connection -> error,
                        ERROR_IM002, NULL,
                        connection -> environment -> requested_version );
                __release_conn( &con_struct );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }

            strcpy( connection -> dsn, dsn );
        }

        __generate_connection_string( &con_struct, in_str_buf, sizeof( in_str_buf ));
        __release_conn( &con_struct );
        ansi_to_unicode_copy( in_str_bufw, in_str_buf, BUFFER_LEN, connection, 0 );

        /*
         * we now have a driver to connect to
         */

        if ( !__connect_part_one( connection, lib_name, driver_name, &warnings ))
        {
            __disconnect_part_four( connection );       /* release unicode handles */

            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: connect_part_one fails" );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }

        if ( !CHECK_SQLBROWSECONNECTW( connection ) &&
            !CHECK_SQLBROWSECONNECT( connection ))
        {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: IM001" );

            __disconnect_part_one( connection );
            __disconnect_part_four( connection );       /* release unicode handles */
            __post_internal_error( &connection -> error,
                    ERROR_IM001, NULL,
                    connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }
        in_str = in_str_bufw;
        in_str_len = wide_strlen(in_str);
    }
    else
    {
        in_str = conn_str_in;
        in_str_len = len_conn_str_in == SQL_NTS ? wide_strlen(in_str) : len_conn_str_in;
    }

    if ( CHECK_SQLBROWSECONNECTW( connection ))
    {
        ret = SQLBROWSECONNECTW( connection,
                connection -> driver_dbc,
                in_str,
                in_str_len,
                conn_str_out,
                conn_str_out_max,
                ptr_conn_str_out );

        connection -> unicode_driver = 1;
    }
    else if (CHECK_SQLBROWSECONNECT( connection ))
    {
        SQLCHAR *an_in_str = (SQLCHAR*) unicode_to_ansi_alloc( in_str, SQL_NTS, connection, 0 );
        SQLCHAR *ob = conn_str_out ? malloc( (conn_str_out_max + 1) * sizeof(SQLWCHAR) ) : 0;
        SQLSMALLINT len;

        ret = SQLBROWSECONNECT( connection,
                connection -> driver_dbc,
                an_in_str,
                SQL_NTS,
                ob,
                conn_str_out_max,
                &len );

        *ptr_conn_str_out = len;
        if(ob)
        {
            if ( ptr_conn_str_out ) {
                int wptr;

                ansi_to_unicode_copy(conn_str_out, (char*)ob, conn_str_out_max, connection, &wptr );

                *ptr_conn_str_out = (SQLSMALLINT) wptr;
            }
            else {
                ansi_to_unicode_copy(conn_str_out, (char*)ob, conn_str_out_max, connection, NULL );
            }
            free(ob);
        }
        free(an_in_str);

        connection -> unicode_driver = 0;
    }
    else
    {
        dm_log_write( __FILE__,
                      __LINE__,
                      LOG_INFO,
                      LOG_INFO,
                      "Error: IM001" );

        __disconnect_part_one( connection );
        __disconnect_part_four( connection );       /* release unicode handles */
        __post_internal_error( &connection -> error,
                               ERROR_IM001, NULL,
                               connection -> environment -> requested_version );
        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );

    }

    if ( !SQL_SUCCEEDED( ret ) || ret == SQL_NEED_DATA )
    {
        /*
         * get the error from the driver before
         * losing the connection
         */
        if ( connection -> unicode_driver )
        {
            if ( CHECK_SQLGETDIAGFIELDW( connection ) &&
                    CHECK_SQLGETDIAGRECW( connection ))
            {
                extract_diag_error_w( SQL_HANDLE_DBC,
                        connection -> driver_dbc,
                        connection,
                        &connection -> error,
                        ret,
                        1 );
            }
            else if ( CHECK_SQLERRORW( connection )) 
            {
                extract_sql_error_w( SQL_NULL_HENV, 
                        connection -> driver_dbc, 
                        SQL_NULL_HSTMT, 
                        connection,
                        &connection -> error, 
                        ret );
            }
            else if ( CHECK_SQLGETDIAGFIELD( connection ) &&
                    CHECK_SQLGETDIAGREC( connection ))
            {
                extract_diag_error( SQL_HANDLE_DBC,
                        connection -> driver_dbc,
                        connection,
                        &connection -> error,
                        ret,
                        1 );
            }
            else if ( CHECK_SQLERROR( connection )) 
            {
                extract_sql_error( SQL_NULL_HENV, 
                        connection -> driver_dbc, 
                        SQL_NULL_HSTMT, 
                        connection,
                        &connection -> error, 
                        ret );
            }
            else 
            {
                __post_internal_error( &connection -> error,
                    ERROR_HY000, "Driver returned SQL_ERROR or SQL_SUCCESS_WITH_INFO but no error reporting API found",
                    connection -> environment -> requested_version );
            }
        }
        else
        {
            if ( CHECK_SQLGETDIAGFIELD( connection ) &&
                    CHECK_SQLGETDIAGREC( connection ))
            {
                extract_diag_error( SQL_HANDLE_DBC,
                        connection -> driver_dbc,
                        connection,
                        &connection -> error,
                        ret,
                        1 );
            }
            else if ( CHECK_SQLERROR( connection )) 
            {
                extract_sql_error( SQL_NULL_HENV, 
                        connection -> driver_dbc, 
                        SQL_NULL_HSTMT, 
                        connection,
                        &connection -> error, 
                        ret );
            }
            else if ( CHECK_SQLGETDIAGFIELDW( connection ) &&
                    CHECK_SQLGETDIAGRECW( connection ))
            {
                extract_diag_error_w( SQL_HANDLE_DBC,
                        connection -> driver_dbc,
                        connection,
                        &connection -> error,
                        ret,
                        1 );
            }
            else if ( CHECK_SQLERRORW( connection )) 
            {
                extract_sql_error_w( SQL_NULL_HENV, 
                        connection -> driver_dbc, 
                        SQL_NULL_HSTMT, 
                        connection,
                        &connection -> error, 
                        ret );
            }
            else 
            {
                __post_internal_error( &connection -> error,
                    ERROR_HY000, "Driver returned SQL_ERROR or SQL_SUCCESS_WITH_INFO but no error reporting API found",
                    connection -> environment -> requested_version );
            }
        }

    	if ( ret != SQL_NEED_DATA ) 
		{
            /* If an error occurred during SQLBrowseConnect, we need to keep the
               connection in the same state (C2 or C3). This allows the application
               to either try the SQLBrowseConnect again, or disconnect an active
               browse session with SQLDisconnect. Otherwise the driver may continue
               to have an active connection while the DM thinks it does not,
               causing more errors later on. */
            if ( connection -> state == STATE_C2 )
            {
                /* only disconnect and unload if we never started browsing */
                __disconnect_part_one( connection );
                __disconnect_part_four( connection );  /* release unicode handles - also sets state to C2 */
            }
		}
		else 
		{
       		connection -> state = STATE_C3;
		}
    }
    else
    {
        /*
         * we should be connected now
         */

        connection -> state = STATE_C4;

        if( ret == SQL_SUCCESS_WITH_INFO )
        {
            function_return_ex( IGNORE_THREAD, connection, ret, TRUE, DEFER_R0 );
        }

        if ( !__connect_part_two( connection ))
        {
            __disconnect_part_two( connection );
            __disconnect_part_one( connection );
            __disconnect_part_four( connection );       /* release unicode handles */
            if ( log_info.log_flag )
            {
                sprintf( connection -> msg, 
                        "\n\t\tExit:[%s]\
                        \n\t\t\tconnect_part_two fails",
                            __get_return_status( SQL_ERROR, s1 ));

                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        connection -> msg );
            }

            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
        }
    }

    if ( log_info.log_flag )
    {
        sprintf( connection -> msg, 
                "\n\t\tExit:[%s]\
                \n\t\t\tPtr Conn Str Out = %s",
                    __get_return_status( ret, s2 ),
                    __sptr_as_string( s1, ptr_conn_str_out ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    if ( warnings && ret == SQL_SUCCESS )
    {
        ret = SQL_SUCCESS_WITH_INFO;
    }

    return function_return_nodrv( SQL_HANDLE_DBC, connection, ret );
}
