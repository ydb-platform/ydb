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
 * $Id: SQLConnectW.c,v 1.15 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLConnectW.c,v $
 * Revision 1.15  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.14  2008/09/29 14:02:44  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.13  2007/02/28 15:37:47  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.12  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.11  2002/12/20 11:36:46  lurcher
 *
 * Update DMEnvAttr code to allow setting in the odbcinst.ini entry
 *
 * Revision 1.10  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.9  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.8  2002/07/25 09:30:26  lurcher
 *
 * Additional unicode and iconv changes
 *
 * Revision 1.7  2002/07/24 08:49:51  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.6  2002/05/28 13:30:34  lurcher
 *
 * Tidy up for AIX
 *
 * Revision 1.5  2002/05/24 12:42:50  lurcher
 *
 * Alter NEWS and ChangeLog to match their correct usage
 * Additional UNICODE tweeks
 *
 * Revision 1.4  2002/04/10 11:04:36  lurcher
 *
 * Fix endian issue with 4 byte unicode support
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
 * Revision 1.4  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2001/01/04 13:16:25  nick
 *
 * Add support for GNU portable threads and tidy up some UNICODE compile
 * warnings
 *
 * Revision 1.1  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLConnectW.c,v $";

/*
 * connection pooling stuff
 */

extern int pooling_enabled;
extern CPOOLHEAD *pool_head;
extern int pool_max_size;
extern int pool_wait_timeout;



SQLRETURN SQLConnectW( SQLHDBC connection_handle,
           SQLWCHAR *server_name,
           SQLSMALLINT name_length1,
           SQLWCHAR *user_name,
           SQLSMALLINT name_length2,
           SQLWCHAR *authentication,
           SQLSMALLINT name_length3 )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    int len, ret_from_connect;
    SQLWCHAR dsn[ SQL_MAX_DSN_LENGTH + 1 ];
    char lib_name[ INI_MAX_PROPERTY_VALUE + 1 ];
    char driver_name[ INI_MAX_PROPERTY_VALUE + 1 ];
    SQLCHAR ansi_dsn[ SQL_MAX_DSN_LENGTH + 1 ];
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ], s2[ 100 + LOG_MESSAGE_LEN ], s3[ 100 + LOG_MESSAGE_LEN ], ansi_user[ SQL_MAX_DSN_LENGTH + 1 ], ansi_pwd[ SQL_MAX_DSN_LENGTH + 1 ];
    int warnings;
    CPOOLHEAD *pooh = 0;

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

				if ( CHECK_SQLCONNECTW( parent_connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

					return SQLCONNECTW( parent_connection, 
							connection_handle, 
							server_name, 
							name_length1,
							user_name,
							name_length2,
							authentication,
							name_length3 );
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
\n\t\t\tServer Name = %s\
\n\t\t\tUser Name = %s\
\n\t\t\tAuthentication = %s",
                connection,
                __wstring_with_length( s1, server_name, name_length1 ),
                __wstring_with_length( s2, user_name, name_length2 ),
                __wstring_with_length_pass( s3, authentication, name_length3 ));

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                connection -> msg );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    if (( name_length1 < 0 && name_length1 != SQL_NTS ) ||
        ( name_length2 < 0 && name_length2 != SQL_NTS ) ||
        ( name_length3 < 0 && name_length3 != SQL_NTS ))

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
     * check the state of the connection
     */
    if ( connection -> state != STATE_C2 )
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

    if ( name_length1 && server_name )
    {
        if ( name_length1 == SQL_NTS )
        {
            len = wide_strlen( server_name );

            if ( len > SQL_MAX_DSN_LENGTH )
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
        }
        else
        {
            len = name_length1;

            if ( len > SQL_MAX_DSN_LENGTH )
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
        }
        memcpy( dsn, server_name, sizeof( dsn[ 0 ] ) * len );
        dsn[ len ] = (SQLWCHAR) 0;
    }
    else if ( name_length1 > SQL_MAX_DSN_LENGTH )
    {
        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                "Error: IM010" );

        __post_internal_error( &connection -> error,
                ERROR_IM010, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }
    else
    {
        int i;

        for ( i = 0; i < 8; i ++ )
            dsn[ i ] = "DEFAULT"[i];
    }

    /*
     * can we find a pooled connection to use here ?
     */

    connection -> pooled_connection = NULL;

    if ( pooling_enabled ) {

        char *ansi_server_name, *ansi_user_name, *ansi_authentication;
        int ansi_name_length1, ansi_name_length2, ansi_name_length3;
        int retpool;
        int retrying = 0;
        time_t wait_begin = time( NULL );


        if ( server_name ) {
            ansi_server_name = unicode_to_ansi_alloc( server_name, name_length1, connection, &ansi_name_length1 );
        }
        else {
            ansi_server_name = NULL;
            ansi_name_length1 = 0;
        }

        if ( user_name ) {
            ansi_user_name = unicode_to_ansi_alloc( user_name, name_length2, connection, &ansi_name_length2 );
        }
        else {
            ansi_user_name = NULL;
            ansi_name_length2 = 0;
        }

        if ( authentication ) {
            ansi_authentication = unicode_to_ansi_alloc( authentication, name_length3, connection, &ansi_name_length3 );
        }
        else {
            ansi_authentication = NULL;
            ansi_name_length3 = 0;
        }

retry:
        retpool = search_for_pool( connection, 
                                        ansi_server_name, ansi_name_length1,
                                        ansi_user_name, ansi_name_length2,
                                        ansi_authentication, ansi_name_length3,
                                        NULL, 0,
                                        &pooh, retrying );

        /*
         * found usable existing connection from pool
         */
        if ( retpool == 1 )
        {
            ret_from_connect = SQL_SUCCESS;

            if ( ansi_server_name ) {
                free( ansi_server_name );
            }
            if ( ansi_user_name ) {
                free( ansi_user_name );
            }
            if ( ansi_authentication ) {
                free( ansi_authentication );
            }

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg,
                        "\n\t\tExit:[%s]",
                            __get_return_status( ret_from_connect, s1 ));

                dm_log_write( __FILE__,
                            __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        connection -> msg );
            }

            connection -> state = STATE_C4;

            return function_return_nodrv( SQL_HANDLE_DBC, connection, ret_from_connect );
        }

        /*
         * pool is at capacity
         */
        if ( retpool == 2 )
        {
            /*
             * either no timeout or exceeded the timeout
             */
            if ( ! pool_wait_timeout || time( NULL ) - wait_begin > pool_wait_timeout )
            {
                if ( ansi_server_name ) {
                    free( ansi_server_name );
                }
                if ( ansi_user_name ) {
                    free( ansi_user_name );
                }
                if ( ansi_authentication ) {
                    free( ansi_authentication );
                }

                mutex_pool_exit();
                dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: HYT02" );

                __post_internal_error( &connection -> error,
                    ERROR_HYT02, NULL,
                    connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }

            /*
             * wait up to 1 second for a signal and try again
             */
            pool_timedwait( connection );
            retrying = 1;
            goto retry;
        }

        /*
         * 1 pool entry has been reserved. Early exits henceforth need to unreserve.
         */

        /*
         * else save the info for later
         */

        connection -> dsn_length = 0;

        if ( ansi_server_name )
        {
            if ( ansi_name_length1 < 0 )
            {
                connection -> _server = strdup( ansi_server_name );
            }
            else
            {
                connection -> _server = malloc( ansi_name_length1 );
                memcpy( connection -> _server, ansi_server_name, ansi_name_length1 );
            }
        }
        else
        {
            connection -> _server = strdup( "" );
        }
        connection -> server_length = ansi_name_length1;

        if ( ansi_user_name )
        {
            if ( ansi_name_length2 < 0 )
            {
                connection -> _user = strdup( ansi_user_name );
            }
            else
            {
                connection -> _user = malloc( ansi_name_length2 );
                memcpy( connection -> _user, ansi_user_name, ansi_name_length2 );
            }
        }
        else
        {
            connection -> _user = strdup( "" );
        }
        connection -> user_length = ansi_name_length2;

        if ( ansi_authentication )
        {
            if ( ansi_name_length3 < 0 )
            {
                connection -> _password = strdup( ansi_authentication );
            }
            else
            {
                connection -> _password = malloc( ansi_name_length3 );
                memcpy( connection -> _password, ansi_authentication, ansi_name_length3 );
            }
        }
        else
        {
            connection -> _password = strdup( "" );
        }
        connection -> password_length = ansi_name_length3;

        if ( ansi_server_name ) {
            free( ansi_server_name );
        }
        if ( ansi_user_name ) {
            free( ansi_user_name );
        }
        if ( ansi_authentication ) {
            free( ansi_authentication );
        }
    }

    unicode_to_ansi_copy((char*) ansi_dsn, sizeof( ansi_dsn ), dsn, sizeof( ansi_dsn ), NULL, NULL );

    if ( !*ansi_dsn || !__find_lib_name((char*) ansi_dsn, lib_name, driver_name ))
    {
        /*
         * if not found look for a default
         */

        if ( !__find_lib_name( "DEFAULT", lib_name, driver_name ))
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: IM002" );

            __post_internal_error( &connection -> error,
                    ERROR_IM002, NULL,
                    connection -> environment -> requested_version );

            pool_unreserve( pooh );

            return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
        }
    }

    /*
     * do we have any Environment, Connection, or Statement attributes set in the ini ?
     */

    __handle_attr_extensions( connection, (char*) ansi_dsn, driver_name );

    /*
     * if necessary change the threading level
     */

    warnings = 0;

    if ( !__connect_part_one( connection, lib_name, driver_name, &warnings ))
    {
        __disconnect_part_four( connection );       /* release unicode handles */

        pool_unreserve( pooh );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    if ( !CHECK_SQLCONNECTW( connection ) &&
            !CHECK_SQLCONNECT( connection ))
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

        pool_unreserve( pooh );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    if ( CHECK_SQLCONNECTW( connection ))
    {
        if ( CHECK_SQLSETCONNECTATTR( connection ))
        {
            SQLSETCONNECTATTR( connection,
                    connection -> driver_dbc,
                    SQL_ATTR_ANSI_APP,
                    SQL_AA_FALSE,
                    0 );
        }

        ret_from_connect = SQLCONNECTW( connection,
                connection -> driver_dbc,
                dsn, SQL_NTS,
                user_name, name_length2,
                authentication, name_length3 );

        connection -> unicode_driver = 1;
    }
    else
    {
        if ( user_name )
        {
            if ( name_length2 == SQL_NTS )
                unicode_to_ansi_copy((char*) ansi_user, sizeof( ansi_user ),user_name, sizeof( ansi_user ), connection, NULL);
            else
                unicode_to_ansi_copy((char*) ansi_user, sizeof( ansi_user ),user_name, name_length2, connection, NULL );
        }

        if ( authentication )
        {
            if ( name_length3 == SQL_NTS )
                unicode_to_ansi_copy((char*) ansi_pwd, sizeof( ansi_pwd ), authentication, sizeof( ansi_pwd ), connection, NULL);
            else
                unicode_to_ansi_copy((char*) ansi_pwd, sizeof( ansi_pwd ), authentication, name_length3, connection, NULL );
        }

        /*
        if ( CHECK_SQLSETCONNECTATTR( connection ))
        {
            int lret;
                
            lret = SQLSETCONNECTATTR( connection,
                    connection -> driver_dbc,
                    SQL_ATTR_ANSI_APP,
                    SQL_AA_TRUE,
                    0 );
        }
        */

        ret_from_connect = SQLCONNECT( connection,
                connection -> driver_dbc,
                ansi_dsn, SQL_NTS,
                user_name ? ansi_user : NULL, name_length2,
                authentication ? ansi_pwd : NULL, name_length3 );

        connection -> unicode_driver = 0;
    }

    if ( ret_from_connect != SQL_SUCCESS )
    {
        /*
         * get the errors from the driver before
         * loseing the connection 
         */

        if ( connection -> unicode_driver )
        {
            SQLWCHAR sqlstate[ 6 ];
            SQLINTEGER native_error;
            SQLSMALLINT ind;
            SQLWCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
            SQLRETURN ret;

            if ( CHECK_SQLERRORW( connection ))
            {
                do
                {
                    ret = SQLERRORW( connection,
                            SQL_NULL_HENV,
                            connection -> driver_dbc,
                            SQL_NULL_HSTMT,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text )/sizeof(SQLWCHAR),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex_w( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );
                    }

                    sprintf( connection -> msg,
                            "\n\t\tExit:[%s]",
                                __get_return_status( ret_from_connect, s1 ));

                    dm_log_write( __FILE__,
                            __LINE__,
                            LOG_INFO,
                            LOG_INFO,
                            connection -> msg );
                }
                while( SQL_SUCCEEDED( ret ));
            }
            else if ( CHECK_SQLGETDIAGRECW( connection ))
            {
                int rec = 1;

                do
                {
                    ret = SQLGETDIAGRECW( connection,
                            SQL_HANDLE_DBC,
                            connection -> driver_dbc,
                            rec ++,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text )/sizeof(SQLWCHAR),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex_w( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );
                    }

                    sprintf( connection -> msg,
                            "\n\t\tExit:[%s]",
                                __get_return_status( ret_from_connect, s1 ));

                    dm_log_write( __FILE__,
                            __LINE__,
                            LOG_INFO,
                            LOG_INFO,
                            connection -> msg );
                }
                while( SQL_SUCCEEDED( ret ));
            }
        }
        else
        {
            SQLCHAR sqlstate[ 6 ];
            SQLINTEGER native_error;
            SQLSMALLINT ind;
            SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
            SQLRETURN ret;

            if ( CHECK_SQLERROR( connection ))
            {
                do
                {
                    ret = SQLERROR( connection,
                            SQL_NULL_HENV,
                            connection -> driver_dbc,
                            SQL_NULL_HSTMT,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );
                    }

                    sprintf( connection -> msg,
                            "\n\t\tExit:[%s]",
                                __get_return_status( ret_from_connect, s1 ));

                    dm_log_write( __FILE__,
                            __LINE__,
                            LOG_INFO,
                            LOG_INFO,
                            connection -> msg );
                }
                while( SQL_SUCCEEDED( ret ));
            }
            else if ( CHECK_SQLGETDIAGREC( connection ))
            {
                int rec = 1;

                do
                {
                    ret = SQLGETDIAGREC( connection,
                            SQL_HANDLE_DBC,
                            connection -> driver_dbc,
                            rec ++,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );
                    }

                    sprintf( connection -> msg,
                            "\n\t\tExit:[%s]",
                                __get_return_status( ret_from_connect, s1 ));

                    dm_log_write( __FILE__,
                            __LINE__,
                            LOG_INFO,
                            LOG_INFO,
                            connection -> msg );
                }
                while( SQL_SUCCEEDED( ret ));
            }
        }


        /* 
         * if it was a error then return now
         */

        if ( !SQL_SUCCEEDED( ret_from_connect ))
        {
            __disconnect_part_one( connection );
            __disconnect_part_four( connection );       /* release unicode handles */

            pool_unreserve( pooh );

            return function_return( SQL_HANDLE_DBC, connection, ret_from_connect, DEFER_R0 );
        }
    }

    /*
     * we should be connected now
     */
    connection -> state = STATE_C4;
    strcpy( connection -> dsn, (char*)ansi_dsn );

    /*
     * did we get the type we wanted
     */
    if ( connection -> driver_version !=
            connection -> environment -> requested_version )
    {
        connection -> driver_version =
            connection -> environment -> requested_version;

        __post_internal_error( &connection -> error,
                ERROR_01000, "Driver does not support the requested version",
                connection -> environment -> requested_version );
        ret_from_connect = SQL_SUCCESS_WITH_INFO;
    }

    if ( !__connect_part_two( connection ))
    {
        /*
         * the cursor lib can kill us here, so be careful
         */

        __disconnect_part_two( connection );
        __disconnect_part_one( connection );
        __disconnect_part_four( connection );       /* release unicode handles */

        connection -> state = STATE_C3;

        pool_unreserve( pooh );

        return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
    }

    if ( log_info.log_flag ) 
    {
        sprintf( connection -> msg,
                "\n\t\tExit:[%s]",
                    __get_return_status( ret_from_connect, s1 ));

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                connection -> msg );
    }

    if ( warnings && ret_from_connect == SQL_SUCCESS )
    {
        ret_from_connect = SQL_SUCCESS_WITH_INFO;
    }

    if ( pooling_enabled && !add_to_pool( connection, pooh ) )
    {
        pool_unreserve( pooh );
    }

    return function_return_nodrv( SQL_HANDLE_DBC, connection, ret_from_connect );
}
