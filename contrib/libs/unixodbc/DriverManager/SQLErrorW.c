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
 * $Id: SQLErrorW.c,v 1.9 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLErrorW.c,v $
 * Revision 1.9  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.8  2008/05/20 13:43:47  lurcher
 * Vms fixes
 *
 * Revision 1.7  2007/02/28 15:37:48  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.6  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.5  2002/07/25 09:30:26  lurcher
 *
 * Additional unicode and iconv changes
 *
 * Revision 1.4  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.3  2002/05/21 14:19:44  lurcher
 *
 * * Update libtool to escape from AIX build problem
 * * Add fix to avoid file handle limitations
 * * Add more UNICODE changes, it looks like it is native 16 representation
 *   the old way can be reproduced by defining UCS16BE
 * * Add iusql, its just the same as isql but uses the wide functions
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

static char const rcsid[]= "$RCSfile: SQLErrorW.c,v $";

SQLRETURN extract_parent_handle_err( int handle_type,
        SQLHENV environment_handle,
        SQLHDBC connection_handle,
        SQLHSTMT statement_handle,
        SQLWCHAR *sqlstate,
        SQLINTEGER *native_error,
        SQLWCHAR *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length )
{

    dm_log_write( __FILE__,
            __LINE__,
            LOG_INFO,
            LOG_INFO,
            "Error: SQL_INVALID_HANDLE" );

    if ( handle_type != SQL_HANDLE_ENV )
    {
#ifdef WITH_HANDLE_REDIRECT
        {
            DMHDBC *parent_handle_dbc;
            DMHSTMT *parent_handle_stmt;

            switch ( handle_type )
            {
                case SQL_HANDLE_DBC:
                    {
                        parent_handle_dbc = find_parent_handle( connection_handle, handle_type );
                    }
                    break;

                case SQL_HANDLE_STMT:
                    {
                        parent_handle_stmt = find_parent_handle( statement_handle, handle_type );
                        parent_handle_dbc = parent_handle_stmt->connection;
                    }
                    break;

                default:
                    {
                        return SQL_INVALID_HANDLE;
                    }
            }

            if ( parent_handle_dbc )
            {
                dm_log_write( __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Info: found parent handle" );

				if ( CHECK_SQLERRORW( parent_handle_dbc ))
				{
                    dm_log_write(__FILE__,
                            __LINE__,
                            LOG_INFO,
                            LOG_INFO,
                            "Info: calling redirected driver function" );

                    return SQLERRORW( parent_handle_dbc,
                            environment_handle,
                            connection_handle,
                            statement_handle,
                            sqlstate,
                            native_error,
                            message_text,
                            buffer_length,
                            text_length );
                }
            }
        }
#endif
    }

    return SQL_INVALID_HANDLE;
}

/*
 * unicode mapping function
 */

static SQLRETURN local_extract_sql_error_w( EHEAD *head,
        SQLWCHAR *sqlstate,
        SQLINTEGER *native_error,
        SQLWCHAR *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length )
{
    ERROR *err;
    SQLRETURN ret;

    if ( sqlstate )
    {
        SQLWCHAR *tmp;

        tmp = ansi_to_unicode_alloc((SQLCHAR*) "00000", SQL_NTS,  __get_connection( head ), NULL );
        wide_strcpy( sqlstate, tmp );
        free( tmp );
    }

    if ( head -> sql_error_head.error_count < 1 )
    {
        return SQL_NO_DATA;
    }

    err = head -> sql_error_head.error_list_head;
    head -> sql_error_head.error_list_head = err -> next;

    /*
     * is it the last
     */
    if ( head -> sql_error_head.error_list_tail == err )
        head -> sql_error_head.error_list_tail = NULL;

    /*
     * not empty yet
     */
    if ( head -> sql_error_head.error_list_head )
    {
        head -> sql_error_head.error_list_head -> prev = NULL;
    }

    head -> sql_error_head.error_count --;

    if ( sqlstate )
    {
        wide_strcpy( sqlstate, err -> sqlstate );
    }
    if ( message_text && buffer_length < wide_strlen( err -> msg ) + 1 )
    {
        ret = SQL_SUCCESS_WITH_INFO;
    }
    else
    {
        ret = SQL_SUCCESS;
    }

    if ( message_text )
    {
        if ( ret == SQL_SUCCESS )
        {
            wide_strcpy( message_text, err -> msg );
        }
        else
        {
            memcpy( message_text, err -> msg, buffer_length * 2 );
            message_text[ buffer_length - 1 ] = 0;
        }
    }

    if ( text_length )
    {
        *text_length = wide_strlen( err -> msg );
    }

	if ( native_error )
	{
		*native_error = err -> native_error;
	}

    /*
     * clean up
     */

    free( err -> msg );
    free( err );

    /*
     * map 3 to 2 if required
     */

    if ( SQL_SUCCEEDED( ret ) && sqlstate )
        __map_error_state_w( sqlstate, __get_version( head ));

    return ret;
}

SQLRETURN SQLErrorW( SQLHENV environment_handle,
           SQLHDBC connection_handle,
           SQLHSTMT statement_handle,
           SQLWCHAR *sqlstate,
           SQLINTEGER *native_error,
           SQLWCHAR *message_text,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *text_length )
{
    SQLRETURN ret;
    SQLCHAR s0[ 64 ], s1[ 100 + LOG_MESSAGE_LEN ];
    SQLCHAR s2[ 100 + LOG_MESSAGE_LEN ];
    SQLCHAR s3[ 100 + LOG_MESSAGE_LEN ];

    DMHENV  environment = NULL;
    DMHDBC  connection = NULL;
    DMHSTMT statement = NULL;
    void *active_handle = NULL;

    EHEAD *herror;
    char *handle_msg;

    int handle_type;
    const char *handle_type_ptr;

    if ( statement_handle )
    {
        statement = ( DMHSTMT ) statement_handle;
        handle_type = SQL_HANDLE_STMT;

        if ( !__validate_stmt( statement ))
        {
            return extract_parent_handle_err( handle_type, environment_handle, connection_handle, statement_handle, sqlstate, native_error, message_text, buffer_length, text_length );
        }

        connection = statement->connection;
        active_handle = statement;
        herror = &statement->error;
        handle_msg = statement->msg;
        handle_type_ptr = "Statement";
    }
    else if ( connection_handle )
    {
        connection =  ( DMHDBC ) connection_handle;
        handle_type = SQL_HANDLE_DBC;

        if ( !__validate_dbc( connection ))
        {
            return extract_parent_handle_err( handle_type, environment_handle, connection_handle, statement_handle, sqlstate, native_error, message_text, buffer_length, text_length );
        }

        active_handle = connection;
        herror = &connection->error;
        handle_msg = connection->msg;
        handle_type_ptr = "Connection";
    }
    else if ( environment_handle )
    {
        environment = ( DMHENV ) environment_handle;
        handle_type = SQL_HANDLE_ENV;

        if ( !__validate_env( environment ))
        {
            return extract_parent_handle_err( handle_type, environment_handle, connection_handle, statement_handle, sqlstate, native_error, message_text, buffer_length, text_length );
        }

        active_handle = environment;
        herror = &environment->error;
        handle_msg = environment->msg;
        handle_type_ptr = "Environment";
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

    thread_protect( handle_type, active_handle );

    if ( log_info.log_flag )
    {
        sprintf( handle_msg,
            "\n\t\tEntry:\
\n\t\t\t%s = %p\
\n\t\t\tSQLState = %p\
\n\t\t\tNative = %p\
\n\t\t\tMessage Text = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tText Len Ptr = %p",
                handle_type_ptr,
                active_handle,
                sqlstate,
                native_error,
                message_text,
                buffer_length,
                text_length );

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                handle_msg );
    }

    /*
     * Do diag extraction here if defer flag is set.
     * Clean the flag after extraction.
     * The defer flag will not be set for DMHENV in function_return_ex.
     */
    if ( connection && herror->defer_extract )
    {
        extract_error_from_driver( herror, connection, herror->ret_code_deferred, 0 );

        herror->defer_extract = 0;
        herror->ret_code_deferred = 0;
    }

    ret = local_extract_sql_error_w( herror,
            sqlstate,
            native_error,
            message_text,
            buffer_length,
            text_length );

    if ( log_info.log_flag )
    {
        if ( SQL_SUCCEEDED( ret ))
        {
            char *ts1, *ts2;

            sprintf( handle_msg,
                "\n\t\tExit:[%s]\
\n\t\t\tSQLState = %s\
\n\t\t\tNative = %s\
\n\t\t\tMessage Text = %s",
                    __get_return_status( ret, s2 ),
                    __sdata_as_string( s3, SQL_CHAR,
                                      NULL, ts1 = unicode_to_ansi_alloc( sqlstate, SQL_NTS, connection, NULL )),
                    __iptr_as_string( s0, native_error ),
                    __sdata_as_string( s1, SQL_CHAR,
                                      text_length, ( ts2 = unicode_to_ansi_alloc( message_text, SQL_NTS, connection, NULL ))));

            free( ts1 );
            free( ts2 );
        }
        else
        {
            sprintf( handle_msg,
                "\n\t\tExit:[%s]",
                __get_return_status( ret, s2 ));
        }

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                handle_msg );
    }

    thread_release( handle_type, active_handle );

   return ret;
}

