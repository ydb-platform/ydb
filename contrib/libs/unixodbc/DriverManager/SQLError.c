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
 * $Id: SQLError.c,v 1.11 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLError.c,v $
 * Revision 1.11  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.10  2008/09/29 14:02:45  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.9  2008/05/20 13:43:47  lurcher
 * Vms fixes
 *
 * Revision 1.8  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.7  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.6  2002/11/11 17:10:08  lurcher
 *
 * VMS changes
 *
 * Revision 1.5  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.4  2002/07/24 08:49:51  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.3  2002/02/27 11:27:14  lurcher
 *
 * Fix bug in error reporting
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
 * Revision 1.5  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.4  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.3  2001/01/06 15:00:12  nick
 *
 * Fix bug in SQLError introduced with UNICODE
 *
 * Revision 1.2  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.14  2000/06/23 16:11:35  ngorham
 *
 * Map ODBC 2 SQLSTATE values to ODBC 3
 *
 * Revision 1.13  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.12  1999/11/10 22:15:48  ngorham
 *
 * Fix some bugs with the DM and error reporting.
 *
 * Revision 1.11  1999/11/10 03:51:33  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.10  1999/10/24 23:54:17  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.9  1999/10/14 06:49:24  ngorham
 *
 * Remove @all_includes@ from Drivers/MiniSQL/Makefile.am
 *
 * Revision 1.8  1999/09/21 22:34:24  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.7  1999/08/03 21:47:39  shandyb
 * Moving to automake: changed files in DriverManager
 *
 * Revision 1.6  1999/07/15 06:22:33  ngorham
 *
 * Fixed spelling mistake
 *
 * Revision 1.5  1999/07/14 19:46:04  ngorham
 *
 * Fix the error logging when SQLError or SQLGetDiagRec returns SQL_NO_DATA
 *
 * Revision 1.4  1999/07/12 19:42:05  ngorham
 *
 * Finished off SQLGetDiagField.c and fixed a but that caused SQLError to
 * fail with Perl and PHP, connect errors were not being returned because
 * I was checking to the environment being set, they were setting the
 * statement and the environment. The order of checking has been changed.
 *
 * Revision 1.3  1999/07/04 21:05:07  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/30 23:56:54  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:06  sShandyb
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
 * Revision 1.3  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.2  1999/04/29 21:40:58  nick
 * End of another night :-)
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLError.c,v $ $Revision: 1.11 $";

static SQLRETURN local_extract_sql_error( EHEAD *head,
        SQLCHAR *sqlstate,
        SQLINTEGER *native_error,
        SQLCHAR *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length,
        DMHDBC connection )
{
    ERROR *err;
    SQLRETURN ret;
    char *str;

    if ( sqlstate )
        strcpy((char*) sqlstate, "00000" );

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
        unicode_to_ansi_copy((char*) sqlstate, 6, err -> sqlstate, SQL_NTS, connection, NULL );
    }

    str = unicode_to_ansi_alloc( err -> msg, SQL_NTS, connection, NULL );

    if ( message_text && buffer_length < strlen( str ) + 1 )
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
            strcpy((char*) message_text, str );
        }
        else
        {
            memcpy( message_text, str, buffer_length );
            message_text[ buffer_length - 1 ] = '\0';
        }
    }

    if ( text_length )
    {
        *text_length = strlen( str );
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

    if ( str ) free( str );

    /*
     * map 3 to 2 if required
     */

    if ( SQL_SUCCEEDED( ret ) && sqlstate )
        __map_error_state( (char *)sqlstate, __get_version( head ));

    return ret;
}

SQLRETURN SQLErrorA( SQLHENV environment_handle,
           SQLHDBC connection_handle,
           SQLHSTMT statement_handle,
           SQLCHAR *sqlstate,
           SQLINTEGER *native_error,
           SQLCHAR *message_text,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *text_length )
{
    return SQLError( environment_handle,
                        connection_handle,
                        statement_handle,
                        sqlstate,
                        native_error,
                        message_text,
                        buffer_length,
                        text_length );
}

SQLRETURN SQLError( SQLHENV environment_handle,
           SQLHDBC connection_handle,
           SQLHSTMT statement_handle,
           SQLCHAR *sqlstate,
           SQLINTEGER *native_error,
           SQLCHAR *message_text,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *text_length )
{
    SQLRETURN ret;
    SQLCHAR s0[ 64 ], s1[ 100 + LOG_MESSAGE_LEN ];
    SQLCHAR s2[ 100 + LOG_MESSAGE_LEN ];

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

        if ( !__validate_stmt( statement ))
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: SQL_INVALID_HANDLE" );

            return SQL_INVALID_HANDLE;
        }

        connection = statement->connection;
        active_handle = statement;
        handle_type = SQL_HANDLE_STMT;
        herror = &statement->error;
        handle_msg = statement->msg;
        handle_type_ptr = "Statement";
    }
    else if (connection_handle)
    {
        connection =  ( DMHDBC ) connection_handle;

        if ( !__validate_dbc( connection ))
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: SQL_INVALID_HANDLE" );

            return SQL_INVALID_HANDLE;
        }

        active_handle = connection;
        handle_type = SQL_HANDLE_DBC;
        herror = &connection->error;
        handle_msg = connection->msg;
        handle_type_ptr = "Connection";
    }
    else if ( environment_handle )
    {
        environment = ( DMHENV ) environment_handle;

        if ( !__validate_env( environment ))
        {
            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: SQL_INVALID_HANDLE" );

            return SQL_INVALID_HANDLE;
        }

        active_handle = environment;
        handle_type = SQL_HANDLE_ENV;
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
#ifdef HAVE_SNPRINTF
        snprintf( handle_msg, LOG_MSG_MAX*2,
#else
        sprintf( handle_msg,
#endif
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

    ret = local_extract_sql_error( herror,
            sqlstate,
            native_error,
            message_text,
            buffer_length,
            text_length,
            connection );

    if ( log_info.log_flag )
    {
        if ( SQL_SUCCEEDED( ret ))
        {
#ifdef HAVE_SNPRINTF
            snprintf( handle_msg, LOG_MSG_MAX*2,
#else
            sprintf( handle_msg,
#endif
                "\n\t\tExit:[%s]\
\n\t\t\tSQLState = %s\
\n\t\t\tNative = %s\
\n\t\t\tMessage Text = %s",
                        __get_return_status( ret, s2 ),
                        sqlstate,
                        __iptr_as_string( s0, native_error ),
                        __sdata_as_string( s1, SQL_CHAR,
                            text_length, message_text ));
        }
        else
        {
#ifdef HAVE_SNPRINTF
            snprintf( handle_msg, LOG_MSG_MAX*2,
#else
            sprintf( handle_msg,
#endif
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

