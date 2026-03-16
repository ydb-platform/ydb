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
 * $Id: SQLGetDiagRec.c,v 1.21 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetDiagRec.c,v $
 * Revision 1.21  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.20  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.19  2009/02/04 09:30:02  lurcher
 * Fix some SQLINTEGER/SQLLEN conflicts
 *
 * Revision 1.18  2008/09/29 14:02:45  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.17  2008/05/20 13:43:47  lurcher
 * Vms fixes
 *
 * Revision 1.16  2007/02/12 11:49:34  lurcher
 * Add QT4 support to existing GUI parts
 *
 * Revision 1.15  2006/11/27 14:08:34  lurcher
 * Sync up dirs
 *
 * Revision 1.14  2006/05/31 17:35:34  lurcher
 * Add unicode ODBCINST entry points
 *
 * Revision 1.13  2006/04/24 08:42:10  lurcher
 * Handle resetting statement descriptors to implicit values, by passing in NULL or the implicit descrptor  to SQLSetStmtAttr with the attribute SQL_ATTR_APP_PARAM_DESC or SQL_ATTR_APP_ROW_DESC. Also catch trying to call SQLGetDescField on a closed connection
 *
 * Revision 1.12  2005/12/19 18:43:26  lurcher
 * Add new parts to contrib and alter how the errors are returned from the driver
 *
 * Revision 1.11  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.10  2003/02/25 13:28:30  lurcher
 *
 * Allow errors on the drivers AllocHandle to be reported
 * Fix a problem that caused errors to not be reported in the log
 * Remove a redundant line from the spec file
 *
 * Revision 1.9  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.8  2002/11/13 15:59:20  lurcher
 *
 * More VMS changes
 *
 * Revision 1.7  2002/11/11 17:10:15  lurcher
 *
 * VMS changes
 *
 * Revision 1.6  2002/10/14 09:46:10  lurcher
 *
 * Remove extra return
 *
 * Revision 1.5  2002/10/08 13:36:07  lurcher
 *
 * Fix memory leak
 *
 * Revision 1.4  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.3  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
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
 * Revision 1.2  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.11  2000/06/23 16:11:38  ngorham
 *
 * Map ODBC 2 SQLSTATE values to ODBC 3
 *
 * Revision 1.10  1999/12/01 09:20:07  ngorham
 *
 * Fix some threading problems
 *
 * Revision 1.9  1999/11/17 21:08:58  ngorham
 *
 * Fix Bug with the ODBC 3 error handling
 *
 * Revision 1.8  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.7  1999/11/10 22:15:48  ngorham
 *
 * Fix some bugs with the DM and error reporting.
 *
 * Revision 1.6  1999/11/10 03:51:33  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.5  1999/09/21 22:34:25  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.4  1999/07/14 19:46:04  ngorham
 *
 * Fix the error logging when SQLError or SQLGetDiagRec returns SQL_NO_DATA
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
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.1  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLGetDiagRec.c,v $ $Revision: 1.21 $";

int __is_env( EHEAD * head )
{
    int type;

    memcpy( &type, head -> owning_handle, sizeof( type ));

    return type == HENV_MAGIC;
}

DMHDBC __get_connection( EHEAD * head )
{
    int type;

    memcpy( &type, head -> owning_handle, sizeof( type ));

    switch ( type )
    {
      case HDBC_MAGIC:
        {
            DMHDBC connection = ( DMHDBC ) head -> owning_handle;

            return connection;
        }

      case HSTMT_MAGIC:
        {
            DMHSTMT statement = ( DMHSTMT ) head -> owning_handle;

            return statement -> connection;
        }

      case HDESC_MAGIC:
        {
            DMHDESC descriptor = ( DMHDESC ) head -> owning_handle;

            return descriptor -> connection;
        }
    }
    return NULL;
}

int __get_version( EHEAD * head )
{
    int type;

    memcpy( &type, head -> owning_handle, sizeof( type ));

    switch ( type )
    {
      case HENV_MAGIC:
        {
            DMHENV environment = ( DMHENV ) head -> owning_handle;

            return environment -> requested_version;
        }

      case HDBC_MAGIC:
        {
            DMHDBC connection = ( DMHDBC ) head -> owning_handle;

            return connection -> environment -> requested_version;
        }

      case HSTMT_MAGIC:
        {
            DMHSTMT statement = ( DMHSTMT ) head -> owning_handle;

            return statement -> connection -> environment -> requested_version;
        }

      case HDESC_MAGIC:
        {
            DMHDESC descriptor = ( DMHDESC ) head -> owning_handle;

            return descriptor -> connection -> environment -> requested_version;
        }
    }
    return 0;
}
    

DRV_SQLHANDLE __get_driver_handle( EHEAD * head )
{
    int type;

    memcpy( &type, head -> owning_handle, sizeof( type ));

    switch ( type )
    {
      case HDBC_MAGIC:
        {
            DMHDBC connection = ( DMHDBC ) head -> owning_handle;

            return connection -> driver_dbc;
        }

      case HSTMT_MAGIC:
        {
            DMHSTMT statement = ( DMHSTMT ) head -> owning_handle;

            return statement -> driver_stmt;
        }

      case HDESC_MAGIC:
        {
            DMHDESC descriptor = ( DMHDESC ) head -> owning_handle;

            return descriptor -> driver_desc;
        }
    }
    return ( SQLHANDLE ) 0;
}

static SQLRETURN extract_sql_error_rec( EHEAD *head,
        SQLCHAR *sqlstate,
        SQLINTEGER rec_number,
        SQLINTEGER *native_error,
        SQLCHAR *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length )
{
    SQLRETURN ret;

    if ( sqlstate )
        strcpy((char*) sqlstate, "00000" );

    if ( rec_number <= head -> sql_diag_head.internal_count )
    {
        ERROR *ptr;
        SQLCHAR *as1 = NULL;

        ptr = head -> sql_diag_head.internal_list_head;
        while( rec_number > 1 )
        {
            ptr = ptr -> next;
            rec_number --;
        }

		if ( !ptr ) 
		{
	    	return SQL_NO_DATA;
		}

        as1 = (SQLCHAR*) unicode_to_ansi_alloc( ptr -> msg, SQL_NTS, __get_connection( head ), NULL );

        if ( sqlstate )
        {
            unicode_to_ansi_copy((char*) sqlstate, 6, ptr -> sqlstate, SQL_NTS, __get_connection( head ), NULL );
        }
        if ( buffer_length < strlen((char*) as1 ) + 1 )
        {
            ret = SQL_SUCCESS_WITH_INFO;
        }
        else
        {
            ret = SQL_SUCCESS;
        }

        if ( message_text && as1 )
        {
            if ( ret == SQL_SUCCESS )
            {
                strcpy((char*) message_text, (char*) as1 );
            }
            else
            {
                memcpy( message_text, as1, buffer_length );
                message_text[ buffer_length - 1 ] = '\0';
            }
        }

        if ( text_length && as1 )
        {
            *text_length = strlen((char*) as1 );
        }

        if ( native_error )
        {
            *native_error = ptr -> native_error;
        }

        /*
         * map 3 to 2 if required
         */

        if ( SQL_SUCCEEDED( ret ) && sqlstate )
            __map_error_state( (char*) sqlstate, __get_version( head ));

        if ( as1 )
        {
            free( as1 );
        }
        return ret;
    }
    else if ( !__is_env( head ) && __get_connection( head ) -> state != STATE_C2
        && head->sql_diag_head.error_count )
    {
        ERROR *ptr;
        SQLWCHAR *s1 = NULL, *s2 = NULL;

		rec_number -= head -> sql_diag_head.internal_count;

        s1 = malloc( sizeof( SQLWCHAR ) * ( 6 + 1 ));

        if ( buffer_length > 0 )
        {
            s2 = malloc( sizeof( SQLWCHAR ) * ( buffer_length + 1 ));
        }

        if ( __get_connection( head ) -> unicode_driver &&
            CHECK_SQLGETDIAGRECW( __get_connection( head )))
        {
            ret = SQLGETDIAGRECW( __get_connection( head ),
                    head -> handle_type,
                    __get_driver_handle( head ),
                    rec_number,
                    s1,
                    native_error,
                    s2,
                    buffer_length,
                    text_length );

            /*
             * map 3 to 2 if required
             */

            if ( SQL_SUCCEEDED( ret ) && sqlstate )
            {
                unicode_to_ansi_copy((char*) sqlstate, 6, s1, SQL_NTS, __get_connection( head ), NULL );
                __map_error_state((char*) sqlstate, __get_version( head ));
                if ( message_text )
                {
                    unicode_to_ansi_copy((char*) message_text, buffer_length, s2, SQL_NTS, __get_connection( head ), NULL );
                }
            }

        }
        else if ( !__get_connection( head ) -> unicode_driver &&
            CHECK_SQLGETDIAGREC( __get_connection( head )))
        {
            ret = SQLGETDIAGREC( __get_connection( head ),
                    head -> handle_type,
                    __get_driver_handle( head ),
                    rec_number,
                    sqlstate,
                    native_error,
                    message_text,
                    buffer_length,
                    text_length );

            /*
             * map 3 to 2 if required
             */

            if ( SQL_SUCCEEDED( ret ) && sqlstate )
                __map_error_state((char*) sqlstate, __get_version( head ));
        }
        else
        {
            SQLCHAR *as1 = NULL;

            ptr = head -> sql_diag_head.error_list_head;
            while( rec_number > 1 )
            {
                ptr = ptr -> next;
                rec_number --;
            }

			if ( !ptr ) 
			{
                if ( s1 )
                    free( s1 );

                if ( s2 )
                    free( s2 );

	    		return SQL_NO_DATA;
			}

            as1 = (SQLCHAR*) unicode_to_ansi_alloc( ptr -> msg, SQL_NTS, __get_connection( head ), NULL );

            if ( sqlstate )
            {
                unicode_to_ansi_copy((char*) sqlstate, 6, ptr -> sqlstate, SQL_NTS, __get_connection( head ), NULL );
            }
            if ( as1 && buffer_length < strlen((char*) as1 ) + 1 )
            {
                ret = SQL_SUCCESS_WITH_INFO;
            }
            else
            {
                ret = SQL_SUCCESS;
            }

            if ( message_text && as1 )
            {
                if ( ret == SQL_SUCCESS )
                {
                    strcpy((char*) message_text,(char*) as1 );
                }
                else
                {
                    memcpy( message_text, as1, buffer_length );
                    message_text[ buffer_length - 1 ] = '\0';
                }
            }

            if ( text_length && as1 )
            {
                *text_length = strlen((char*) as1 );
            }

            if ( native_error )
            {
                *native_error = ptr -> native_error;
            }

            /*
             * map 3 to 2 if required
             */

            if ( SQL_SUCCEEDED( ret ) && sqlstate )
                __map_error_state((char*) sqlstate, __get_version( head ));

            if ( as1 )
            {
                free( as1 );
            }
        }

        if ( s1 )
            free( s1 );

        if ( s2 )
            free( s2 );

        return ret;
    }
    else 
    {
	    return SQL_NO_DATA;
    }
}

SQLRETURN SQLGetDiagRecA( SQLSMALLINT handle_type,
        SQLHANDLE   handle,
        SQLSMALLINT rec_number,
        SQLCHAR     *sqlstate,
        SQLINTEGER  *native,
        SQLCHAR     *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length_ptr )
{
    return SQLGetDiagRec( handle_type,
                        handle,
                        rec_number,
                        sqlstate,
                        native,
                        message_text,
                        buffer_length,
                        text_length_ptr );
}

SQLRETURN SQLGetDiagRec( SQLSMALLINT handle_type,
        SQLHANDLE   handle,
        SQLSMALLINT rec_number,
        SQLCHAR     *sqlstate,
        SQLINTEGER  *native,
        SQLCHAR     *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length_ptr )
{
    SQLRETURN ret;
    SQLCHAR s0[ 64 ], s1[ 100 + LOG_MESSAGE_LEN ];
    SQLCHAR s2[ 100 + LOG_MESSAGE_LEN ];

    DMHENV environment = ( DMHENV ) handle;
    DMHDBC connection = NULL;
    DMHSTMT statement = NULL;
    DMHDESC descriptor = NULL;

    EHEAD *herror;
    char *handle_msg;
    const char *handle_type_ptr;

    if ( rec_number < 1 )
    {
        return SQL_ERROR;
    }

    switch ( handle_type )
    {
        case SQL_HANDLE_ENV:
            {
                if ( !__validate_env( environment ))
                {
                    dm_log_write( __FILE__,
                            __LINE__,
                            LOG_INFO,
                            LOG_INFO,
                            "Error: SQL_INVALID_HANDLE" );

                    return SQL_INVALID_HANDLE;
                }

                herror = &environment->error;
                handle_msg = environment->msg;
                handle_type_ptr = "Environment";
            }
            break;
        case SQL_HANDLE_DBC:
            {
                connection = ( DMHDBC ) handle;

                if (!__validate_dbc(connection))
                {
                    return SQL_INVALID_HANDLE;
                }

                herror = &connection->error;
                handle_msg = connection->msg;
                handle_type_ptr = "Connection";
            }
            break;

        case SQL_HANDLE_STMT:
            {
                statement = ( DMHSTMT ) handle;

                if ( !__validate_stmt( statement ))
                {
                    return SQL_INVALID_HANDLE;
                }

                connection = statement->connection;
                herror = &statement->error;
                handle_msg = statement->msg;
                handle_type_ptr = "Statement";
            }
            break;

        case SQL_HANDLE_DESC:
            {
                descriptor = ( DMHDESC ) handle;

                if ( !__validate_desc( descriptor ))
                {
                    return SQL_INVALID_HANDLE;
                }

                connection = descriptor->connection;
                herror = &descriptor->error;
                handle_msg = descriptor->msg;
                handle_type_ptr = "Descriptor";
            }
            break;

        default:
            {
                return SQL_NO_DATA;
            }
    }

    thread_protect( handle_type, handle );

    if ( log_info.log_flag )
    {
#ifdef HAVE_SNPRINTF
        snprintf( handle_msg, LOG_MSG_MAX*2,
#else
        sprintf( handle_msg,
#endif
            "\n\t\tEntry:\
\n\t\t\t%s = %p\
\n\t\t\tRec Number = %d\
\n\t\t\tSQLState = %p\
\n\t\t\tNative = %p\
\n\t\t\tMessage Text = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tText Len Ptr = %p",
                handle_type_ptr,
                handle,
                rec_number,
                sqlstate,
                native,
                message_text,
                buffer_length,
                text_length_ptr );

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                handle_msg );
    }

    /*
     * Do diag extraction here if defer flag is set.
     * Clean the flag after extraction.
     */
    if ( connection && herror->defer_extract )
    {
        extract_error_from_driver( herror, connection, herror->ret_code_deferred, 0 );

        herror->defer_extract = 0;
        herror->ret_code_deferred = 0;
    }

    ret = extract_sql_error_rec( herror,
            sqlstate,
            rec_number,
            native,
            message_text,
            buffer_length,
            text_length_ptr );

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
                    sqlstate ? sqlstate : (SQLCHAR *)"NULL",
                    __iptr_as_string( s0, native ),
                    __sdata_as_string( s1, SQL_CHAR,
                                      text_length_ptr, message_text ));
        }
        else
        {
#ifdef HAVE_SNPRINTF
            snprintf( handle_msg, LOG_MSG_MAX*2,
#else
            sprintf( handle_msg,
#endif
                    "\n\t\tExit:[%s]",
                    __get_return_status( ret, s1 ));
        }

        dm_log_write( __FILE__,
                __LINE__,
                LOG_INFO,
                LOG_INFO,
                handle_msg );
    }

    thread_release( handle_type, handle );

    return ret;


}

