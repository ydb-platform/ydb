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
 * $Id: SQLGetDiagRecW.c,v 1.11 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetDiagRecW.c,v $
 * Revision 1.11  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.10  2009/02/04 09:30:02  lurcher
 * Fix some SQLINTEGER/SQLLEN conflicts
 *
 * Revision 1.9  2007/11/26 11:37:23  lurcher
 * Sync up before tag
 *
 * Revision 1.8  2007/02/28 15:37:48  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.7  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.6  2002/11/11 17:10:17  lurcher
 *
 * VMS changes
 *
 * Revision 1.5  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
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

static char const rcsid[]= "$RCSfile: SQLGetDiagRecW.c,v $";

extern int __is_env( EHEAD * head );        /* in SQLGetDiagRec.c */

SQLRETURN extract_parent_handle_rec( DRV_SQLHANDLE handle,
        int         handle_type,
        SQLSMALLINT rec_number,
        SQLWCHAR    *sqlstate,
        SQLINTEGER  *native,
        SQLWCHAR    *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length_ptr )
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
            DMHDESC *parent_handle_desc;

            switch ( handle_type )
            {
                case SQL_HANDLE_DBC:
                    {
                        parent_handle_dbc = find_parent_handle( handle, handle_type );
                    }
                    break;

                case SQL_HANDLE_STMT:
                    {
                        parent_handle_stmt = find_parent_handle( handle, handle_type );
                        parent_handle_dbc = parent_handle_stmt->connection;
                    }
                    break;

                case SQL_HANDLE_DESC:
                    {
                        parent_handle_desc = find_parent_handle( handle, handle_type );
                        parent_handle_dbc = parent_handle_desc->connection;
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

                if ( CHECK_SQLGETDIAGRECW( parent_handle_dbc ))
                {
                    dm_log_write( __FILE__,
                            __LINE__,
                            LOG_INFO,
                            LOG_INFO,
                            "Info: calling redirected driver function" );

                    return SQLGETDIAGRECW( parent_handle_dbc,
                            handle_type,
                            handle,
                            rec_number,
                            sqlstate,
                            native,
                            message_text,
                            buffer_length,
                            text_length_ptr );
                }
            }
        }
#endif
    }

    return SQL_INVALID_HANDLE;
}

static SQLRETURN extract_sql_error_rec_w( EHEAD *head,
        SQLWCHAR *sqlstate,
        SQLINTEGER rec_number,
        SQLINTEGER *native_error,
        SQLWCHAR *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length )
{
    SQLRETURN ret;

    if ( sqlstate )
    {
        SQLWCHAR *tmp;

        tmp = ansi_to_unicode_alloc((SQLCHAR*) "00000", SQL_NTS, __get_connection( head ), NULL );
        wide_strcpy( sqlstate, tmp );
        free( tmp );
    }

    if ( rec_number <= head -> sql_diag_head.internal_count )
    {
        ERROR *ptr;

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

        if ( sqlstate )
        {
            wide_strcpy( sqlstate, ptr -> sqlstate );
        }
        if ( buffer_length < wide_strlen( ptr -> msg ) + 1 )
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
                wide_strcpy( message_text, ptr -> msg );
            }
            else
            {
                memcpy( message_text, ptr -> msg, buffer_length * 2 );
                message_text[ buffer_length - 1 ] = '\0';
            }
        }

        if ( text_length )
        {
            *text_length = wide_strlen( ptr -> msg );
        }

        if ( native_error )
        {
            *native_error = ptr -> native_error;
        }

        /*
         * map 3 to 2 if required
         */

        if ( SQL_SUCCEEDED( ret ) && sqlstate )
            __map_error_state_w(sqlstate, __get_version( head ));

        return ret;
    }
    else if ( !__is_env( head ) && __get_connection( head ) -> state != STATE_C2
        && head->sql_diag_head.error_count )
    {
        ERROR *ptr;
        rec_number -= head -> sql_diag_head.internal_count;

        if ( __get_connection( head ) -> unicode_driver &&
            CHECK_SQLGETDIAGRECW( __get_connection( head )))
        {
            ret = SQLGETDIAGRECW( __get_connection( head ),
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
            {
                __map_error_state_w( sqlstate, __get_version( head ));
            }

            return ret;
        }
        else if ( !__get_connection( head ) -> unicode_driver &&
            CHECK_SQLGETDIAGREC( __get_connection( head )))
        {
            SQLCHAR *as1 = NULL, *as2 = NULL;

            if ( sqlstate )
            {
                as1 = malloc( 7 );
            }

            if ( message_text && buffer_length > 0 )
            {
                as2 = malloc( buffer_length + 1 );
            }
                
            ret = SQLGETDIAGREC( __get_connection( head ),
                    head -> handle_type,
                    __get_driver_handle( head ),
                    rec_number,
                    as1 ? as1 : (SQLCHAR *)sqlstate,
                    native_error,
                    as2 ? as2 : (SQLCHAR *)message_text,
                    buffer_length,
                    text_length );

            /*
             * map 3 to 2 if required
             */

            if ( SQL_SUCCEEDED( ret ) && sqlstate )
            {
                if ( as1 )
                {
                    ansi_to_unicode_copy( sqlstate,(char*) as1, SQL_NTS, __get_connection( head ), NULL );
                    __map_error_state_w( sqlstate, __get_version( head ));
                }
                if ( message_text )
                {
                    if ( as2 )
                    {
                        ansi_to_unicode_copy( message_text,(char*) as2, SQL_NTS, __get_connection( head ), NULL );
                    }
                }
            }

            if ( as1 ) free( as1 );
            if ( as2 ) free( as2 );

            return ret;
        }
        else
        {
            ptr = head -> sql_diag_head.error_list_head;
            while( rec_number > 1 )
            {
                ptr = ptr -> next;
                rec_number --;
            }

			if ( !ptr ) 
			{
	    		return SQL_NO_DATA;
			}

            if ( sqlstate )
            {
                wide_strcpy( sqlstate, ptr -> sqlstate );
            }
            if ( buffer_length < wide_strlen( ptr -> msg ) + 1 )
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
                    wide_strcpy( message_text, ptr -> msg );
                }
                else
                {
                    memcpy( message_text, ptr -> msg, buffer_length * 2 );
                    message_text[ buffer_length - 1 ] = '\0';
                }
            }

            if ( text_length )
            {
                *text_length = wide_strlen( ptr -> msg );
            }

            if ( native_error )
            {
                *native_error = ptr -> native_error;
            }

            /*
             * map 3 to 2 if required
             */

            if ( SQL_SUCCEEDED( ret ) && sqlstate )
                __map_error_state_w( sqlstate, __get_version( head ));

            return ret;
        }
    }
    else
    {
        return SQL_NO_DATA;
    }
}

SQLRETURN SQLGetDiagRecW( SQLSMALLINT handle_type,
        SQLHANDLE   handle,
        SQLSMALLINT rec_number,
        SQLWCHAR     *sqlstate,
        SQLINTEGER  *native,
        SQLWCHAR     *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length_ptr )
{
    SQLRETURN ret;
    SQLCHAR s0[ 64 ], s1[ 100 + LOG_MESSAGE_LEN ];
    SQLCHAR s2[ 100 + LOG_MESSAGE_LEN ];
    SQLCHAR s3[ 100 + LOG_MESSAGE_LEN ];

    DMHENV environment = ( DMHENV ) handle;
    DMHDBC connection = NULL;
    DMHSTMT statement = NULL;
    DMHDESC descriptor = NULL;

    EHEAD  *herror;
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
                    return extract_parent_handle_rec( environment, handle_type, rec_number, sqlstate, native, message_text, buffer_length, text_length_ptr );
                }

                herror = &environment->error;
                handle_msg = environment->msg;
                handle_type_ptr = "Environment";
            }
            break;
        case SQL_HANDLE_DBC:
            {
                connection = ( DMHDBC ) handle;

                if ( !__validate_dbc( connection ))
                {
                    return extract_parent_handle_rec( connection, handle_type, rec_number, sqlstate, native, message_text, buffer_length, text_length_ptr);
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
                   return extract_parent_handle_rec( statement, handle_type, rec_number, sqlstate, native, message_text, buffer_length, text_length_ptr );
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
                   return extract_parent_handle_rec( descriptor, handle_type, rec_number, sqlstate, native, message_text, buffer_length, text_length_ptr );
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
        sprintf( handle_msg,
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

    ret = extract_sql_error_rec_w( herror,
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
            char *ts1, *ts2;

            sprintf( handle_msg,
                "\n\t\tExit:[%s]\
\n\t\t\tSQLState = %s\
\n\t\t\tNative = %s\
\n\t\t\tMessage Text = %s",
                    __get_return_status( ret, s2 ),
                    __sdata_as_string( s3, SQL_CHAR,
                                      NULL, ts1 = unicode_to_ansi_alloc(sqlstate, SQL_NTS, connection, NULL )),
                    __iptr_as_string( s0, native ),
                    __sdata_as_string( s1, SQL_CHAR,
                                      text_length_ptr, ts2 = unicode_to_ansi_alloc(message_text, SQL_NTS, connection, NULL )));

            if ( ts1 )
            {
                free( ts1 );
            }
            if ( ts2 )
            {
                free( ts2 );
            }
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

    thread_release( handle_type, handle );

    return ret;

}

