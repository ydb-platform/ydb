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
 * $Id: SQLGetInfoW.c,v 1.14 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetInfoW.c,v $
 * Revision 1.14  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.13  2008/08/29 08:01:39  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.12  2007/02/28 15:37:48  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.11  2005/10/06 08:50:58  lurcher
 * Fix problem with SQLDrivers not returning first entry
 *
 * Revision 1.10  2004/11/22 17:02:49  lurcher
 * Fix unicode/ansi conversion in the SQLGet functions
 *
 * Revision 1.9  2004/11/20 13:21:38  lurcher
 * Fix unicode bug in SQLGetInfoW
 *
 * Revision 1.8  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.7  2003/03/05 09:48:44  lurcher
 *
 * Add some 64 bit fixes
 *
 * Revision 1.6  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
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

static char const rcsid[]= "$RCSfile: SQLGetInfoW.c,v $";

SQLRETURN SQLGetInfoW( SQLHDBC connection_handle,
           SQLUSMALLINT info_type,
           SQLPOINTER info_value,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    SQLRETURN ret = SQL_SUCCESS;
    int type;
    char txt[ 30 ], *cptr;
    SQLPOINTER *ptr;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];
	SQLUSMALLINT sval;

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

				if ( CHECK_SQLGETINFOW( parent_connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

					return SQLGETINFOW( parent_connection, 
							connection_handle, 
           					info_type,
           					info_value,
           					buffer_length,
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
\n\t\t\tInfo Type = %s\
\n\t\t\tInfo Value = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tStrLen = %p",
                connection,
                __info_as_string( s1, info_type ),
                info_value, 
                (int)buffer_length,
                (void*)string_length );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    if ( info_type != SQL_ODBC_VER &&
            info_type != SQL_DM_VER &&
            connection -> state == STATE_C2 )
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
    else if ( connection -> state == STATE_C3 )
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

    if ( buffer_length < 0 )
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

    switch ( info_type )
    {
      case SQL_DATA_SOURCE_NAME:
        type = 1;
        cptr = connection -> dsn;
        break;

      case SQL_DM_VER:
        type = 1;
        sprintf( txt, "%02d.%02d.%04d.%04d",
                SQL_SPEC_MAJOR, SQL_SPEC_MINOR, 
                atoi( VERSION ), atoi( VERSION + 2 ));
        cptr = txt;
        break;

      case SQL_ODBC_VER:
        type = 1;
        sprintf( txt, "%02d.%02d",
                SQL_SPEC_MAJOR, SQL_SPEC_MINOR );
        cptr = txt;
        break;

      case SQL_DRIVER_HDBC:
        type = 2;
        ptr = (SQLPOINTER) connection -> driver_dbc;
        break;

      case SQL_DRIVER_HENV:
        type = 2;
        ptr = (SQLPOINTER) connection -> driver_env;
        break;

      case SQL_DRIVER_HDESC:
        {
            DMHDESC hdesc;
            if ( info_value && __validate_desc ( hdesc = *(DMHDESC*) info_value ) )
            {
                type = 2;

                ptr = (SQLPOINTER) hdesc -> driver_desc;
            }
            else
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
        }
        break;

      case SQL_DRIVER_HLIB:
        type = 2;
        ptr = connection -> dl_handle;
        break;

      case SQL_DRIVER_HSTMT:
        {
            DMHSTMT hstmt;
            if ( info_value && __validate_stmt( hstmt = *(DMHSTMT*)info_value ) )
            {
                type = 2;

                ptr = (SQLPOINTER) hstmt -> driver_stmt;
            }
            else
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
        }
        break;

      case SQL_XOPEN_CLI_YEAR:
        type = 1;
        cptr = "1994";
        break;

	  case SQL_ATTR_DRIVER_THREADING:
		type = 3;
		sval = connection -> threading_level;
		break;

      default:
        /*
         * pass all the others on
         */
        if ( connection -> unicode_driver ||
			CHECK_SQLGETINFOW( connection ))
        {
            if ( !CHECK_SQLGETINFOW( connection ))
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

            ret = SQLGETINFOW( connection,
                    connection -> driver_dbc,
                    info_type,
                    info_value,
                    buffer_length,
                    string_length );

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
        else
        {
            SQLCHAR *as1 = NULL;

            if ( !CHECK_SQLGETINFO( connection ))
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

            switch( info_type )
            {
              case SQL_ACCESSIBLE_PROCEDURES:
              case SQL_ACCESSIBLE_TABLES:
              case SQL_CATALOG_NAME:
              case SQL_CATALOG_NAME_SEPARATOR:
              case SQL_CATALOG_TERM:
              case SQL_COLLATION_SEQ:
              case SQL_COLUMN_ALIAS:
              case SQL_DATA_SOURCE_NAME:
              case SQL_DATA_SOURCE_READ_ONLY:
              case SQL_DATABASE_NAME:
              case SQL_DBMS_NAME:
              case SQL_DBMS_VER:
              case SQL_DESCRIBE_PARAMETER:
              case SQL_DRIVER_NAME:
              case SQL_DRIVER_ODBC_VER:
              case SQL_DRIVER_VER:
              case SQL_ODBC_VER:
              case SQL_EXPRESSIONS_IN_ORDERBY:
              case SQL_IDENTIFIER_QUOTE_CHAR:
              case SQL_INTEGRITY:
              case SQL_KEYWORDS:
              case SQL_LIKE_ESCAPE_CLAUSE:
              case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
              case SQL_MULT_RESULT_SETS:
              case SQL_MULTIPLE_ACTIVE_TXN:
              case SQL_NEED_LONG_DATA_LEN:
              case SQL_ORDER_BY_COLUMNS_IN_SELECT:
              case SQL_PROCEDURE_TERM:
              case SQL_PROCEDURES:
              case SQL_ROW_UPDATES:
              case SQL_SCHEMA_TERM:
              case SQL_SEARCH_PATTERN_ESCAPE:
              case SQL_SERVER_NAME:
              case SQL_SPECIAL_CHARACTERS:
              case SQL_TABLE_TERM:
              case SQL_USER_NAME:
              case SQL_XOPEN_CLI_YEAR:
              case SQL_OUTER_JOINS:
                if ( SQL_SUCCEEDED( ret ) && info_value && buffer_length > 0 )
                {
                    as1 = malloc( buffer_length + 1 );
                }
                break;
            }

            ret = SQLGETINFO( connection,
                    connection -> driver_dbc,
                    info_type,
                    as1 ? as1 : info_value,
                    buffer_length,
                    string_length );

            switch( info_type )
            {
              case SQL_ACCESSIBLE_PROCEDURES:
              case SQL_ACCESSIBLE_TABLES:
              case SQL_CATALOG_NAME:
              case SQL_CATALOG_NAME_SEPARATOR:
              case SQL_CATALOG_TERM:
              case SQL_COLLATION_SEQ:
              case SQL_COLUMN_ALIAS:
              case SQL_DATA_SOURCE_NAME:
              case SQL_DATA_SOURCE_READ_ONLY:
              case SQL_DATABASE_NAME:
              case SQL_DBMS_NAME:
              case SQL_DBMS_VER:
              case SQL_DESCRIBE_PARAMETER:
              case SQL_DRIVER_NAME:
              case SQL_DRIVER_ODBC_VER:
              case SQL_DRIVER_VER:
              case SQL_ODBC_VER:
              case SQL_EXPRESSIONS_IN_ORDERBY:
              case SQL_IDENTIFIER_QUOTE_CHAR:
              case SQL_INTEGRITY:
              case SQL_KEYWORDS:
              case SQL_LIKE_ESCAPE_CLAUSE:
              case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
              case SQL_MULT_RESULT_SETS:
              case SQL_MULTIPLE_ACTIVE_TXN:
              case SQL_NEED_LONG_DATA_LEN:
              case SQL_ORDER_BY_COLUMNS_IN_SELECT:
              case SQL_PROCEDURE_TERM:
              case SQL_PROCEDURES:
              case SQL_ROW_UPDATES:
              case SQL_SCHEMA_TERM:
              case SQL_SEARCH_PATTERN_ESCAPE:
              case SQL_SERVER_NAME:
              case SQL_SPECIAL_CHARACTERS:
              case SQL_TABLE_TERM:
              case SQL_USER_NAME:
              case SQL_XOPEN_CLI_YEAR:
              case SQL_OUTER_JOINS:
                if ( SQL_SUCCEEDED( ret ) && info_value && as1 )
                {
                    ansi_to_unicode_copy( info_value, (char*) as1, SQL_NTS, connection, NULL );
				}
				if ( SQL_SUCCEEDED( ret ) && string_length )
				{
					*string_length *= sizeof( SQLWCHAR );
                }
                break;
            }

            if ( as1 ) free( as1 );

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

        return function_return( SQL_HANDLE_DBC, connection, ret, DEFER_R3 );
    }

    if ( type == 1 )
    {
        SQLWCHAR *s1;
        int len;

        s1 = ansi_to_unicode_alloc((SQLCHAR*) cptr, SQL_NTS, connection, NULL );

        len = strlen( cptr ) * sizeof( SQLWCHAR );

        if ( string_length )
            *string_length = len;

        if ( info_value )
        {
            if ( buffer_length > len + 1 )
            {
                wide_strcpy( info_value, s1 );
            }
            else
            {
                memcpy( info_value, s1, ( buffer_length - 1 * sizeof( SQLWCHAR )));
                ((SQLWCHAR*)info_value)[ buffer_length - 1 ] = '\0';
                ret = SQL_SUCCESS_WITH_INFO;
            }
        }

        if ( s1 )
            free( s1 );
    }
    else if ( type == 2 )
    {
        if ( info_value )
            *((void **)info_value) = ptr;

        if ( string_length )
            *string_length = sizeof( SQLPOINTER );
    }
	else if ( type == 3 ) 
	{
        if ( info_value )
            *((SQLUSMALLINT *)info_value) = sval;

        if ( string_length )
            *string_length = sizeof( SQLUSMALLINT );
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

    return function_return_nodrv( SQL_HANDLE_DBC, connection, ret );
}
