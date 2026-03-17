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
 * $Id: SQLGetInfo.c,v 1.14 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetInfo.c,v $
 * Revision 1.14  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.13  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.12  2008/09/29 14:02:45  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.11  2007/01/02 10:29:18  lurcher
 * Fix descriptor leak with unicode only driver
 *
 * Revision 1.10  2006/08/31 12:44:52  lurcher
 * Check in for 2.2.12 release
 *
 * Revision 1.9  2006/01/06 18:44:35  lurcher
 * Couple of unicode fixes
 *
 * Revision 1.8  2005/10/06 08:50:58  lurcher
 * Fix problem with SQLDrivers not returning first entry
 *
 * Revision 1.7  2004/11/22 17:02:49  lurcher
 * Fix unicode/ansi conversion in the SQLGet functions
 *
 * Revision 1.6  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.5  2003/03/05 09:48:44  lurcher
 *
 * Add some 64 bit fixes
 *
 * Revision 1.4  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.3  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.2  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.9  2001/07/31 12:03:46  nick
 *
 * Fix how the DM gets the CLI year for SQLGetInfo
 * Fix small bug in strncasecmp
 *
 * Revision 1.8  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.7  2001/04/23 13:58:43  nick
 *
 * Assorted tweeks to text driver to get it to work with StarOffice
 *
 * Revision 1.6  2001/04/18 15:03:37  nick
 *
 * Fix problem when going to DB2 unicode driver
 *
 * Revision 1.5  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.4  2001/01/12 19:43:12  nick
 *
 * Fixed UNICODE conversion bug in SQLGetInfo
 *
 * Revision 1.3  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.2  2000/09/08 08:58:17  nick
 *
 * Add SQL_DRIVER_HDESC to SQLGetinfo
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.9  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.8  1999/11/10 03:51:34  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.7  1999/10/29 21:07:40  ngorham
 *
 * Fix some stupid bugs in the DM
 * Make the postgres driver work via unix sockets
 *
 * Revision 1.6  1999/10/24 23:54:18  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.5  1999/09/21 22:34:25  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.4  1999/07/10 21:10:16  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
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
 * Revision 1.1.1.1  1999/05/27 18:23:18  pharvey
 * Imported sources
 *
 * Revision 1.2  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLGetInfo.c,v $ $Revision: 1.14 $";

SQLRETURN SQLGetInfoA( SQLHDBC connection_handle,
           SQLUSMALLINT info_type,
           SQLPOINTER info_value,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length )
{
    return SQLGetInfo( connection_handle,
                info_type,
                info_value,
                buffer_length,
                string_length );
}

SQLRETURN SQLGetInfoInternal( SQLHDBC connection_handle,
           SQLUSMALLINT info_type,
           SQLPOINTER info_value,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length,
           int do_checks )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    SQLRETURN ret = SQL_SUCCESS;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];
    int type;
	SQLUSMALLINT sval;
    char txt[ 30 ], *cptr;
    SQLPOINTER *ptr;

    if ( do_checks )
    {
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
\n\t\t\tInfo Type = %s (%d)\
\n\t\t\tInfo Value = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tStrLen = %p",
                    connection,
                    __info_as_string( s1, info_type ),
                    info_type,
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

                return do_checks ? function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR ) : SQL_ERROR;
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

                return do_checks ? function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR ) : SQL_ERROR;
            }
        }
        break;

      case SQL_XOPEN_CLI_YEAR:
        type = 1;
        cptr = connection -> cli_year;
        break;

	  case SQL_ATTR_DRIVER_THREADING:
		type = 3;
		sval = connection -> threading_level;
		break;

      default:
        /*
         * pass all the others on
         */

        if ( connection -> unicode_driver )
        {
            SQLWCHAR *s1 = NULL;

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

                return do_checks ? function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR ) : SQL_ERROR;
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
                if ( info_value && buffer_length > 0 )
                {
					buffer_length = sizeof( SQLWCHAR ) * ( buffer_length + 1 );
                    s1 = malloc( buffer_length );
                }
                break;
            }

            ret = SQLGETINFOW( connection,
                    connection -> driver_dbc,
                    info_type,
                    s1 ? s1 : info_value,
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
                if ( SQL_SUCCEEDED( ret ) && info_value && s1 )
                {
                    unicode_to_ansi_copy( info_value, buffer_length, s1, SQL_NTS, connection, NULL  );
                }
				if ( SQL_SUCCEEDED( ret ) && string_length && info_value ) 
				{
					*string_length = strlen(info_value);
				}
                break;
            }
            if ( s1 )
            {
                free( s1 );
            }
        }
        else
        {
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

                return do_checks ? function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR ) : SQL_ERROR;
            }

            ret = SQLGETINFO( connection,
                    connection -> driver_dbc,
                    info_type,
                    info_value,
                    buffer_length,
                    string_length );
        }

        return do_checks ? function_return( SQL_HANDLE_DBC, connection, ret, DEFER_R3 ) : ret;
    }

    if ( type == 1 )
    {
        if ( string_length )
            *string_length = strlen( cptr );

        if ( info_value )
        {
            if ( buffer_length > strlen( cptr ) + 1 )
            {
                strcpy( info_value, cptr );
            }
            else
            {
                memcpy( info_value, cptr, buffer_length - 1 );
                ((char*)info_value)[ buffer_length - 1 ] = '\0';
                ret = SQL_SUCCESS_WITH_INFO;
            }
        }
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

    return do_checks ? function_return_nodrv( SQL_HANDLE_DBC, connection, ret ) : ret;
}

SQLRETURN __SQLGetInfo( SQLHDBC connection_handle,
           SQLUSMALLINT info_type,
           SQLPOINTER info_value,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length )
{
    return SQLGetInfoInternal( connection_handle, info_type, info_value,
        buffer_length, string_length, 0);
}

SQLRETURN SQLGetInfo( SQLHDBC connection_handle,
           SQLUSMALLINT info_type,
           SQLPOINTER info_value,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length )
{
    return SQLGetInfoInternal( connection_handle, info_type, info_value,
        buffer_length, string_length, 1);
}
