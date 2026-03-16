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
 * $Id: SQLGetData.c,v 1.15 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetData.c,v $
 * Revision 1.15  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.14  2007/04/02 10:50:19  lurcher
 * Fix some 64bit problems (only when sizeof(SQLLEN) == 8 )
 *
 * Revision 1.13  2006/04/11 10:22:56  lurcher
 * Fix a data type check
 *
 * Revision 1.12  2006/03/08 11:22:13  lurcher
 * Add check for valid C_TYPE
 *
 * Revision 1.11  2004/02/02 10:10:45  lurcher
 *
 * Fix some connection pooling problems
 * Include sqlucode in sqlext
 *
 * Revision 1.10  2003/10/30 18:20:46  lurcher
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
 * Revision 1.7  2002/08/19 09:11:49  lurcher
 *
 * Fix Maxor ineffiecny in Postgres Drivers, and fix a return state
 *
 * Revision 1.6  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.5  2002/07/10 15:05:57  lurcher
 *
 * Alter the return code in the Postgres driver, for a warning, it should be
 * 01000 it was 00000
 * Fix a problem in DriverManagerII with the postgres driver as the driver
 * doesn't return a propper list of schemas
 * Allow the delimiter to be set in isql to a hex/octal char not just a
 * printable one
 *
 * Revision 1.4  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.3  2001/12/04 10:16:59  lurcher
 *
 * Fix SQLSetScrollOption problem
 *
 * Revision 1.2  2001/11/22 14:27:02  lurcher
 *
 * Add UNICODE conversion fix
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.5  2001/09/27 17:05:48  nick
 *
 * Assorted fixes and tweeks
 *
 * Revision 1.4  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2001/01/01 11:04:13  nick
 *
 * Add UNICODE conversion to SQLGetData
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.10  2000/06/20 13:30:09  ngorham
 *
 * Fix problems when using bookmarks
 *
 * Revision 1.9  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.8  1999/10/24 23:54:18  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.7  1999/10/09 00:56:16  ngorham
 *
 * Added Manush's patch to map ODBC 3-2 datetime values
 *
 * Revision 1.6  1999/10/09 00:15:58  ngorham
 *
 * Add mapping from SQL_TYPE_X to SQL_X and SQL_C_TYPE_X to SQL_C_X
 * when the driver is a ODBC 2 one
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
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.4  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.3  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.2  1999/04/29 20:47:37  nick
 * Another checkpoint
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLGetData.c,v $ $Revision: 1.15 $";

SQLRETURN SQLGetData( SQLHSTMT statement_handle,
           SQLUSMALLINT column_number,
           SQLSMALLINT target_type,
           SQLPOINTER target_value,
           SQLLEN buffer_length,
           SQLLEN *strlen_or_ind )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ], s2[ 100 + LOG_MESSAGE_LEN ];
    int unicode_switch = 0;
    SQLLEN ind_value;
    SQLCHAR *as1 = NULL;
    SQLCHAR s3[ 100 + LOG_MESSAGE_LEN ];

    /*
     * check statement
     */

    if ( !__validate_stmt( statement ))
    {
        dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

        return SQL_INVALID_HANDLE;
    }

    function_entry( statement );

    if ( log_info.log_flag )
    {
        sprintf( statement -> msg, "\n\t\tEntry:\
\n\t\t\tStatement = %p\
\n\t\t\tColumn Number = %d\
\n\t\t\tTarget Type = %d %s\
\n\t\t\tBuffer Length = %d\
\n\t\t\tTarget Value = %p\
\n\t\t\tStrLen Or Ind = %p",
                statement,
                column_number,
                target_type, 
                __sql_as_text( target_type ),
                (int)buffer_length,
                target_value,
                (void*)strlen_or_ind );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    thread_protect( SQL_HANDLE_STMT, statement );

    if ( column_number == 0 &&
            statement -> bookmarks_on == SQL_UB_OFF && statement -> connection -> bookmarks_on == SQL_UB_OFF )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 07009" );

        __post_internal_error_api( &statement -> error,
                ERROR_07009, NULL,
                statement -> connection -> environment -> requested_version,
                SQL_API_SQLGETDATA );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    /*
     * can't trust the numcols value
     *
    if ( statement -> numcols < column_number )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 07009" );

        __post_internal_error( &statement -> error,
                ERROR_07009, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
     */

    /*
     * check states
     */

    if ( statement -> state == STATE_S1 ||
            statement -> state == STATE_S2 ||
            statement -> state == STATE_S3 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &statement -> error,
                ERROR_HY010, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
    else if ( statement -> state == STATE_S4 ||
            statement -> state == STATE_S5 ||
            (( statement -> state == STATE_S6 || statement -> state == STATE_S7 )
            && statement -> eod ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 24000" );

        __post_internal_error( &statement -> error,
                ERROR_24000, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
    else if ( statement -> state == STATE_S8 ||
            statement -> state == STATE_S9 ||
            statement -> state == STATE_S10 ||
            statement -> state == STATE_S13 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &statement -> error,
                ERROR_HY010, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if ( statement -> state == STATE_S11 ||
            statement -> state == STATE_S12 )
    {
        if ( statement -> interupted_func != SQL_API_SQLGETDATA )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY010" );

            __post_internal_error( &statement -> error,
                    ERROR_HY010, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
    }

    if ( target_value == NULL ) {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY009" );

        __post_internal_error( &statement -> error,
                ERROR_HY009, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if ( buffer_length < 0 ) {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY090" );

        __post_internal_error( &statement -> error,
                ERROR_HY090, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    /*
     * TO_DO assorted checks need adding here, relating to bound columns
     * and what sort of SQLGetData extensions the driver supports
     */

	/*
	 * check valid C_TYPE
	 */

	if ( !check_target_type( target_type, statement -> connection -> environment -> requested_version ))
	{
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY003" );

        __post_internal_error( &statement -> error,
                ERROR_HY003, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
	}

    if ( !CHECK_SQLGETDATA( statement -> connection ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: IM001" );

        __post_internal_error( &statement -> error,
                ERROR_IM001, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if (statement -> connection -> driver_act_ver==SQL_OV_ODBC2)
    {
        switch( target_type )
        {
          case SQL_WCHAR:
            target_type = SQL_CHAR;
            unicode_switch = 1;
            buffer_length = buffer_length / sizeof( SQLWCHAR );
            break;

          case SQL_WVARCHAR:
            target_type = SQL_VARCHAR;
            unicode_switch = 1;
            buffer_length = buffer_length / sizeof( SQLWCHAR );
            break;

          case SQL_WLONGVARCHAR:
            target_type = SQL_LONGVARCHAR;
            unicode_switch = 1;
            buffer_length = buffer_length / sizeof( SQLWCHAR );
            break;
        }
    }

    if ( unicode_switch )
    {
        if ( buffer_length > 0 && target_value )
        {
            as1 = malloc( buffer_length + 1 );

            ret = SQLGETDATA( statement -> connection,
                statement -> driver_stmt,
                column_number,
                __map_type(MAP_C_DM2D,statement->connection,target_type),
                as1,
                buffer_length,
                &ind_value );
        }
        else
        {
            ret = SQLGETDATA( statement -> connection,
                statement -> driver_stmt,
                column_number,
                __map_type(MAP_C_DM2D,statement->connection,target_type),
                target_value,
                buffer_length,
                &ind_value );
        }
        
    }
    else
    {
        ret = SQLGETDATA( statement -> connection,
            statement -> driver_stmt,
            column_number,
            __map_type(MAP_C_DM2D,statement->connection,target_type),
            target_value,
            buffer_length,
            strlen_or_ind );
    }

    if ( ret == SQL_STILL_EXECUTING )
    {
        statement -> interupted_func = SQL_API_SQLGETDATA;
        if ( statement -> state != STATE_S11 &&
                statement -> state != STATE_S12 )
        {
            statement -> interupted_state = statement -> state;
            statement -> state = STATE_S11;
        }
    }
    else if ( SQL_SUCCEEDED( ret ) && unicode_switch )
    {
        if ( target_value && ind_value >= 0 && as1 )
        {
            if ( ind_value > buffer_length )
            {
                ansi_to_unicode_copy( target_value, (char*) as1, buffer_length, statement -> connection, NULL );
            }
            else
            {
                ansi_to_unicode_copy( target_value, (char*) as1, ind_value + 1, statement -> connection, NULL );
            }
        }

        if ( as1 )
        {
            free( as1 );
        }

        if ( ind_value > 0 )
        {
            ind_value *= sizeof( SQLWCHAR );
        }

        if ( strlen_or_ind )
        {
            *strlen_or_ind = ind_value;
        }
    }

    if ( ret != SQL_STILL_EXECUTING
        && (statement -> state == STATE_S11 || statement -> state == STATE_S12) )
    {
        statement -> state = statement -> interupted_state;
    }

    if ( statement -> state == STATE_S14 ) {
        statement -> state = STATE_S15;
    }

    if ( log_info.log_flag )
    {
        sprintf( statement -> msg, 
                "\n\t\tExit:[%s]\
                \n\t\t\tBuffer = %s\
                \n\t\t\tStrlen Or Ind = %s",
                    __get_return_status( ret, s3 ),
                    __data_as_string( s1, target_type, 
                        strlen_or_ind, target_value ),
                    __ptr_as_string( s2, strlen_or_ind ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    return function_return( SQL_HANDLE_STMT, statement, ret, DEFER_R3 );
}
