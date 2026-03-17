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
 * $Id: SQLDescribeColW.c,v 1.14 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLDescribeColW.c,v $
 * Revision 1.14  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.13  2008/08/29 08:01:38  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.12  2008/05/20 13:43:47  lurcher
 * Vms fixes
 *
 * Revision 1.11  2007/04/02 10:50:18  lurcher
 * Fix some 64bit problems (only when sizeof(SQLLEN) == 8 )
 *
 * Revision 1.10  2007/02/28 15:37:47  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.9  2007/01/02 10:27:50  lurcher
 * Fix descriptor leak with unicode only driver
 *
 * Revision 1.8  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.7  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.6  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.5  2002/08/19 09:11:49  lurcher
 *
 * Fix Maxor ineffiecny in Postgres Drivers, and fix a return state
 *
 * Revision 1.4  2002/07/24 08:49:51  lurcher
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
 * Revision 1.4  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2001/03/21 12:26:27  nick
 *
 * Alter def for SQLDescribeColW
 *
 * Revision 1.1  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLDescribeColW.c,v $";

SQLRETURN SQLDescribeColW( SQLHSTMT statement_handle,
           SQLUSMALLINT column_number,
           SQLWCHAR *column_name,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *name_length,
           SQLSMALLINT *data_type,
           SQLULEN *column_size,
           SQLSMALLINT *decimal_digits,
           SQLSMALLINT *nullable )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ], s2[ 100 + LOG_MESSAGE_LEN ], s3[ 100 + LOG_MESSAGE_LEN ], s4[ 100 + LOG_MESSAGE_LEN ];
    SQLCHAR s5[ 100 + LOG_MESSAGE_LEN ];
    SQLCHAR s6[ 100 + LOG_MESSAGE_LEN ];

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

#ifdef WITH_HANDLE_REDIRECT
		{
			DMHSTMT parent_statement;

			parent_statement = find_parent_handle( statement, SQL_HANDLE_STMT );

			if ( parent_statement ) {
        		dm_log_write( __FILE__, 
                	__LINE__, 
                    	LOG_INFO, 
                    	LOG_INFO, 
                    	"Info: found parent handle" );

				if ( CHECK_SQLDESCRIBECOLW( parent_statement -> connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

                	return  SQLDESCRIBECOLW( parent_statement -> connection,
							statement_handle,
							column_number,
							column_name,
							buffer_length,
							name_length,
							data_type,
							column_size,
							decimal_digits,
							nullable );
				}
			}
		}
#endif
        return SQL_INVALID_HANDLE;
    }

    function_entry( statement );

    if ( log_info.log_flag )
    {
        sprintf( statement -> msg, "\n\t\tEntry:\
\n\t\t\tStatement = %p\
\n\t\t\tColumn Number = %d\
\n\t\t\tColumn Name = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tName Length = %p\
\n\t\t\tData Type = %p\
\n\t\t\tColumn Size = %p\
\n\t\t\tDecimal Digits = %p\
\n\t\t\tNullable = %p",
                statement,
                column_number,
                column_name,
                buffer_length,
                name_length,
                data_type,
                column_size,
                decimal_digits,
                nullable );

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
                SQL_API_SQLDESCRIBECOL );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    /*
     * sadly we can't trust the numcols value 
     *
    if ( statement -> numcols < column_number )
    {
        __post_internal_error( &statement -> error,
                ERROR_07009, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
     */

    if ( buffer_length < 0 )
    {
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
     * check states
     */

    if ( statement -> state == STATE_S1 ||
            statement -> state == STATE_S8 ||
            statement -> state == STATE_S9 ||
            statement -> state == STATE_S10 ||
            statement -> state == STATE_S13 ||
            statement -> state == STATE_S14 ||
            statement -> state == STATE_S15 )
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
    /* 
     * This seems to be down to the driver in the MS DM
     *
    else if ( statement -> state == STATE_S2 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 07005" );

        __post_internal_error( &statement -> error,
                ERROR_07005, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
    */
    else if ( statement -> state == STATE_S4 )
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
    else if ( statement -> state == STATE_S8 ||
            statement -> state == STATE_S9 ||
            statement -> state == STATE_S10 )
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
        if ( statement -> interupted_func != SQL_API_SQLDESCRIBECOL )
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

    if ( statement -> connection -> unicode_driver ||
      CHECK_SQLDESCRIBECOLW( statement -> connection ))
    {
        if ( !CHECK_SQLDESCRIBECOLW( statement -> connection ))
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

        ret = SQLDESCRIBECOLW( statement -> connection,
                statement -> driver_stmt,
                column_number,
                column_name,
                buffer_length,
                name_length,
                data_type,
                column_size,
                decimal_digits,
                nullable );
    }
    else
    {
        SQLCHAR *as1 = NULL;

        if ( !CHECK_SQLDESCRIBECOL( statement -> connection ))
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

        if ( buffer_length > 0 && column_name )
        {
            as1 = malloc( buffer_length + 1 );
        }

        ret = SQLDESCRIBECOL( statement -> connection,
                statement -> driver_stmt,
                column_number,
                as1 ? as1 : (SQLCHAR*)column_name,
                buffer_length,
                name_length,
                data_type,
                column_size,
                decimal_digits,
                nullable );

        if ( column_name && as1 )
        {
            ansi_to_unicode_copy( column_name, (char*) as1, SQL_NTS, statement -> connection, NULL );
        }

        if ( as1 )
        {
            free( as1 );
        }
    }

    if ( (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) && data_type ) 
    {
        *data_type=__map_type(MAP_SQL_D2DM,statement->connection, *data_type);
    }

    if ( ret == SQL_STILL_EXECUTING )
    {
        statement -> interupted_func = SQL_API_SQLDESCRIBECOL;
        if ( statement -> state != STATE_S11 &&
                statement -> state != STATE_S12 )
            statement -> state = STATE_S11;
    }

    if ( log_info.log_flag )
    {
		if ( !SQL_SUCCEEDED( ret )) {
        	sprintf( statement -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, s6 ));
		}
		else {
        	sprintf( statement -> msg, 
                "\n\t\tExit:[%s]\
                \n\t\t\tColumn Name = %s\
                \n\t\t\tData Type = %s\
                \n\t\t\tColumn Size = %s\
                \n\t\t\tDecimal Digits = %s\
                \n\t\t\tNullable = %s",
                    __get_return_status( ret, s6 ),
                    __sdata_as_string( s1, SQL_WCHAR, 
                        name_length, column_name ),
                    __sptr_as_string( s2, data_type ),
                    __ptr_as_string( s3, (SQLLEN*)column_size ),
                    __sptr_as_string( s4, decimal_digits ),
                    __sptr_as_string( s5, nullable ));
		}

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    return function_return( SQL_HANDLE_STMT, statement, ret, DEFER_R3 );
}
