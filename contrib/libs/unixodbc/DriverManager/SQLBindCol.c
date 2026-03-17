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
 * $Id: SQLBindCol.c,v 1.8 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLBindCol.c,v $
 * Revision 1.8  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.7  2007/03/05 09:49:23  lurcher
 * Get it to build on VMS again
 *
 * Revision 1.6  2006/04/11 10:22:56  lurcher
 * Fix a data type check
 *
 * Revision 1.5  2006/03/08 11:22:13  lurcher
 * Add check for valid C_TYPE
 *
 * Revision 1.4  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.3  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.2  2001/12/13 13:00:31  lurcher
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
 * Revision 1.2  2001/04/12 17:43:35  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.9  1999/11/13 23:40:58  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.8  1999/10/24 23:54:17  ngorham
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
 * Revision 1.5  1999/09/21 22:34:23  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.4  1999/07/10 21:10:15  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.3  1999/07/04 21:05:06  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/30 23:56:54  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:05  sShandyb
 * first go at it
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.3  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.2  1999/04/29 20:47:37  nick
 * Another checkpoint
 *
 * Revision 1.1  1999/04/25 23:02:41  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLBindCol.c,v $ $Revision: 1.8 $";

int check_target_type( int c_type, int connection_mode) 
{
    /*
     * driver defined types
     */
    if ( connection_mode >= SQL_OV_ODBC3_80 && c_type >= 0x4000 && c_type <= 0x7FFF ) {
        return 1;
    }

	switch( c_type ) {
		case SQL_C_CHAR:
		case SQL_C_LONG:
		case SQL_C_SHORT:
		case SQL_C_FLOAT:
		case SQL_C_NUMERIC:
		case SQL_C_DEFAULT:
		case SQL_C_DATE:
		case SQL_C_TIME:
		case SQL_C_TIMESTAMP:
		case SQL_C_TYPE_DATE:
		case SQL_C_TYPE_TIME:
		case SQL_C_TYPE_TIMESTAMP:
		case SQL_C_INTERVAL_YEAR:
		case SQL_C_INTERVAL_MONTH:
		case SQL_C_INTERVAL_DAY:
		case SQL_C_INTERVAL_HOUR:
		case SQL_C_INTERVAL_MINUTE:
		case SQL_C_INTERVAL_SECOND:
		case SQL_C_INTERVAL_YEAR_TO_MONTH:
		case SQL_C_INTERVAL_DAY_TO_HOUR:
		case SQL_C_INTERVAL_DAY_TO_MINUTE:
		case SQL_C_INTERVAL_DAY_TO_SECOND:
		case SQL_C_INTERVAL_HOUR_TO_MINUTE:
		case SQL_C_INTERVAL_HOUR_TO_SECOND:
		case SQL_C_INTERVAL_MINUTE_TO_SECOND:
		case SQL_C_BINARY:
		case SQL_C_BIT:
		case SQL_C_SBIGINT:
		case SQL_C_UBIGINT:
		case SQL_C_TINYINT:
		case SQL_C_SLONG:
		case SQL_C_SSHORT:
		case SQL_C_STINYINT:
		case SQL_C_ULONG:
		case SQL_C_USHORT:
		case SQL_C_UTINYINT:
		case SQL_C_GUID:
		case SQL_C_WCHAR:
		case SQL_C_DOUBLE:
#if (ODBCVER >= 0x0400)
        case SQL_TYPE_TIME_WITH_TIMEZONE:
        case SQL_TYPE_TIMESTAMP_WITH_TIMEZONE:
        case SQL_UDT:
        case SQL_ROW:
        case SQL_ARRAY:
        case SQL_MULTISET:
        case SQL_VARIANT_TYPE:
#endif /* ODBCVER >= 0x0400 */
#if (ODBCVER >= 0x0300)
        case SQL_ARD_TYPE:
#endif
#if (ODBCVER >= 0x0380)
        case SQL_APD_TYPE:
#endif
        /*
         * MS Added types
         */
        case -150:  /* SQL_SS_VARIANT */
        case -151:  /* SQL_SS_UDT */
        case -152:  /* SQL_SS_XML */
        case -153:  /* SQL_SS_TABLE */
        case -154:  /* SQL_SS_TIME2 */
        case -155:  /* SQL_SS_TIMESTAMPOFFSET */
			return 1;

		default:
			return 0;
	}
}

SQLRETURN SQLBindCol( SQLHSTMT statement_handle,
		   SQLUSMALLINT column_number,
           SQLSMALLINT target_type,
		   SQLPOINTER target_value,
           SQLLEN buffer_length,
	   	   SQLLEN *strlen_or_ind )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret;

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
\n\t\t\tTarget Value = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tStrLen Or Ind = %p", 
                statement,
                column_number,
                target_type,
                __sql_as_text( target_type ),
                target_value,
                (int)buffer_length,
                (void*)strlen_or_ind );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    thread_protect( SQL_HANDLE_STMT, statement );

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
     * TO_DO
     * Check the type against a bookmark
     * check that the length is 4 for a odbc 2 bookmark
     * remember thats its bound for SQLGetData checks
     */

    /*
     * check states
     */

    if ( statement -> state == STATE_S8 ||
            statement -> state == STATE_S9 ||
            statement -> state == STATE_S10 ||
            statement -> state == STATE_S11 ||
            statement -> state == STATE_S12 ||
            statement -> state == STATE_S13 ||
            statement -> state == STATE_S14 ||
            statement -> state == STATE_S12 )
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
	 * check valid C_TYPE
     * Its possible to call with the indicator and buffer NULL to unbind without setting the type
	 */

	if (( target_value || strlen_or_ind ) && !check_target_type( target_type, statement -> connection -> environment -> requested_version ))
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

    if ( !CHECK_SQLBINDCOL( statement -> connection ))
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

    ret = SQLBINDCOL( statement -> connection ,
            statement -> driver_stmt,
		    column_number,
             __map_type(MAP_C_DM2D,statement->connection,target_type),
		    target_value,
            buffer_length,
	   	    strlen_or_ind );

    if ( log_info.log_flag )
    {
        SQLCHAR buf[ 128 ];

        sprintf( statement -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, buf ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    return function_return( SQL_HANDLE_STMT, statement, ret, DEFER_R3 );
}
