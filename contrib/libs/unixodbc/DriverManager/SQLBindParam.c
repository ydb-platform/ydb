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
 * $Id: SQLBindParam.c,v 1.9 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLBindParam.c,v $
 * Revision 1.9  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.8  2007/03/05 09:49:23  lurcher
 * Get it to build on VMS again
 *
 * Revision 1.7  2006/04/11 10:22:56  lurcher
 * Fix a data type check
 *
 * Revision 1.6  2006/03/08 11:22:13  lurcher
 * Add check for valid C_TYPE
 *
 * Revision 1.5  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.4  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.3  2002/08/19 09:11:49  lurcher
 *
 * Fix Maxor ineffiecny in Postgres Drivers, and fix a return state
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
 * Revision 1.5  1999/09/21 22:34:24  ngorham
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
 * Revision 1.3  1999/05/09 23:27:11  nick
 * All the API done now
 *
 * Revision 1.2  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.1  1999/04/25 23:02:41  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLBindParam.c,v $ $Revision: 1.9 $";

SQLRETURN SQLBindParam( SQLHSTMT statement_handle,
           SQLUSMALLINT parameter_number,
           SQLSMALLINT value_type,
           SQLSMALLINT parameter_type,
           SQLULEN length_precision,
           SQLSMALLINT parameter_scale,
           SQLPOINTER parameter_value,
           SQLLEN *strlen_or_ind)
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
\n\t\t\tParam Number = %d\
\n\t\t\tValue Type = %d %s\
\n\t\t\tParameter Type = %d %s\
\n\t\t\tLength Precision = %d\
\n\t\t\tParameter Scale = %d\
\n\t\t\tParameter Value = %p\
\n\t\t\tStrLen Or Ind = %p", 
                statement,
                parameter_number,
                value_type,
                __c_as_text( value_type ),
                parameter_type,
                __sql_as_text( parameter_type ),
                (int)length_precision,
                (int)parameter_scale,
                (void*)parameter_value,
                (void*)strlen_or_ind );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    thread_protect( SQL_HANDLE_STMT, statement );

    if ( parameter_number < 1 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 07009" );

        __post_internal_error_api( &statement -> error,
                ERROR_07009, NULL,
                statement -> connection -> environment -> requested_version,
                SQL_API_SQLBINDPARAM );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if ( parameter_value == NULL &&
            strlen_or_ind == NULL )
    {
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
	 * check valid C_TYPE
	 */

	if ( !check_target_type( value_type, statement -> connection -> environment -> requested_version ))
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

    if ( CHECK_SQLBINDPARAM( statement -> connection ))
    {
        ret = SQLBINDPARAM( statement -> connection,
                statement -> driver_stmt,
                parameter_number,
                __map_type(MAP_C_DM2D,statement->connection,value_type),
                __map_type(MAP_SQL_DM2D,statement->connection,parameter_type),
                length_precision,
                parameter_scale,
                parameter_value,
                strlen_or_ind );
    }
    else
    {
        /*
         * map to odbc 3 operation
         */

        if ( !CHECK_SQLBINDPARAMETER( statement -> connection ))
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

        /*
         * this probably needs to work out the
         * buffer length
         */

        ret = SQLBINDPARAMETER( statement -> connection,
                statement -> driver_stmt,
                parameter_number,
                SQL_PARAM_INPUT,
                __map_type(MAP_C_DM2D,statement->connection,value_type),
                __map_type(MAP_SQL_DM2D,statement->connection,parameter_type),
                length_precision,
                parameter_scale,
                parameter_value,
                0,
                strlen_or_ind );
    }

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
