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
 * $Id: SQLSetParam.c,v 1.7 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLSetParam.c,v $
 * Revision 1.7  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.6  2005/04/26 08:40:35  lurcher
 *
 * Add data type mapping for SQLSetPos.
 * Remove out of date macro in sqlext.h
 *
 * Revision 1.5  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.4  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.3  2002/08/19 09:11:49  lurcher
 *
 * Fix Maxor ineffiecny in Postgres Drivers, and fix a return state
 *
 * Revision 1.2  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.1.1.1  2001/10/17 16:40:07  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.8  1999/11/13 23:41:01  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.7  1999/10/29 21:07:40  ngorham
 *
 * Fix some stupid bugs in the DM
 * Make the postgres driver work via unix sockets
 *
 * Revision 1.6  1999/10/24 23:54:19  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.5  1999/09/21 22:34:25  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.4  1999/07/10 21:10:17  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.3  1999/07/04 21:05:08  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/30 23:56:55  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:08  sShandyb
 * first go at it
 *
 * Revision 1.4  1999/06/03 22:20:25  ngorham
 *
 * Finished off the ODBC3-2 mapping
 *
 * Revision 1.3  1999/06/02 20:12:10  ngorham
 *
 * Fixed botched log entry, and removed the dos \r from the sql header files.
 *
 * Revision 1.2  1999/06/02 19:57:21  ngorham
 *
 * Added code to check if a attempt is being made to compile with a C++
 * Compiler, and issue a message.
 * Start work on the ODBC2-3 conversions.
 *
 * Revision 1.1.1.1  1999/05/27 18:23:18  pharvey
 * Imported sources
 *
 * Revision 1.2  1999/05/09 23:27:11  nick
 * All the API done now
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLSetParam.c,v $ $Revision: 1.7 $";

int check_value_type( int c_type, int connection_mode)
{
    /*
 *      * driver defined types
 *           */
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
                case SQL_ARD_TYPE:
                case SQL_C_DOUBLE:
                /* case SQL_C_XML: still trying to find what value this is */
                        return 1;

                default:
                        return 0;
        }
}

SQLRETURN SQLSetParam( SQLHSTMT statement_handle,
           SQLUSMALLINT parameter_number,
           SQLSMALLINT value_type,
           SQLSMALLINT parameter_type,
           SQLULEN length_precision,
           SQLSMALLINT parameter_scale,
           SQLPOINTER parameter_value,
           SQLLEN *strlen_or_ind )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

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
                SQL_API_SQLSETPARAM );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if (!check_value_type( value_type, statement -> connection -> environment -> requested_version ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY003" );

        __post_internal_error_api( &statement -> error,
                ERROR_HY003, NULL,
                statement -> connection -> environment -> requested_version,
                SQL_API_SQLSETPARAM );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if ( parameter_value == NULL && strlen_or_ind == NULL && value_type != SQL_PARAM_OUTPUT )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY009" );

        __post_internal_error_api( &statement -> error,
                ERROR_HY009, NULL,
                statement -> connection -> environment -> requested_version,
                SQL_API_SQLSETPARAM );

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

    if ( CHECK_SQLSETPARAM( statement -> connection ))
    {
        ret = SQLSETPARAM( statement -> connection,
                statement -> driver_stmt,
                parameter_number,
                __map_type(MAP_C_DM2D,statement->connection,value_type),
                __map_type(MAP_SQL_DM2D,statement->connection,parameter_type),
                length_precision,
                parameter_scale,
                parameter_value,
                strlen_or_ind );
    }
    else if ( CHECK_SQLBINDPARAMETER( statement -> connection ))
    {
        ret = SQLBINDPARAMETER( statement -> connection,
                statement -> driver_stmt,
                parameter_number,
                SQL_PARAM_INPUT_OUTPUT,
                __map_type(MAP_C_DM2D,statement->connection,value_type),
                __map_type(MAP_SQL_DM2D,statement->connection,parameter_type),
                length_precision,
                parameter_scale,
                parameter_value,
                SQL_SETPARAM_VALUE_MAX,
                strlen_or_ind );
    }
    else if ( CHECK_SQLBINDPARAM( statement -> connection ))
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

    if ( log_info.log_flag )
    {
        sprintf( statement -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, s1 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    return function_return( SQL_HANDLE_STMT, statement, ret, DEFER_R3 );
}
