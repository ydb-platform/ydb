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
 * $Id: SQLSetStmtOption.c,v 1.10 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLSetStmtOption.c,v $
 * Revision 1.10  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.9  2009/02/04 09:30:02  lurcher
 * Fix some SQLINTEGER/SQLLEN conflicts
 *
 * Revision 1.8  2007/11/29 12:00:31  lurcher
 * Add 64 bit type changes to SQLExtendedFetch etc
 *
 * Revision 1.7  2005/11/23 08:29:16  lurcher
 * Add cleanup in postgres driver
 *
 * Revision 1.6  2005/04/05 09:11:31  lurcher
 * The config string being passed into ConfigDsn was wrong, removed semicolon, and added terminating double null
 *
 * Revision 1.5  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.4  2003/03/05 09:48:45  lurcher
 *
 * Add some 64 bit fixes
 *
 * Revision 1.3  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
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
 * Revision 1.4  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 * Revision 1.3  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.8  2000/06/20 13:30:12  ngorham
 *
 * Fix problems when using bookmarks
 *
 * Revision 1.7  1999/11/13 23:41:01  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.6  1999/10/24 23:54:19  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.5  1999/09/21 22:34:26  ngorham
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
 * Revision 1.1.1.1  1999/05/29 13:41:09  sShandyb
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

static char const rcsid[]= "$RCSfile: SQLSetStmtOption.c,v $ $Revision: 1.10 $";

SQLRETURN SQLSetStmtOptionA( SQLHSTMT statement_handle,
           SQLUSMALLINT option,
           SQLULEN value )
{
	return SQLSetStmtOption( statement_handle, option, value );
}

SQLRETURN SQLSetStmtOption( SQLHSTMT statement_handle,
           SQLUSMALLINT option,
           SQLULEN value )
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
\n\t\t\tOption = %s\
\n\t\t\tValue = %d",
                statement,
                __stmt_attr_as_string( s1, option ),
                (int)value );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    thread_protect( SQL_HANDLE_STMT, statement );

    /*
     * check states
     */

    if ( option == SQL_CONCURRENCY ||
            option == SQL_CURSOR_TYPE ||
            option == SQL_SIMULATE_CURSOR ||
            option == SQL_USE_BOOKMARKS )
    {
        if ( statement -> state == STATE_S2 ||
                statement -> state == STATE_S3 )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: S1011" );

            __post_internal_error( &statement -> error,
                 ERROR_S1011, NULL,
                 statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
        else if ( statement -> state == STATE_S4 ||
                statement -> state == STATE_S5 ||
                statement -> state == STATE_S6 ||
                statement -> state == STATE_S7 )
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
                statement -> state == STATE_S11 ||
                statement -> state == STATE_S12 ||
                statement -> state == STATE_S13 ||
                statement -> state == STATE_S14 ||
                statement -> state == STATE_S15 )
        {
            if ( statement -> prepared )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: S1011" );

                __post_internal_error( &statement -> error,
                     ERROR_S1011, NULL,
                     statement -> connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
            }
            else
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: S1010" );

                __post_internal_error( &statement -> error,
                     ERROR_S1010, NULL,
                     statement -> connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
            }
        }
    }
    else
    {
        if ( statement -> state == STATE_S8 ||
                statement -> state == STATE_S9 ||
                statement -> state == STATE_S10 ||
                statement -> state == STATE_S11 ||
                statement -> state == STATE_S12 )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: S1010" );

            __post_internal_error( &statement -> error,
                 ERROR_S1010, NULL,
                 statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
    }

    if ( option == SQL_ATTR_IMP_ROW_DESC || 
        option == SQL_ATTR_IMP_PARAM_DESC )
    {
        dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY017" );

        __post_internal_error( &statement -> error,
                ERROR_HY017, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    /*
     * is it a legitimate value
     */
    ret = dm_check_statement_attrs( statement, option, (SQLPOINTER)value );

    if ( ret != SQL_SUCCESS ) 
    {
        dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY011" );

        __post_internal_error( &statement -> error,
                ERROR_HY024, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    /*
     * is it something overridden
     */

    value = (SQLULEN) __attr_override( statement, SQL_HANDLE_STMT, option, (void*) value, NULL );

    if ( CHECK_SQLSETSTMTOPTION( statement -> connection ))
    {
        ret = SQLSETSTMTOPTION( statement -> connection,
                statement -> driver_stmt,
                option,
                value );
    }
    else if ( CHECK_SQLSETSTMTATTR( statement -> connection ))
    {
        switch ( option )
        {
          case SQL_ATTR_APP_PARAM_DESC:
            if ( value )
                memcpy( &statement -> apd, (void*)value, 
                        sizeof( statement -> apd ));

            ret = SQL_SUCCESS;
            break;

          case SQL_ATTR_APP_ROW_DESC:
            if ( value )
                memcpy( &statement -> ard, (void*)value, 
                        sizeof( statement -> ard ));

            ret = SQL_SUCCESS;
            break;

          case SQL_ATTR_IMP_PARAM_DESC:
            if ( value )
                memcpy( &statement -> ipd, (void*)value, 
                        sizeof( statement -> ipd ));

            ret = SQL_SUCCESS;
            break;

          case SQL_ATTR_IMP_ROW_DESC:
            if ( value )
                memcpy( &statement -> ird, (void*)value, 
                        sizeof( statement -> ird ));

            ret = SQL_SUCCESS;
            break;

          default:
            ret = SQLSETSTMTATTR( statement -> connection,
                    statement -> driver_stmt,
                    option,
                    (SQLPOINTER)(intptr_t) value,
                    SQL_NTS );
            break;
        }
    }
    else if ( CHECK_SQLSETSTMTATTRW( statement -> connection ))
    {
        switch ( option )
        {
          case SQL_ATTR_APP_PARAM_DESC:
            if ( value )
                memcpy( &statement -> apd, (void*)value, 
                        sizeof( statement -> apd ));

            ret = SQL_SUCCESS;
            break;

          case SQL_ATTR_APP_ROW_DESC:
            if ( value )
                memcpy( &statement -> ard, (void*)value, 
                        sizeof( statement -> ard ));

            ret = SQL_SUCCESS;
            break;

          case SQL_ATTR_IMP_PARAM_DESC:
            if ( value )
                memcpy( &statement -> ipd, (void*)value, 
                        sizeof( statement -> ipd ));

            ret = SQL_SUCCESS;
            break;

          case SQL_ATTR_IMP_ROW_DESC:
            if ( value )
                memcpy( &statement -> ird, (void*)value, 
                        sizeof( statement -> ird ));

            ret = SQL_SUCCESS;
            break;

          default:
            ret = SQLSETSTMTATTRW( statement -> connection,
                    statement -> driver_stmt,
                    option,
                    (SQLPOINTER)(intptr_t) value,
                    SQL_NTS );
            break;
        }
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

    /*
     * take notice of this
     */

    if ( option == SQL_USE_BOOKMARKS && SQL_SUCCEEDED( ret ))
    {
        statement -> bookmarks_on = (SQLULEN) value;
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
