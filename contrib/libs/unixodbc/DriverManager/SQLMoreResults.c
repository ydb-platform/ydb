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
 * $Id: SQLMoreResults.c,v 1.8 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLMoreResults.c,v $
 * Revision 1.8  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.7  2006/03/08 09:18:41  lurcher
 * fix silly typo that was using sizeof( SQL_WCHAR ) instead of SQLWCHAR
 *
 * Revision 1.6  2003/12/19 16:25:38  lurcher
 *
 * Fix incorrect state in SQLMoreResults.c
 *
 * Revision 1.5  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.4  2003/01/27 15:01:01  lurcher
 *
 * On error from SQLMoreResults DONT change to S1
 *
 * Revision 1.3  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.2  2002/08/15 08:10:33  lurcher
 *
 * Couple of small fixes from John L Miller
 *
 * Revision 1.1.1.1  2001/10/17 16:40:06  lurcher
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
 * Revision 1.9  2000/06/20 12:44:00  ngorham
 *
 * Fix bug that caused a success with info message from SQLExecute or
 * SQLExecDirect to be lost if used with a ODBC 3 driver and the application
 * called SQLGetDiagRec
 *
 * Revision 1.8  2000/06/16 16:52:18  ngorham
 *
 * Stop info messages being lost when calling SQLExecute etc on ODBC 3
 * drivers, the SQLNumResultCols were clearing the error before
 * function return had a chance to get to them
 *
 * Revision 1.7  1999/11/13 23:41:00  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
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
 * Revision 1.3  1999/07/04 21:05:08  ngorham
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
 * Revision 1.2  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLMoreResults.c,v $ $Revision: 1.8 $";

SQLRETURN SQLMoreResults( SQLHSTMT statement_handle )
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
\n\t\t\tStatement = %p",
                statement );

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
    if ( statement -> state == STATE_S1 ||
            /* statement -> state == STATE_S2 || */
            statement -> state == STATE_S3 )
    {
        sprintf( statement -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( SQL_NO_DATA, s1 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_NO_DATA );
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
    else if ( statement -> state == STATE_S11 ||
            statement -> state == STATE_S12 )
    {
        if ( statement -> interupted_func != SQL_API_SQLMORERESULTS )
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

#ifdef NR_PROBE
    if ( !CHECK_SQLMORERESULTS( statement -> connection ) ||
            !CHECK_SQLNUMRESULTCOLS( statement -> connection ))
#else
    if ( !CHECK_SQLMORERESULTS( statement -> connection ))
#endif
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

    ret = SQLMORERESULTS( statement -> connection ,
            statement -> driver_stmt );

    if ( SQL_SUCCEEDED( ret ))
    { 
#ifdef NR_PROBE
        /*
         * grab any errors
         */

        if ( ret == SQL_SUCCESS_WITH_INFO )
        {
            function_return_ex( IGNORE_THREAD, statement, ret, TRUE, DEFER_R3 );
        }

        SQLNUMRESULTCOLS( statement -> connection,
                statement -> driver_stmt, &statement -> numcols );

        if ( statement -> numcols == 0 )
        {
            statement -> state = STATE_S4;
        }
        else
        {
            statement -> state = STATE_S5;
        }
#else
        /*
         * We don't know for sure
         */
        statement -> hascols = 0;
        statement -> state = STATE_S5;
#endif
    }
    else if ( ret == SQL_STILL_EXECUTING )
    {
        statement -> interupted_func = SQL_API_SQLEXECUTE;
        if ( statement -> state != STATE_S11 &&
                statement -> state != STATE_S12 )
            statement -> state = STATE_S11;
    }
    else if ( ret == SQL_NO_DATA )
    {
        if ( statement -> prepared )
        {
            if ( statement -> state == STATE_S4 )
            {
                statement -> state = STATE_S2;
            }
            else
            {
                statement -> state = STATE_S3;
            }
        }
        else
        {
            statement -> state = STATE_S1;
        }
    }
    else if ( ret == SQL_NEED_DATA )
    {
        statement -> interupted_func = SQL_API_SQLMORERESULTS;
        statement -> interupted_state = statement -> state;
        statement -> state = STATE_S8;
    }
    else if ( ret == SQL_PARAM_DATA_AVAILABLE )
    {
        statement -> interupted_func = SQL_API_SQLMORERESULTS;
        statement -> interupted_state = statement -> state;
        statement -> state = STATE_S13;
    }
    else
    {
        /*
         * Leave the state where it is 
         */
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
