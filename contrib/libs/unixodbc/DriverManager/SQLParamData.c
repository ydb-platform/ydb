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
 * $Id: SQLParamData.c,v 1.7 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLParamData.c,v $
 * Revision 1.7  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.6  2007/05/25 16:42:32  lurcher
 * Sync up
 *
 * Revision 1.5  2005/11/21 17:25:43  lurcher
 * A few DM fixes for Oracle's ODBC driver
 *
 * Revision 1.4  2004/05/07 09:53:13  lurcher
 *
 *
 * Fix potential problrm in stats if creating a semaphore fails
 * Alter state after SQLParamData from S4 to S5
 *
 * Revision 1.3  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.2  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
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
 * Revision 1.10  2000/06/20 12:44:00  ngorham
 *
 * Fix bug that caused a success with info message from SQLExecute or
 * SQLExecDirect to be lost if used with a ODBC 3 driver and the application
 * called SQLGetDiagRec
 *
 * Revision 1.9  2000/06/16 16:52:18  ngorham
 *
 * Stop info messages being lost when calling SQLExecute etc on ODBC 3
 * drivers, the SQLNumResultCols were clearing the error before
 * function return had a chance to get to them
 *
 * Revision 1.8  2000/05/21 21:49:19  ngorham
 *
 * Assorted fixes
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
 * Revision 1.1.1.1  1999/05/29 13:41:08  sShandyb
 * first go at it
 *
 * Revision 1.1.1.1  1999/05/27 18:23:18  pharvey
 * Imported sources
 *
 * Revision 1.2  1999/05/04 22:41:12  nick
 * and another night ends
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLParamData.c,v $ $Revision: 1.7 $";

SQLRETURN SQLParamData( SQLHSTMT statement_handle,
           SQLPOINTER *value )
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
\n\t\t\tValue = %p",
                statement,
                value );

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
            statement -> state == STATE_S2 ||
            statement -> state == STATE_S3 ||
            statement -> state == STATE_S4 ||
            statement -> state == STATE_S5 ||
            statement -> state == STATE_S6 ||
            statement -> state == STATE_S7 ||
            statement -> state == STATE_S9 ||
            statement -> state == STATE_S14 )
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
        if ( statement -> interupted_func != SQL_API_SQLPARAMDATA )
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

    if ( !CHECK_SQLPARAMDATA( statement -> connection ))
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
	 * When a NULL is passed, driver tries to access this memory and dumps core,
	 * so pass a vaild pointer to the driver. This mirrors what the MS DM does
	 */


	if (!value)
	{
		statement -> valueptr = NULL;
		value = &statement -> valueptr;
	}

    ret = SQLPARAMDATA( statement -> connection,
            statement -> driver_stmt,
            value );

    if ( ret == SQL_STILL_EXECUTING )
    {
        statement -> interupted_func = SQL_API_SQLPARAMDATA;
        if ( statement -> state != STATE_S11 &&
                statement -> state != STATE_S12 )
            statement -> state = STATE_S11;
    }
    else if ( SQL_SUCCEEDED( ret ))
    {
        if ( statement -> interupted_func == SQL_API_SQLEXECDIRECT ||
                statement -> interupted_func == SQL_API_SQLEXECUTE || 
                statement -> interupted_func == SQL_API_SQLMORERESULTS )
        {
#ifdef NR_PROBE
            SQLRETURN local_ret;

            /*
             * grab any errors
             */

            if ( ret == SQL_SUCCESS_WITH_INFO )
            {
                function_return_ex( IGNORE_THREAD, statement, ret, TRUE, DEFER_R0 );
            }

            local_ret = SQLNUMRESULTCOLS( statement -> connection,
                    statement -> driver_stmt, &statement -> hascols );

            if ( statement -> hascols > 0 )
                    statement -> state = STATE_S5;
            else
                    statement -> state = STATE_S4;
#else
            statement -> hascols = 1;
            statement -> state = STATE_S5;
#endif
        }
        else if ( statement -> interupted_func ==
            SQL_API_SQLSETPOS &&
            statement -> interupted_state == STATE_S7 )
        {
            statement -> state = STATE_S7;
        }
        else if ( statement -> interupted_func ==
                SQL_API_SQLBULKOPERATIONS &&
                statement -> interupted_state == STATE_S5 )
        {
            statement -> state = STATE_S5;
        }
        else
        {
            statement -> state = STATE_S6;
            statement -> eod = 0;
        }
    }
    else if ( ret == SQL_NEED_DATA )
    {
        statement -> state = STATE_S9;
    }
    else if ( ret == SQL_PARAM_DATA_AVAILABLE  )
    {
        statement -> state = STATE_S14;
    }
	else if ( ret == SQL_NO_DATA )
	{
		statement -> interupted_func = 0;
		statement -> state = STATE_S4;
	}
    else
    {
        if ( statement -> interupted_func == SQL_API_SQLEXECDIRECT )
        {
            statement -> state = STATE_S1;
        }
        else if ( statement -> interupted_func == SQL_API_SQLEXECUTE &&
                statement -> hascols )
        {
            statement -> state = STATE_S3;
        }
        else if ( statement -> interupted_func == SQL_API_SQLEXECUTE )
        {
            statement -> state = STATE_S2;
        }
        else if ( statement -> interupted_func ==
                SQL_API_SQLBULKOPERATIONS &&
                statement -> interupted_state == STATE_S5 )
        {
            statement -> state = STATE_S5;
        }
        else if ( statement -> interupted_func ==
                SQL_API_SQLSETPOS &&
                statement -> interupted_state == STATE_S7 )
        {
            statement -> state = STATE_S7;
        }
        else
        {
            statement -> state = STATE_S6;
            statement -> eod = 0;
        }
    }

    if ( log_info.log_flag )
    {
        sprintf( statement -> msg, 
                "\n\t\tExit:[%s]\
\n\t\t\tValue = %p",
                    __get_return_status( ret, s1 ),
                    *value );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    return function_return( SQL_HANDLE_STMT, statement, ret, DEFER_R0 );
}
