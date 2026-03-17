/*********************************************************************
 *
 * (pharvey@codebydesign.com).
 *
 * Modified and extended by Nick Gorham
 * (nick@lurcher.org).
 *
 * copyright (c) 1999 Nick Gorham
 *
 * Any bugs or problems should be considered the fault of Nick and not
 * Peter.
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
 * $Id: SQLExecDirect.c,v 1.11 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLExecDirect.c,v $
 * Revision 1.11  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.10  2006/04/11 10:22:56  lurcher
 * Fix a data type check
 *
 * Revision 1.9  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.8  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.7  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.6  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.5  2002/07/18 15:21:56  lurcher
 *
 * Fix problem with SQLExecute/SQLExecDirect returning SQL_NO_DATA
 *
 * Revision 1.4  2002/01/21 18:00:51  lurcher
 *
 * Assorted fixed and changes, mainly UNICODE/bug fixes
 *
 * Revision 1.3  2002/01/15 14:47:44  lurcher
 *
 * Reset stmt->prepared flag after entering a SQLParamData state from
 * SQLExecDirect
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
 * Revision 1.5  2001/08/17 11:03:35  nick
 *
 * Fix final state from SQLExecute if error happens
 *
 * Revision 1.4  2001/04/18 15:03:37  nick
 *
 * Fix problem when going to DB2 unicode driver
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.11  2000/06/20 12:43:58  ngorham
 *
 * Fix bug that caused a success with info message from SQLExecute or
 * SQLExecDirect to be lost if used with a ODBC 3 driver and the application
 * called SQLGetDiagRec
 *
 * Revision 1.10  2000/06/16 16:52:16  ngorham
 *
 * Stop info messages being lost when calling SQLExecute etc on ODBC 3
 * drivers, the SQLNumResultCols were clearing the error before
 * function return had a chance to get to them
 *
 * Revision 1.9  1999/11/18 22:42:09  ngorham
 *
 * Fix missing function_entry in SQLExecDirect()
 *
 * Revision 1.8  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.7  1999/11/10 03:51:33  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
 *
 * Revision 1.6  1999/10/24 23:54:18  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.5  1999/09/21 22:34:24  ngorham
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
 * Revision 1.2  1999/06/30 23:56:54  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:06  sShandyb
 * first go at it
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.5  1999/05/04 22:41:12  nick
 * and another night ends
 *
 * Revision 1.4  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.3  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.2  1999/04/29 21:40:58  nick
 * End of another night :-)
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLExecDirect.c,v $ $Revision: 1.11 $";

SQLRETURN SQLExecDirectA( SQLHSTMT statement_handle,
           SQLCHAR *statement_text,
           SQLINTEGER text_length )
{
    return SQLExecDirect( statement_handle,
                            statement_text,
                            text_length );
}

SQLRETURN SQLExecDirect( SQLHSTMT statement_handle,
           SQLCHAR *statement_text,
           SQLINTEGER text_length )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret;
    SQLCHAR *s1;
    SQLCHAR s2[ 100 + LOG_MESSAGE_LEN ];

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
        /*
         * allocate some space for the buffer
         */

        if ( statement_text && text_length == SQL_NTS )
        {
            s1 = malloc( strlen((char*) statement_text ) + LOG_MESSAGE_LEN );
        }
        else if ( statement_text )
        {
            s1 = malloc( text_length + LOG_MESSAGE_LEN );
        }
        else
        {
            s1 = malloc( LOG_MESSAGE_LEN );
        }

        sprintf( statement -> msg, "\n\t\tEntry:\
\n\t\t\tStatement = %p\
\n\t\t\tSQL = %s",
                statement,
                __string_with_length( s1, statement_text, text_length ));

        free( s1 );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    thread_protect( SQL_HANDLE_STMT, statement );

    if ( !statement_text )
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

    if ( text_length <= 0 && text_length != SQL_NTS )
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

#ifdef NR_PROBE
    if ( statement -> state == STATE_S5 ||
            statement -> state == STATE_S6 ||
            statement -> state == STATE_S7 )
#else
    if (( statement -> state == STATE_S6 && statement -> eod == 0 ) ||
            statement -> state == STATE_S7 )
#endif
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

    if ( statement -> state == STATE_S11 ||
            statement -> state == STATE_S12 )
    {
        if ( statement -> interupted_func != SQL_API_SQLEXECDIRECT )
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

    if ( statement -> connection -> unicode_driver )
    {
        SQLWCHAR *s1;
        int wlen;

#ifdef NR_PROBE
        if ( !CHECK_SQLEXECDIRECTW( statement -> connection ) ||
                !CHECK_SQLNUMRESULTCOLS( statement -> connection ))
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
#else
        if ( !CHECK_SQLEXECDIRECTW( statement -> connection ))
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
#endif

        s1 = ansi_to_unicode_alloc( statement_text, text_length, statement -> connection, &wlen );

        text_length = wlen;

        ret = SQLEXECDIRECTW( statement -> connection,
                statement -> driver_stmt,
                s1,
                text_length );

        if ( s1 )
            free( s1 );
    }
    else
    {
#ifdef NR_PROBE
        if ( !CHECK_SQLEXECDIRECT( statement -> connection ) ||
                !CHECK_SQLNUMRESULTCOLS( statement -> connection ))
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
#else
        if ( !CHECK_SQLEXECDIRECT( statement -> connection ))
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
#endif

        ret = SQLEXECDIRECT( statement -> connection,
                statement -> driver_stmt,
                statement_text,
                text_length );
    }

    if ( SQL_SUCCEEDED( ret ))
    {
#ifdef NR_PROBE
        SQLRETURN local_ret;
  
        /*
         * grab any errors
         */

        if ( ret == SQL_SUCCESS_WITH_INFO )
        {
            function_return_ex( IGNORE_THREAD, statement, ret, TRUE, DEFER_R1 );
        }

        local_ret = SQLNUMRESULTCOLS( statement -> connection,
                statement -> driver_stmt, &statement -> numcols );

        if ( statement -> numcols > 0 )
        {
            statement -> state = STATE_S5;
        }
        else
        {
            statement -> state = STATE_S4;
        }
#else
        /*
         * We don't know for sure
         */
        statement -> hascols = 1;
        statement -> state = STATE_S5;
#endif

        statement -> prepared = 0;

        /*
         * there is a issue here with transactions, but for the
         * moment
         *
        statement -> connection -> state = STATE_C6;
         */
    }
    else if ( ret == SQL_NO_DATA )
    {
        statement -> state = STATE_S4;
        statement -> prepared = 0;
    }
    else if ( ret == SQL_NEED_DATA )
    {
        statement -> interupted_func = SQL_API_SQLEXECDIRECT;
        statement -> interupted_state = statement -> state;
        statement -> state = STATE_S8;

        statement -> prepared = 0;
    }
    else if ( ret == SQL_PARAM_DATA_AVAILABLE )
    {
        statement -> interupted_func = SQL_API_SQLEXECDIRECT;
        statement -> interupted_state = statement -> state;
        statement -> state = STATE_S13;
    }
    else if ( ret == SQL_STILL_EXECUTING )
    {
        statement -> interupted_func = SQL_API_SQLEXECDIRECT;
        if ( statement -> state != STATE_S11 &&
                statement -> state != STATE_S12 )
            statement -> state = STATE_S11;

        statement -> prepared = 0;
    }
    else if (( statement -> state >= STATE_S2 && statement -> state <= STATE_S4 ) ||
              ( statement -> state >= STATE_S11 && statement -> state <= STATE_S12 &&
              statement -> interupted_state >= STATE_S2 && statement -> interupted_state <= STATE_S4 ))
    {
        statement -> state = STATE_S1;
    }
    else if ( statement -> state >= STATE_S11 && statement -> state <= STATE_S12 )
    {
        statement -> state = statement -> interupted_state;
    }

    if ( log_info.log_flag )
    {
        sprintf( statement -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, s2 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    return function_return( SQL_HANDLE_STMT, statement, ret, DEFER_R1 );
}
