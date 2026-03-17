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
 * $Id: SQLFetchScroll.c,v 1.6 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLFetchScroll.c,v $
 * Revision 1.6  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.5  2007/11/29 12:00:31  lurcher
 * Add 64 bit type changes to SQLExtendedFetch etc
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
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.8  2000/05/22 17:10:34  ngorham
 *
 * Fix problems with the FetchScroll -> ExtendedFetch Mapping
 *
 * Revision 1.7  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
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
 * Revision 1.1.1.1  1999/05/29 13:41:07  sShandyb
 * first go at it
 *
 * Revision 1.2  1999/06/02 23:48:45  ngorham
 *
 * Added more 3-2 mapping
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
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

static char const rcsid[]= "$RCSfile: SQLFetchScroll.c,v $ $Revision: 1.6 $";

SQLRETURN SQLFetchScroll( SQLHSTMT statement_handle,
           SQLSMALLINT fetch_orientation,
           SQLLEN fetch_offset )
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
\n\t\t\tFetch Orentation = %d\
\n\t\t\tFetch Offset = %d",
                statement,
                fetch_orientation,
                (int)fetch_offset );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    thread_protect( SQL_HANDLE_STMT, statement );

    if (( fetch_orientation != SQL_FETCH_NEXT &&
            fetch_orientation != SQL_FETCH_PRIOR &&
            fetch_orientation != SQL_FETCH_FIRST &&
            fetch_orientation != SQL_FETCH_LAST &&
            fetch_orientation != SQL_FETCH_ABSOLUTE &&
            fetch_orientation != SQL_FETCH_RELATIVE &&
            fetch_orientation != SQL_FETCH_BOOKMARK ) ||
          (fetch_orientation == SQL_FETCH_BOOKMARK
           && statement -> bookmarks_on == SQL_UB_OFF) )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY106" );

        __post_internal_error( &statement -> error,
                ERROR_HY106, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

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

    if ( statement -> state == STATE_S4 )
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

    if ( statement -> state == STATE_S7 )
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

    if ( statement -> state == STATE_S8 ||
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
        if ( statement -> interupted_func != SQL_API_SQLFETCHSCROLL )
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

    if ( CHECK_SQLFETCHSCROLL( statement -> connection ))
    {
        ret = SQLFETCHSCROLL( statement -> connection,
                statement -> driver_stmt,
                fetch_orientation,
                fetch_offset );
    }
    else if ( statement -> connection -> driver_act_ver == SQL_OV_ODBC2 &&
            CHECK_SQLEXTENDEDFETCH( statement -> connection ))
    {
        /*
         * map to ODBC 2 call
         */
        SQLINTEGER bm_ptr = 0;

        if ( fetch_orientation == SQL_FETCH_BOOKMARK )
        {
            if ( statement -> fetch_bm_ptr )
                bm_ptr = *statement -> fetch_bm_ptr;

            ret = SQLEXTENDEDFETCH( statement -> connection,
                    statement -> driver_stmt,
                    fetch_orientation,
                    bm_ptr,
                    statement -> row_ct_ptr,
                    statement -> row_st_arr );
        }
        else
        {
            ret = SQLEXTENDEDFETCH( statement -> connection,
                    statement -> driver_stmt,
                    fetch_orientation,
                    fetch_offset,
                    statement -> row_ct_ptr,
                    statement -> row_st_arr );
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

    if ( ret == SQL_STILL_EXECUTING )
    {
        statement -> interupted_func = SQL_API_SQLFETCHSCROLL;
        if ( statement -> state != STATE_S11 &&
                statement -> state != STATE_S12 )
            statement -> state = STATE_S11;
    }
    else if ( SQL_SUCCEEDED( ret ))
    {
        statement -> eod = 0;
        statement -> state = STATE_S6;
    }
    else if ( ret == SQL_NO_DATA ) {
        statement -> eod = 1;
        statement -> state = STATE_S6;
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
