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
 * $Id: SQLSetScrollOptions.c,v 1.9 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLSetScrollOptions.c,v $
 * Revision 1.9  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.8  2007/01/02 10:27:50  lurcher
 * Fix descriptor leak with unicode only driver
 *
 * Revision 1.7  2005/11/23 08:29:16  lurcher
 * Add cleanup in postgres driver
 *
 * Revision 1.6  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.5  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.4  2002/01/15 10:31:34  lurcher
 *
 * Fix invalid diag message
 *
 * Revision 1.3  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.2  2001/12/04 10:16:59  lurcher
 *
 * Fix SQLSetScrollOption problem
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
 * Revision 1.1.1.1  1999/05/29 13:41:08  sShandyb
 * first go at it
 *
 * Revision 1.5  1999/06/03 22:20:25  ngorham
 *
 * Finished off the ODBC3-2 mapping
 *
 * Revision 1.4  1999/06/02 23:48:45  ngorham
 *
 * Added more 3-2 mapping
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

static char const rcsid[]= "$RCSfile: SQLSetScrollOptions.c,v $ $Revision: 1.9 $";

SQLRETURN SQLSetScrollOptions(
    SQLHSTMT           statement_handle,
    SQLUSMALLINT       f_concurrency,
    SQLLEN             crow_keyset,
    SQLUSMALLINT       crow_rowset )
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
\n\t\t\tConcurrency = %d\
\n\t\t\tKeyset = %d\
\n\t\t\tRowset = %d",
                statement,
                f_concurrency,
                (int)crow_keyset,
                crow_rowset );

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

    if ( statement -> state != STATE_S1 )
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

    if (( crow_keyset != SQL_SCROLL_FORWARD_ONLY &&
            crow_keyset != SQL_SCROLL_STATIC &&
            crow_keyset != SQL_SCROLL_KEYSET_DRIVEN &&
            crow_keyset != SQL_SCROLL_DYNAMIC ) ||
            !crow_rowset)
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: S1107" );

        __post_internal_error( &statement -> error,
                ERROR_S1107, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if ( f_concurrency != SQL_CONCUR_READ_ONLY &&
             f_concurrency != SQL_CONCUR_LOCK &&
             f_concurrency != SQL_CONCUR_ROWVER &&
             f_concurrency != SQL_CONCUR_VALUES )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: S1108" );

        __post_internal_error( &statement -> error,
                ERROR_S1108, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if ( CHECK_SQLSETSCROLLOPTIONS( statement -> connection ))
    {
        ret = SQLSETSCROLLOPTIONS( statement -> connection,
                statement -> driver_stmt,
                f_concurrency,
                crow_keyset,
                crow_rowset );
    }
    else if ( statement -> connection -> driver_act_ver >= SQL_OV_ODBC3 &&
            (CHECK_SQLGETINFO( statement -> connection ) || 
			CHECK_SQLGETINFOW( statement -> connection )) &&
            (CHECK_SQLSETSTMTATTR( statement -> connection ) ||
            CHECK_SQLSETSTMTATTRW( statement -> connection )))
    {
        SQLINTEGER info_type, ivp;

        switch( crow_keyset )
        {
          case SQL_SCROLL_FORWARD_ONLY:
            info_type = SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2;
            break;

          case SQL_SCROLL_STATIC:
            info_type = SQL_STATIC_CURSOR_ATTRIBUTES2;
            break;

          case SQL_SCROLL_KEYSET_DRIVEN:
            info_type = SQL_KEYSET_CURSOR_ATTRIBUTES2;
            break;

          case SQL_SCROLL_DYNAMIC:
            info_type = SQL_DYNAMIC_CURSOR_ATTRIBUTES2;
            break;

          default:
            if ( crow_keyset > crow_rowset )
            {
                info_type = SQL_KEYSET_CURSOR_ATTRIBUTES2;
            }
            else
            {
                dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: S1107" );

                __post_internal_error( &statement -> error,
                        ERROR_S1107, NULL,
                        statement -> connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
            }
            break;
        }

        ret = __SQLGetInfo( statement -> connection,
                info_type,
                &ivp,
                sizeof( ivp ),
                0 );

        if( !SQL_SUCCEEDED( ret ))
        {
            dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: SQLGetInfo fails" );
            
            return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
        }

        if ( f_concurrency == SQL_CONCUR_READ_ONLY &&
                !( ivp & SQL_CA2_READ_ONLY_CONCURRENCY ))
        {
            dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: S1C00" );

            __post_internal_error( &statement -> error,
                    ERROR_S1C00, NULL,
                    statement -> connection -> environment -> requested_version );
            
            return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
        }
        else if ( f_concurrency == SQL_CONCUR_LOCK &&
                !( ivp & SQL_CA2_LOCK_CONCURRENCY ))
        {
            dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: S1C00" );

            __post_internal_error( &statement -> error,
                    ERROR_S1C00, NULL,
                    statement -> connection -> environment -> requested_version );
            
            return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
        }
        else if ( f_concurrency == SQL_CONCUR_ROWVER &&
                !( ivp & SQL_CA2_OPT_ROWVER_CONCURRENCY ))
        {
            dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: S1C00" );

            __post_internal_error( &statement -> error,
                    ERROR_S1C00, NULL,
                    statement -> connection -> environment -> requested_version );
            
            return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
        }
        if ( f_concurrency == SQL_CONCUR_VALUES &&
                !( ivp & SQL_CA2_OPT_VALUES_CONCURRENCY ))
        {
            dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: S1C00" );

            __post_internal_error( &statement -> error,
                    ERROR_S1C00, NULL,
                    statement -> connection -> environment -> requested_version );
            
            return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
        }
        if ( f_concurrency != SQL_CONCUR_READ_ONLY &&
            f_concurrency != SQL_CONCUR_LOCK &&
            f_concurrency != SQL_CONCUR_ROWVER &&
            f_concurrency != SQL_CONCUR_VALUES )
        {
            dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: S1108" );

            __post_internal_error( &statement -> error,
                    ERROR_S1108, NULL,
                    statement -> connection -> environment -> requested_version );
            
            return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
        }

        if(CHECK_SQLSETSTMTATTR( statement -> connection ))
        {
            ret = SQLSETSTMTATTR( statement -> connection,
                        statement -> driver_stmt,
                        SQL_ATTR_CONCURRENCY,
                        (SQLPOINTER)(intptr_t) f_concurrency,
                        0 );
        }
        else if ( CHECK_SQLSETSTMTATTRW( statement -> connection ))
        {
            ret = SQLSETSTMTATTRW( statement -> connection,
                        statement -> driver_stmt,
                        SQL_ATTR_CONCURRENCY,
                        (SQLPOINTER)(intptr_t) f_concurrency,
                        0 );
        }

        if ( !SQL_SUCCEEDED( ret ))
        {
            dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: SQLSetStmtAttr fails" );
            
            return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
        }


        switch( crow_keyset )
        {
          case SQL_SCROLL_FORWARD_ONLY:
            info_type = SQL_CURSOR_FORWARD_ONLY;
            break;

          case SQL_SCROLL_STATIC:
            info_type = SQL_CURSOR_STATIC;
            break;

          case SQL_SCROLL_KEYSET_DRIVEN:
            info_type = SQL_CURSOR_KEYSET_DRIVEN;
            break;

          case SQL_SCROLL_DYNAMIC:
            info_type = SQL_CURSOR_DYNAMIC;
            break;

          default:
            if ( crow_keyset > crow_rowset )
            {
                info_type = SQL_CURSOR_KEYSET_DRIVEN;
            }
            else
            {
                dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: S1107" );

                __post_internal_error( &statement -> error,
                        ERROR_S1107, NULL,
                        statement -> connection -> environment -> requested_version );

                return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
            }
            break;
        }

        if(CHECK_SQLSETSTMTATTR( statement -> connection ))
        {
             ret = SQLSETSTMTATTR( statement -> connection,
                                   statement -> driver_stmt,
                                   SQL_ATTR_CURSOR_TYPE,
                                   (SQLPOINTER)(intptr_t) info_type,
                                   0 );
        }
        else if(CHECK_SQLSETSTMTATTRW( statement -> connection ))
        {
             ret = SQLSETSTMTATTRW( statement -> connection,
                                   statement -> driver_stmt,
                                   SQL_ATTR_CURSOR_TYPE,
                                   (SQLPOINTER)(intptr_t) info_type,
                                   0 );
        }

        if ( !SQL_SUCCEEDED( ret ))
        {
            dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: SQLSetStmtAttr fails" );
            
            return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
        }

        if ( crow_keyset > 0 )
        {
            if(CHECK_SQLSETSTMTATTR( statement -> connection ))
            {
                 ret = SQLSETSTMTATTR( statement -> connection,
                                       statement -> driver_stmt,
                                       SQL_ATTR_KEYSET_SIZE,
                                       (SQLPOINTER)(intptr_t) crow_keyset,
                                       0 );
            }
            else if(CHECK_SQLSETSTMTATTRW( statement -> connection ))
            {
                 ret = SQLSETSTMTATTRW( statement -> connection,
                                       statement -> driver_stmt,
                                       SQL_ATTR_KEYSET_SIZE,
                                       (SQLPOINTER)(intptr_t) crow_keyset,
                                       0 );
            }

            if ( !SQL_SUCCEEDED( ret ))
            {
                dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQLSetStmtAttr fails" );
                
                return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR, DEFER_R3 );
            }
        }
        if(CHECK_SQLSETSTMTATTR( statement -> connection ))
        {
             ret = SQLSETSTMTATTR( statement -> connection,
                                   statement -> driver_stmt,
                                   SQL_ROWSET_SIZE,
                                   (SQLPOINTER)(intptr_t) crow_rowset,
                                   0 );
        }
        else if(CHECK_SQLSETSTMTATTRW( statement -> connection ))
        {
             ret = SQLSETSTMTATTRW( statement -> connection,
                                   statement -> driver_stmt,
                                   SQL_ROWSET_SIZE,
                                   (SQLPOINTER)(intptr_t) crow_rowset,
                                   0 );
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
