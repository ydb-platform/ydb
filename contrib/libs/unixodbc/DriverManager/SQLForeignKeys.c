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
 * $Id: SQLForeignKeys.c,v 1.7 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLForeignKeys.c,v $
 * Revision 1.7  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.6  2004/01/12 09:54:39  lurcher
 *
 * Fix problem where STATE_S5 stops metadata calls
 *
 * Revision 1.5  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.4  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.3  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.2  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
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
 * Revision 1.2  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
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
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.3  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.2  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLForeignKeys.c,v $ $Revision: 1.7 $";

SQLRETURN SQLForeignKeysA(
    SQLHSTMT           statement_handle,
    SQLCHAR            *szpk_catalog_name,
    SQLSMALLINT        cbpk_catalog_name,
    SQLCHAR            *szpk_schema_name,
    SQLSMALLINT        cbpk_schema_name,
    SQLCHAR            *szpk_table_name,
    SQLSMALLINT        cbpk_table_name,
    SQLCHAR            *szfk_catalog_name,
    SQLSMALLINT        cbfk_catalog_name,
    SQLCHAR            *szfk_schema_name,
    SQLSMALLINT        cbfk_schema_name,
    SQLCHAR            *szfk_table_name,
    SQLSMALLINT        cbfk_table_name )
{
    return SQLForeignKeys( statement_handle,
                            szpk_catalog_name,
                            cbpk_catalog_name,
                            szpk_schema_name,
                            cbpk_schema_name,
                            szpk_table_name,
                            cbpk_table_name,
                            szfk_catalog_name,
                            cbfk_catalog_name,
                            szfk_schema_name,
                            cbfk_schema_name,
                            szfk_table_name,
                            cbfk_table_name );
}

SQLRETURN SQLForeignKeys(
    SQLHSTMT           statement_handle,
    SQLCHAR            *szpk_catalog_name,
    SQLSMALLINT        cbpk_catalog_name,
    SQLCHAR            *szpk_schema_name,
    SQLSMALLINT        cbpk_schema_name,
    SQLCHAR            *szpk_table_name,
    SQLSMALLINT        cbpk_table_name,
    SQLCHAR            *szfk_catalog_name,
    SQLSMALLINT        cbfk_catalog_name,
    SQLCHAR            *szfk_schema_name,
    SQLSMALLINT        cbfk_schema_name,
    SQLCHAR            *szfk_table_name,
    SQLSMALLINT        cbfk_table_name )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ], s2[ 100 + LOG_MESSAGE_LEN ], s3[ 100 + LOG_MESSAGE_LEN ], s4[ 100 + LOG_MESSAGE_LEN ];
    SQLCHAR s5[ 100 + LOG_MESSAGE_LEN ], s6[ 100 + LOG_MESSAGE_LEN ];

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
\n\t\t\tPK Catalog Name = %s\
\n\t\t\tPK Schema Name = %s\
\n\t\t\tPK Table Name = %s\
\n\t\t\tFK Catalog Name = %s\
\n\t\t\tFK Schema Name = %s\
\n\t\t\tFK Table Name = %s",
                statement,
                __string_with_length( s1, szpk_catalog_name, cbpk_catalog_name ), 
                __string_with_length( s2, szpk_schema_name, cbpk_schema_name ), 
                __string_with_length( s3, szpk_table_name, cbpk_table_name ), 
                __string_with_length( s4, szfk_catalog_name, cbfk_catalog_name ), 
                __string_with_length( s5, szfk_schema_name, cbfk_schema_name ), 
                __string_with_length( s6, szfk_table_name, cbfk_table_name ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    thread_protect( SQL_HANDLE_STMT, statement );

    if ( !szpk_table_name && !szfk_table_name )
    {
        __post_internal_error( &statement -> error,
                ERROR_HY009, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
    
    if (( cbpk_catalog_name < 0 && cbpk_catalog_name != SQL_NTS ) ||
            ( cbpk_schema_name < 0 && cbpk_schema_name != SQL_NTS ) ||
            ( cbpk_table_name < 0 && cbpk_table_name != SQL_NTS ) ||
            ( cbfk_catalog_name < 0 && cbfk_catalog_name != SQL_NTS ) ||
            ( cbfk_schema_name < 0 && cbfk_schema_name != SQL_NTS ) ||
            ( cbfk_table_name < 0 && cbfk_table_name != SQL_NTS ))
    {
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
        if ( statement -> interupted_func != SQL_API_SQLFOREIGNKEYS )
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

    /*
     * TO_DO Check the SQL_ATTR_METADATA_ID settings
     */

    if ( statement -> connection -> unicode_driver )
    {
        SQLWCHAR *s1, *s2, *s3, *s4, *s5, *s6;
        int wlen;

        if ( !CHECK_SQLFOREIGNKEYSW( statement -> connection ))
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

        s1 = ansi_to_unicode_alloc( szpk_catalog_name, cbpk_catalog_name, statement -> connection, &wlen );
        cbpk_catalog_name = wlen;
        s2 = ansi_to_unicode_alloc( szpk_schema_name, cbpk_schema_name, statement -> connection, &wlen );
        cbpk_schema_name = wlen;
        s3 = ansi_to_unicode_alloc( szpk_table_name, cbpk_table_name, statement -> connection, &wlen );
        cbpk_table_name = wlen;
        s4 = ansi_to_unicode_alloc( szfk_catalog_name, cbfk_catalog_name, statement -> connection, &wlen );
        cbfk_catalog_name = wlen;
        s5 = ansi_to_unicode_alloc( szfk_schema_name, cbfk_schema_name, statement -> connection, &wlen );
        cbfk_schema_name = wlen;
        s6 = ansi_to_unicode_alloc( szfk_table_name, cbfk_table_name, statement -> connection, &wlen );
        cbfk_table_name = wlen;

        ret = SQLFOREIGNKEYSW( statement -> connection ,
                statement -> driver_stmt,
                s1,
                cbpk_catalog_name,
                s2,
                cbpk_schema_name,
                s3,
                cbpk_table_name,
                s4,
                cbfk_catalog_name,
                s5,
                cbfk_schema_name,
                s6,
                cbfk_table_name );

        if ( s1 )
            free( s1 );
        if ( s2 )
            free( s2 );
        if ( s3 )
            free( s3 );
        if ( s4 )
            free( s4 );
        if ( s5 )
            free( s5 );
        if ( s6 )
            free( s6 );
    }
    else
    {
        if ( !CHECK_SQLFOREIGNKEYS( statement -> connection ))
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

        ret = SQLFOREIGNKEYS( statement -> connection ,
                statement -> driver_stmt,
                szpk_catalog_name,
                cbpk_catalog_name,
                szpk_schema_name,
                cbpk_schema_name,
                szpk_table_name,
                cbpk_table_name,
                szfk_catalog_name,
                cbfk_catalog_name,
                szfk_schema_name,
                cbfk_schema_name,
                szfk_table_name,
                cbfk_table_name );
    }

    if ( SQL_SUCCEEDED( ret ))
    {
#ifdef NR_PROBE
		/********
		 * Added this to get num cols from drivers which can only tell
		 * us after execute - PAH
		 */

        /*
         * grab any errors
         */

        if ( ret == SQL_SUCCESS_WITH_INFO )
        {
            function_return_ex( IGNORE_THREAD, statement, ret, TRUE, DEFER_R1 );
        }

        SQLNUMRESULTCOLS( statement -> connection,
                statement -> driver_stmt, &statement -> numcols );
		/******/
#endif
        statement -> hascols = 1;
        statement -> state = STATE_S5;
        statement -> prepared = 0;
    }
    else if ( ret == SQL_STILL_EXECUTING )
    {
        statement -> interupted_func = SQL_API_SQLFOREIGNKEYS;
        if ( statement -> state != STATE_S11 &&
                statement -> state != STATE_S12 )
            statement -> state = STATE_S11;
    }
    else
    {
        statement -> state = STATE_S1;
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

    return function_return( SQL_HANDLE_STMT, statement, ret, DEFER_R1 );
}
