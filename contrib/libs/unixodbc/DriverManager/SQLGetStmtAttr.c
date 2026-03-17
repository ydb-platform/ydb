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
 * $Id: SQLGetStmtAttr.c,v 1.8 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetStmtAttr.c,v $
 * Revision 1.8  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.7  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.6  2009/02/04 09:30:02  lurcher
 * Fix some SQLINTEGER/SQLLEN conflicts
 *
 * Revision 1.5  2003/10/30 18:20:46  lurcher
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
 * Revision 1.3  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.2  2002/07/16 13:08:18  lurcher
 *
 * Filter attribute values from SQLSetStmtAttr to SQLSetStmtOption to fit
 * within ODBC 2
 * Make DSN's double clickable in ODBCConfig
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.5  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
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
 * Revision 1.11  2000/06/24 18:45:09  ngorham
 *
 * Fix for SQLExtendedFetch on big endian platforms. the row count pointer
 * was declared as a small not a int.
 *
 * Revision 1.10  2000/02/06 23:26:10  ngorham
 *
 * Fix bug with missing '&' with SQLGetStmtAttr
 *
 * Revision 1.9  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.8  1999/10/29 21:07:40  ngorham
 *
 * Fix some stupid bugs in the DM
 * Make the postgres driver work via unix sockets
 *
 * Revision 1.7  1999/10/24 23:54:18  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.6  1999/09/21 22:34:25  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.5  1999/07/10 21:10:16  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.4  1999/07/04 21:05:07  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.3  1999/06/30 23:56:55  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.2  1999/06/19 17:51:40  ngorham
 *
 * Applied assorted minor bug fixes
 *
 * Revision 1.1.1.1  1999/05/29 13:41:07  sShandyb
 * first go at it
 *
 * Revision 1.4  1999/06/02 23:48:45  ngorham
 *
 * Added more 3-2 mapping
 *
 * Revision 1.3  1999/06/02 20:12:10  ngorham
 *
 * Fixed botched log entry, and removed the dos \r from the sql header files.
 *
 * Revision 1.2  1999/06/02 19:57:20  ngorham
 *
 * Added code to check if a attempt is being made to compile with a C++
 * Compiler, and issue a message.
 * Start work on the ODBC2-3 conversions.
 *
 * Revision 1.1.1.1  1999/05/27 18:23:18  pharvey
 * Imported sources
 *
 * Revision 1.3  1999/05/09 23:27:11  nick
 * All the API done now
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

static char const rcsid[]= "$RCSfile: SQLGetStmtAttr.c,v $ $Revision: 1.8 $";

SQLRETURN SQLGetStmtAttrA( SQLHSTMT statement_handle,
           SQLINTEGER attribute,
           SQLPOINTER value,
           SQLINTEGER buffer_length,
           SQLINTEGER *string_length )
{
    return SQLGetStmtAttr( statement_handle,
           attribute,
           value,
           buffer_length,
           string_length );
}

SQLRETURN SQLGetStmtAttr( SQLHSTMT statement_handle,
           SQLINTEGER attribute,
           SQLPOINTER value,
           SQLINTEGER buffer_length,
           SQLINTEGER *string_length )
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
\n\t\t\tAttribute = %s\
\n\t\t\tValue = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tStrLen = %p",
                statement,
                __stmt_attr_as_string( s1, attribute ),
                value, 
                (int)buffer_length,
                (void*)string_length );

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

    if ( attribute == SQL_ATTR_ROW_NUMBER || attribute == SQL_GET_BOOKMARK )
    {
        if ( statement -> state == STATE_S1 ||
                statement -> state == STATE_S2 ||
                statement -> state == STATE_S3 ||
                statement -> state == STATE_S4 ||
                statement -> state == STATE_S5 ||
                (( statement -> state == STATE_S6 ||
                  statement -> state == STATE_S7 ) && statement -> eod ))
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
    }

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
     * states S5 - S7 are handled by the driver
     */

    if ( statement -> connection -> unicode_driver )
    {
        if ( !CHECK_SQLGETSTMTATTRW( statement -> connection ) &&
            !CHECK_SQLGETSTMTOPTIONW( statement -> connection ) &&
            !CHECK_SQLGETSTMTATTR( statement -> connection ) &&
            !CHECK_SQLGETSTMTOPTION( statement -> connection ))
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
    }
    else
    {
        if ( !CHECK_SQLGETSTMTATTR( statement -> connection ) &&
            !CHECK_SQLGETSTMTOPTION( statement -> connection ))
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
    }

    /*
     * map descriptors to our copies
     */

    if ( attribute == SQL_ATTR_APP_ROW_DESC )
    {
        if ( value )
            memcpy( value, &statement -> ard, sizeof( statement -> ard ));

        ret = SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_APP_PARAM_DESC )
    {
        if ( value )
            memcpy( value, &statement -> apd, sizeof( SQLHANDLE ));

        ret = SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_IMP_ROW_DESC )
    {
        if ( value )
            memcpy( value, &statement -> ird, sizeof( SQLHANDLE ));

        ret = SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_IMP_PARAM_DESC )
    {
        if ( value )
            memcpy( value, &statement -> ipd, sizeof( SQLHANDLE ));

        ret =  SQL_SUCCESS;
    }

    /*
     * does the call need mapping from 3 to 2
     */

    else if ( attribute == SQL_ATTR_FETCH_BOOKMARK_PTR &&
            statement -> connection -> driver_act_ver == SQL_OV_ODBC2 &&
            CHECK_SQLEXTENDEDFETCH( statement -> connection ))
    {
        if ( value )
            memcpy( value, &statement -> fetch_bm_ptr, sizeof( SQLULEN * ));

        ret =  SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_ROW_STATUS_PTR &&
            statement -> connection -> driver_act_ver == SQL_OV_ODBC2 &&
            CHECK_SQLEXTENDEDFETCH( statement -> connection ))
    {
        if ( value )
            memcpy( value, &statement -> row_st_arr, sizeof( SQLULEN * ));

        ret = SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_ROWS_FETCHED_PTR &&
            statement -> connection -> driver_act_ver == SQL_OV_ODBC2 &&
            CHECK_SQLEXTENDEDFETCH( statement -> connection ))
    {
        if ( value )
            memcpy( value, &statement -> row_ct_ptr, sizeof( SQLULEN * ));

        ret = SQL_SUCCESS;
    }
    else if ( statement -> connection -> unicode_driver &&
            attribute == SQL_ATTR_ROW_ARRAY_SIZE &&
            statement -> connection -> driver_act_ver == SQL_OV_ODBC2 )
    {
        if ( CHECK_SQLGETSTMTATTRW( statement -> connection ))
        {
            ret = SQLGETSTMTATTRW( statement -> connection,
                    statement -> driver_stmt,
                    SQL_ROWSET_SIZE,
                    value,
                    buffer_length,
                    string_length );
        }
        else
        {
            ret = SQLGETSTMTATTR( statement -> connection,
                    statement -> driver_stmt,
                    SQL_ROWSET_SIZE,
                    value,
                    buffer_length,
                    string_length );
        }
    }
    else if ( !statement -> connection -> unicode_driver &&
            attribute == SQL_ATTR_ROW_ARRAY_SIZE &&
            statement -> connection -> driver_act_ver == SQL_OV_ODBC2 && 
            CHECK_SQLGETSTMTATTR( statement -> connection ))
    {
        ret = SQLGETSTMTATTR( statement -> connection,
                statement -> driver_stmt,
                SQL_ROWSET_SIZE,
                value,
                buffer_length,
                string_length );
    }
    else if ( attribute == SQL_ATTR_ROW_ARRAY_SIZE &&
            statement -> connection -> driver_act_ver == SQL_OV_ODBC2 )
    {
        if ( statement -> connection -> unicode_driver && 
                CHECK_SQLGETSTMTOPTIONW( statement -> connection ))
        {
            ret = SQLGETSTMTOPTIONW( statement -> connection,
                    statement -> driver_stmt,
                    SQL_ROWSET_SIZE,
                    value );
        }
        else
        {
            ret = SQLGETSTMTOPTION( statement -> connection,
                    statement -> driver_stmt,
                    SQL_ROWSET_SIZE,
                    value );
        }
    }
    else if ( statement -> connection -> unicode_driver &&
            ( CHECK_SQLGETSTMTATTRW( statement -> connection ) || 
            CHECK_SQLGETSTMTATTR( statement -> connection )))
    {
        if ( CHECK_SQLGETSTMTATTR( statement -> connection ))
        {
            ret = SQLGETSTMTATTR( statement -> connection,
                statement -> driver_stmt,
                attribute,
                value,
                buffer_length,
                string_length );
        }
        else
        {
            ret = SQLGETSTMTATTRW( statement -> connection,
                statement -> driver_stmt,
                attribute,
                value,
                buffer_length,
                string_length );
        }
    }
    else if ( !statement -> connection -> unicode_driver &&
            CHECK_SQLGETSTMTATTR( statement -> connection ))
    {
        ret = SQLGETSTMTATTR( statement -> connection,
                statement -> driver_stmt,
                attribute,
                value,
                buffer_length,
                string_length );
    }
    else if ( statement -> connection -> unicode_driver &&
            CHECK_SQLGETSTMTOPTIONW( statement -> connection ))
    {
        /*
         * Is it in the legal range of values
         */

        if ( attribute < SQL_STMT_DRIVER_MIN && 
                ( attribute > SQL_ROW_NUMBER || attribute < SQL_QUERY_TIMEOUT ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY092" );

            __post_internal_error( &statement -> error,
                    ERROR_HY092, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }

        ret = SQLGETSTMTOPTIONW( statement -> connection,
                statement -> driver_stmt,
                attribute,
                value );
    }
    else
    {
        /*
         * Is it in the legal range of values
         */

        if ( attribute < SQL_STMT_DRIVER_MIN && 
                ( attribute > SQL_ROW_NUMBER || attribute < SQL_QUERY_TIMEOUT ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY092" );

            __post_internal_error( &statement -> error,
                    ERROR_HY092, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }

        ret = SQLGETSTMTOPTION( statement -> connection,
                statement -> driver_stmt,
                attribute,
                value );
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
