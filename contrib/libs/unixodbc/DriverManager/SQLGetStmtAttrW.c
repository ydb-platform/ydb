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
 * $Id: SQLGetStmtAttrW.c,v 1.7 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetStmtAttrW.c,v $
 * Revision 1.7  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.6  2009/02/04 09:30:02  lurcher
 * Fix some SQLINTEGER/SQLLEN conflicts
 *
 * Revision 1.5  2008/08/29 08:01:39  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.4  2007/02/28 15:37:48  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
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
 * Revision 1.3  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLGetStmtAttrW.c,v $";

SQLRETURN SQLGetStmtAttrW( SQLHSTMT statement_handle,
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

#ifdef WITH_HANDLE_REDIRECT
		{
			DMHSTMT parent_statement;

			parent_statement = find_parent_handle( statement, SQL_HANDLE_STMT );

			if ( parent_statement ) {
        		dm_log_write( __FILE__, 
                	__LINE__, 
                    	LOG_INFO, 
                    	LOG_INFO, 
                    	"Info: found parent handle" );

				if ( CHECK_SQLGETSTMTATTRW( parent_statement -> connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

                	return  SQLGETSTMTATTRW( parent_statement -> connection,
							statement_handle,
							attribute,
							value,
							buffer_length,
							string_length );
				}
			}
		}
#endif
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
                  statement -> state == STATE_S7 )  && statement -> eod ))
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

    if ( statement -> connection -> unicode_driver ||
		    CHECK_SQLGETSTMTATTRW( statement -> connection ))
    {
        if ( !CHECK_SQLGETSTMTATTRW( statement -> connection ))
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
        if ( !CHECK_SQLGETSTMTATTR( statement -> connection ))
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
            memcpy( value, &statement -> fetch_bm_ptr, sizeof( SQLLEN * ));

        ret =  SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_ROW_STATUS_PTR &&
            statement -> connection -> driver_act_ver == SQL_OV_ODBC2 &&
            CHECK_SQLEXTENDEDFETCH( statement -> connection ))
    {
        if ( value )
            memcpy( value, &statement -> row_st_arr, sizeof( SQLLEN * ));

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
    else 
    {
        if ( statement -> connection -> unicode_driver )
        {
            ret = SQLGETSTMTATTRW( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    value,
                    buffer_length,
                    string_length );
        }
        else
        {
            /*
             * don't know if any string attributes to convert...
             */

            ret = SQLGETSTMTATTR( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    value,
                    buffer_length,
                    string_length );
        }
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
