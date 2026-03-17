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
 * $Id: SQLSetStmtAttr.c,v 1.16 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLSetStmtAttr.c,v $
 * Revision 1.16  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.15  2009/02/04 09:30:02  lurcher
 * Fix some SQLINTEGER/SQLLEN conflicts
 *
 * Revision 1.14  2007/12/17 13:13:03  lurcher
 * Fix a couple of descriptor typo's
 *
 * Revision 1.13  2007/02/12 11:49:34  lurcher
 * Add QT4 support to existing GUI parts
 *
 * Revision 1.12  2006/04/27 16:39:50  lurcher
 * fix missing return from SQLSetStmtAttr changes
 *
 * Revision 1.11  2006/04/24 08:42:10  lurcher
 * Handle resetting statement descriptors to implicit values, by passing in NULL or the implicit descrptor  to SQLSetStmtAttr with the attribute SQL_ATTR_APP_PARAM_DESC or SQL_ATTR_APP_ROW_DESC. Also catch trying to call SQLGetDescField on a closed connection
 *
 * Revision 1.10  2005/11/23 08:29:16  lurcher
 * Add cleanup in postgres driver
 *
 * Revision 1.9  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.8  2003/03/05 09:48:45  lurcher
 *
 * Add some 64 bit fixes
 *
 * Revision 1.7  2003/02/27 12:19:40  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.6  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.5  2002/09/18 14:49:32  lurcher
 *
 * DataManagerII additions and some more threading fixes
 *
 * Revision 1.3  2002/07/16 13:08:18  lurcher
 *
 * Filter attribute values from SQLSetStmtAttr to SQLSetStmtOption to fit
 * within ODBC 2
 * Make DSN's double clickable in ODBCConfig
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
 * Revision 1.8  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 * Revision 1.7  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.6  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.5  2001/03/28 14:57:22  nick
 *
 * Fix bugs in corsor lib introduced bu UNCODE and other changes
 *
 * Revision 1.4  2001/01/09 22:33:13  nick
 *
 * Stop passing NULL into SQLExtendedFetch
 * Further fixes to unicode to ansi conversions
 *
 * Revision 1.3  2000/12/18 12:53:29  nick
 *
 * More pooling tweeks
 *
 * Revision 1.2  2000/11/22 19:03:40  nick
 *
 * Fix problem with error status in SQLSpecialColumns
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.11  2000/06/24 18:45:09  ngorham
 *
 * Fix for SQLExtendedFetch on big endian platforms. the row count pointer
 * was declared as a small not a int.
 *
 * Revision 1.10  2000/06/20 13:30:10  ngorham
 *
 * Fix problems when using bookmarks
 *
 * Revision 1.9  2000/02/11 00:41:46  ngorham
 *
 * Added a couple of fixes for drivers without SQLExtendedFetch
 *
 * Revision 1.8  1999/11/13 23:41:01  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.7  1999/10/24 23:54:19  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.6  1999/09/21 22:34:26  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.5  1999/09/19 22:24:34  ngorham
 *
 * Added support for the cursor library
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
 * Revision 1.6  1999/06/04 16:29:00  ngorham
 *
 * Added chack that SQLSetStmtAttr exists in the driver before calling it
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

static char const rcsid[]= "$RCSfile: SQLSetStmtAttr.c,v $ $Revision: 1.16 $";

SQLRETURN SQLSetStmtAttrA( SQLHSTMT statement_handle,
           SQLINTEGER attribute,
           SQLPOINTER value,
           SQLINTEGER string_length )
{
    return SQLSetStmtAttr( statement_handle,
           attribute,
           value,
           string_length );
}

SQLRETURN SQLSetStmtAttr( SQLHSTMT statement_handle,
           SQLINTEGER attribute,
           SQLPOINTER value,
           SQLINTEGER string_length )
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
\n\t\t\tStrLen = %d",
                statement,
                __stmt_attr_as_string( s1, attribute ),
                value, 
                (int)string_length );

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

    if ( attribute == SQL_ATTR_CONCURRENCY ||
            attribute == SQL_ATTR_CURSOR_TYPE ||
            attribute == SQL_ATTR_SIMULATE_CURSOR ||
            attribute == SQL_ATTR_USE_BOOKMARKS ||
            attribute == SQL_ATTR_CURSOR_SCROLLABLE ||
            attribute == SQL_ATTR_CURSOR_SENSITIVITY )
    {
        if ( statement -> state == STATE_S2 ||
                statement -> state == STATE_S3 )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY011" );

            __post_internal_error( &statement -> error,
                 ERROR_HY011, NULL,
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
                        "Error: HY011" );

                __post_internal_error( &statement -> error,
                     ERROR_HY011, NULL,
                     statement -> connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
            }
            else
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
                    "Error: HY010" );

            __post_internal_error( &statement -> error,
                 ERROR_HY010, NULL,
                 statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
    }

    if ( (!CHECK_SQLSETSTMTATTR( statement -> connection ) &&
          !CHECK_SQLSETSTMTATTRW( statement -> connection )) &&
        !CHECK_SQLSETSTMTOPTION( statement -> connection ))
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
     * map descriptors to our copies
     */

    if ( attribute == SQL_ATTR_APP_ROW_DESC )
    {
        DMHDESC desc = ( DMHDESC ) value;

		/*
		 * needs to reset to implicit descriptor, this is safe
		 * without a validate, as the value is either null, or the
		 * same as a descriptor we know is valid
		 */

		if ( desc == NULL || desc == statement -> implicit_ard ) 
		{
			DRV_SQLHDESC drv_desc = NULL;

			ret = SQL_SUCCESS;
			
			if ( desc == statement -> implicit_ard )
			{
				drv_desc = statement -> implicit_ard -> driver_desc;
			}

        	if ( CHECK_SQLSETSTMTATTR( statement -> connection ))
        	{
           	 	ret = SQLSETSTMTATTR( statement -> connection,
           	     	statement -> driver_stmt,
           	     	attribute,
           	     	drv_desc,
           	     	0 );
        	}
        	else if ( CHECK_SQLSETSTMTATTRW( statement -> connection ))
        	{
            	ret = SQLSETSTMTATTRW( statement -> connection,
                	statement -> driver_stmt,
                	attribute,
                	statement -> implicit_ard -> driver_desc,
                	0 );
        	}
            else
            {
                ret = SQLSETSTMTOPTION( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    (SQLULEN) statement -> implicit_ard -> driver_desc );
            }

			if ( ret != SQL_SUCCESS ) 
			{
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
			
			/*
			 * copy DM descriptor
			 */

			statement -> apd = statement -> implicit_apd;

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

        if ( !__validate_desc( desc ))
        {
            thread_release( SQL_HANDLE_STMT, statement );

            sprintf( statement -> msg, 
                    "\n\t\tExit:[%s]",
                    __get_return_status( SQL_INVALID_HANDLE, s1 ));

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    statement -> msg );

            return SQL_INVALID_HANDLE;
        }

        if ( desc -> implicit &&
                desc != statement -> implicit_ard )
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

        if ( desc -> connection !=
                statement -> connection )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY024" );

            __post_internal_error( &statement -> error,
                    ERROR_HY024, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }

        /*
         * set the value to the driver descriptor handle
         */
        value = ( SQLPOINTER ) desc -> driver_desc;
        statement -> ard = desc;
        desc -> associated_with = statement;
    }

    if ( attribute == SQL_ATTR_APP_PARAM_DESC )
    {
        DMHDESC desc = ( DMHDESC ) value;

		/*
		 * needs to reset to implicit descriptor, this is safe
		 * without a validate, as the value is either null, or the
		 * same as a descriptor we know is valid
		 */

		if ( desc == NULL || desc == statement -> implicit_apd ) 
		{
			DRV_SQLHDESC drv_desc = NULL;

			ret = SQL_SUCCESS;
			
			if ( desc == statement -> implicit_apd )
			{
				drv_desc = statement -> implicit_apd -> driver_desc;
			}

        	if ( CHECK_SQLSETSTMTATTR( statement -> connection ))
        	{
           	 	ret = SQLSETSTMTATTR( statement -> connection,
           	     	statement -> driver_stmt,
           	     	attribute,
                	statement -> implicit_apd -> driver_desc,
           	     	0 );
        	}
        	else if ( CHECK_SQLSETSTMTATTRW( statement -> connection ))
        	{
            	ret = SQLSETSTMTATTRW( statement -> connection,
                	statement -> driver_stmt,
                	attribute,
                	statement -> implicit_apd -> driver_desc,
                	0 );
        	}
            else
            {
                ret = SQLSETSTMTOPTION( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    (SQLULEN) drv_desc );
            }

			if ( ret != SQL_SUCCESS ) 
			{
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
			
			/*
			 * copy DM descriptor
			 */

			statement -> apd = statement -> implicit_apd;

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

        if ( !__validate_desc( desc ))
        {
            sprintf( statement -> msg, 
                    "\n\t\tExit:[%s]",
                    __get_return_status( SQL_INVALID_HANDLE, s1 ));

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    statement -> msg );

            thread_release( SQL_HANDLE_STMT, statement );

            return SQL_INVALID_HANDLE;
        }

        if ( desc -> implicit &&
                desc != statement -> implicit_apd )
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

        if ( desc -> connection !=
                statement -> connection )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY024" );

            __post_internal_error( &statement -> error,
                    ERROR_HY024, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }

        /*
         * set the value to the driver descriptor handle
         */
        value = ( SQLPOINTER ) desc -> driver_desc;
        statement -> apd = desc;
        desc -> associated_with = statement;
    }

    /*
     * save for internal use
     */

    if ( attribute == SQL_ATTR_METADATA_ID )
    {
        statement -> metadata_id = (SQLLEN) value;
    }

    if ( attribute == SQL_ATTR_IMP_ROW_DESC || 
        attribute == SQL_ATTR_IMP_PARAM_DESC )
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
    ret = dm_check_statement_attrs( statement, attribute, value );

    if ( ret != SQL_SUCCESS ) 
    {
        dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY024" );

        __post_internal_error( &statement -> error,
                ERROR_HY024, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    /*
     * is it something overridden
     */

    value = __attr_override( statement, SQL_HANDLE_STMT, attribute, value, &string_length );

    /*
     * does the call need mapping from 3 to 2
     */

    if ( attribute == SQL_ATTR_FETCH_BOOKMARK_PTR &&
            statement -> connection -> driver_act_ver == SQL_OV_ODBC2 &&
            CHECK_SQLEXTENDEDFETCH( statement -> connection ) &&
            !CHECK_SQLFETCHSCROLL( statement -> connection ))
    {
        statement -> fetch_bm_ptr = (SQLULEN*) value;
        /*
         * pass on if required
         */
        if ( statement -> connection -> cl_handle )
        {
            if ( CHECK_SQLSETSTMTATTR( statement -> connection ))
            {
                SQLSETSTMTATTR( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    value,
                    string_length );
            }
            else
            {
                ret = SQLSETSTMTOPTION( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    (SQLULEN) value );
            }
        }
        ret = SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_ROW_STATUS_PTR &&
	      statement -> connection -> driver_act_ver == SQL_OV_ODBC2 )
    {
        statement -> row_st_arr = (SQLUSMALLINT*) value;
        /*
         * pass on if required
         */
        if ( statement -> connection -> cl_handle )
        {
            if ( CHECK_SQLSETSTMTATTR( statement -> connection ))
            {
                SQLSETSTMTATTR( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    value,
                    string_length );
            }
            else if ( CHECK_SQLSETSTMTATTRW( statement -> connection ))
            {
                SQLSETSTMTATTRW( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    value,
                    string_length );
            }
            else
            {
                ret = SQLSETSTMTOPTION( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    (SQLULEN) value );
            }
        }
        ret = SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_ROWS_FETCHED_PTR &&
	      statement -> connection -> driver_act_ver == SQL_OV_ODBC2 )
    {
        statement -> row_ct_ptr = (SQLULEN*) value;
        /*
         * pass on if required
         */
        if ( statement -> connection -> cl_handle )
        {
            if ( CHECK_SQLSETSTMTATTR( statement -> connection ))
            {
                SQLSETSTMTATTR( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    value,
                    string_length );
            }
            else if ( CHECK_SQLSETSTMTATTRW( statement -> connection ))
            {
                SQLSETSTMTATTRW( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    value,
                    string_length );
            }
            else
            {
                ret = SQLSETSTMTOPTION( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    (SQLULEN) value );
            }
        }
        ret = SQL_SUCCESS;
    }
    else if ( attribute == SQL_ATTR_ROW_ARRAY_SIZE &&
            statement -> connection -> driver_act_ver == SQL_OV_ODBC2 )
    {
        /*
         * save this in case we need it in SQLExtendedFetch
         */
        statement -> row_array_size = (SQLULEN) value;

        if ( CHECK_SQLSETSTMTATTR( statement -> connection ))
        {
            ret = SQLSETSTMTATTR( statement -> connection,
                statement -> driver_stmt,
                SQL_ROWSET_SIZE,
                value,
                string_length );
        }
        else if ( CHECK_SQLSETSTMTATTRW( statement -> connection ))
        {
            ret = SQLSETSTMTATTRW( statement -> connection,
                statement -> driver_stmt,
                SQL_ROWSET_SIZE,
                value,
                string_length );
        }
        else
        {
            ret = SQLSETSTMTOPTION( statement -> connection,
                statement -> driver_stmt,
                SQL_ROWSET_SIZE,
                (SQLULEN) value );
        }
    }
    else if ( CHECK_SQLSETSTMTATTR( statement -> connection ))
    {
        ret = SQLSETSTMTATTR( statement -> connection,
                statement -> driver_stmt,
                attribute,
                value,
                string_length );
    }
    else if ( CHECK_SQLSETSTMTATTRW( statement -> connection ))
    {
        ret = SQLSETSTMTATTRW( statement -> connection,
                statement -> driver_stmt,
                attribute,
                value,
                string_length );
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

        ret = SQLSETSTMTOPTION( statement -> connection,
                    statement -> driver_stmt,
                    attribute,
                    (SQLULEN) value );
    }

    /*
     * take notice of this
     */

    if ( attribute == SQL_ATTR_USE_BOOKMARKS && SQL_SUCCEEDED( ret ))
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
