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
 * $Id: SQLDisconnect.c,v 1.9 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLDisconnect.c,v $
 * Revision 1.9  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.8  2004/02/18 15:47:44  lurcher
 *
 * Fix a leak in the iconv code
 *
 * Revision 1.7  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.6  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.5  2002/08/12 13:17:52  lurcher
 *
 * Replicate the way the MS DM handles loading of driver libs, and allocating
 * handles in the driver. usage counting in the driver means that dlopen is
 * only called for the first use, and dlclose for the last. AllocHandle for
 * the driver environment is only called for the first time per driver
 * per application environment.
 *
 * Revision 1.4  2002/07/24 08:49:51  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.3  2002/07/04 17:27:56  lurcher
 *
 * Small bug fixes
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.8  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.7  2001/03/21 16:12:29  nick
 *
 * Alter cleaning of stmt and desc handles if a SQLDisconnect fails
 *
 * Revision 1.6  2001/03/02 14:24:23  nick
 *
 * Fix thread detection for Solaris
 *
 * Revision 1.5  2001/02/12 11:20:22  nick
 *
 * Add supoort for calling SQLDriverLoad and SQLDriverUnload
 *
 * Revision 1.4  2000/12/18 12:53:29  nick
 *
 * More pooling tweeks
 *
 * Revision 1.3  2000/12/18 12:32:16  nick
 *
 * Fix missing return codes
 *
 * Revision 1.2  2000/12/14 18:10:19  nick
 *
 * Add connection pooling
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.13  2000/04/27 20:49:03  ngorham
 *
 * Fixes to work with Star Office 5.2
 *
 * Revision 1.12  2001/04/27 01:33:40  ngorham
 *
 * Fix a problem where handles were not being free'd
 *
 * Revision 1.11  2001/04/05 21:15:01  ngorham
 *
 * Fix small memory leak in SQLDisconnect and the Postgres driver
 *
 * Revision 1.10  2000/02/25 00:02:00  ngorham
 *
 * Add a patch to support IBM DB2, and Solaris threads
 *
 * Revision 1.9  2000/02/22 22:14:45  ngorham
 *
 * Added support for solaris threads
 * Added check to overcome bug in PHP4
 * Fixed bug in descriptors and ODBC 3 drivers
 *
 * Revision 1.8  1999/12/28 15:05:01  ngorham
 *
 * Fix bug that caused StarOffice to fail. A SQLConnect, SQLDisconnect,
 * followed by another SQLConnect on the same DBC would fail.
 *
 * Revision 1.7  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.6  1999/10/24 23:54:17  ngorham
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
 * Revision 1.1.1.1  1999/05/29 13:41:05  sShandyb
 * first go at it
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
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

static char const rcsid[]= "$RCSfile: SQLDisconnect.c,v $ $Revision: 1.9 $";

extern int pooling_enabled;

SQLRETURN SQLDisconnect( SQLHDBC connection_handle )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

    /*
     * check connection
     */

    if ( !__validate_dbc( connection ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

        return SQL_INVALID_HANDLE;
    }

    function_entry( connection );

    if ( log_info.log_flag )
    {
        sprintf( connection -> msg, "\n\t\tEntry:\
\n\t\t\tConnection = %p",
                connection );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    /*
     * check states
     */

    if ( connection -> state == STATE_C6 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 25000" );

        __post_internal_error( &connection -> error,
                ERROR_25000, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }
    else if ( connection -> state == STATE_C2 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 08003" );

        __post_internal_error( &connection -> error,
                ERROR_08003, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    /*
     * any statements that are in SQL_NEED_DATA
     */

    if( __check_stmt_from_dbc( connection, STATE_S8 )) {

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &connection -> error,
                ERROR_HY010, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    if( __check_stmt_from_dbc( connection, STATE_S13 )) {

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &connection -> error,
                ERROR_HY010, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    /*
     * is it a pooled connection, or can it go back 
     */

    if ( connection -> pooled_connection ||
         pooling_enabled && connection -> pooling_timeout > 0 )
    {
        __clean_stmt_from_dbc( connection );
        __clean_desc_from_dbc( connection );

        return_to_pool( connection );

        if ( log_info.log_flag )
        {
            sprintf( connection -> msg, 
                    "\n\t\tExit:[%s]",
                        __get_return_status( SQL_SUCCESS, s1 ));

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    connection -> msg );
        }

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_SUCCESS );
    }

    /*
     * disconnect from the driver
     */

    if ( !CHECK_SQLDISCONNECT( connection ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: IM001" );

        __post_internal_error( &connection -> error,
                ERROR_IM001, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    ret = SQLDISCONNECT( connection,
            connection -> driver_dbc );

    if ( SQL_SUCCEEDED( ret ))
    {
        /*
	     * grab any errors
	     */

        if ( ret == SQL_SUCCESS_WITH_INFO )
	    {
	        function_return_ex( IGNORE_THREAD, connection, ret, TRUE, DEFER_R0 );
        }
        
        /*
         * complete disconnection from driver
         */

        __disconnect_part_three( connection );
    }

    if ( log_info.log_flag )
    {
        sprintf( connection -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, s1 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    return function_return( SQL_HANDLE_DBC, connection, ret, DEFER_R0 );
}
