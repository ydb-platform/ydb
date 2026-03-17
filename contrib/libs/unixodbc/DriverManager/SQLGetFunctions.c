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
 * $Id: SQLGetFunctions.c,v 1.5 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetFunctions.c,v $
 * Revision 1.5  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.4  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.3  2002/12/05 17:44:31  lurcher
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
 * Revision 1.3  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
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
 * Revision 1.3  1999/07/04 21:05:07  ngorham
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

static char const rcsid[]= "$RCSfile: SQLGetFunctions.c,v $ $Revision: 1.5 $";

SQLRETURN SQLGetFunctions( SQLHDBC connection_handle,
           SQLUSMALLINT function_id,
           SQLUSMALLINT *supported )
{
    DMHDBC connection = (DMHDBC)connection_handle;
    SQLCHAR s0[ 24 ], s1[ 100 + LOG_MESSAGE_LEN ];

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
\n\t\t\tConnection = %p\
\n\t\t\tId = %s\
\n\t\t\tSupported = %p",
                connection,
                __fid_as_string( s1, function_id ),
                supported );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    if ( function_id == SQL_API_SQLGETFUNCTIONS ||
         function_id == SQL_API_SQLDATASOURCES ||
         function_id == SQL_API_SQLDRIVERS ||
         function_id == SQL_API_SQLGETENVATTR ||
         function_id == SQL_API_SQLSETENVATTR )
    {
        *supported = SQL_TRUE;
        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_SUCCESS );
    }

    if ( connection -> state == STATE_C3 ||
            connection -> state == STATE_C2 )
    {
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

    if (( function_id > SQL_API_SQLBULKOPERATIONS && function_id < SQL_API_SQLCOLUMNS ) ||
         ( function_id > SQL_API_SQLALLOCHANDLESTD && function_id < SQL_API_LOADBYORDINAL ) ||
         ( function_id > SQL_API_LOADBYORDINAL && function_id < SQL_API_ODBC3_ALL_FUNCTIONS ) ||
         ( function_id > SQL_API_ODBC3_ALL_FUNCTIONS && function_id < SQL_API_SQLALLOCHANDLE ) ||
         function_id > SQL_API_SQLFETCHSCROLL )
    {
        __post_internal_error( &connection -> error,
                ERROR_HY095, NULL,
                connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
    }

    __check_for_function( connection, function_id, supported );

    if ( log_info.log_flag )
    {
        sprintf( connection -> msg, 
                "\n\t\tExit:[%s]\
\n\t\t\tSupported = %s",
                    __get_return_status( SQL_SUCCESS, s0 ),
                    __sptr_as_string( s1, (short*)supported ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_SUCCESS );
}
