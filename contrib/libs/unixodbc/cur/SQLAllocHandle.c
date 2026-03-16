/*********************************************************************
 *
 * unixODBC Cursor Library
 *
 * Created by Nick Gorham
 * (nick@lurcher.org).
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
 * $Id: SQLAllocHandle.c,v 1.6 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLAllocHandle.c,v $
 * Revision 1.6  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.5  2009/02/17 09:47:45  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.4  2005/07/08 12:11:23  lurcher
 *
 * Fix a cursor lib problem (it was broken if you did metadata calls)
 * Alter the params to SQLParamOptions to use SQLULEN
 *
 * Revision 1.3  2004/07/24 17:55:38  lurcher
 * Sync up CVS
 *
 * Revision 1.2  2002/11/19 18:52:28  lurcher
 *
 * Alter the cursor lib to not require linking to the driver manager.
 *
 * Revision 1.1.1.1  2001/10/17 16:40:15  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2001/03/28 14:57:22  nick
 *
 * Fix bugs in corsor lib introduced bu UNCODE and other changes
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.2  1999/11/20 20:54:00  ngorham
 *
 * Asorted portability fixes
 *
 * Revision 1.1  1999/09/19 22:22:50  ngorham
 *
 *
 * Added first cursor library work, read only at the moment and only works
 * with selects with no where clause
 *
 *
 **********************************************************************/

#include <config.h>
#include "cursorlibrary.h"

SQLRETURN CLAllocHandle( SQLSMALLINT handle_type,
           SQLHANDLE input_handle,
           SQLHANDLE *output_handle,
           SQLHANDLE dm_handle )
{
    switch ( handle_type )
    {
        case SQL_HANDLE_ENV:
        case SQL_HANDLE_DBC:
          /* 
           * shouldn't be here
           */

          return SQL_ERROR;
          break;

        case SQL_HANDLE_STMT:
          {
            CLHDBC cl_connection = (CLHDBC) input_handle;
            CLHSTMT cl_statement;
            DMHDBC connection = cl_connection -> dm_connection;
            SQLRETURN ret;

            /*
             * allocate a cursor lib statement
             */

            cl_statement = malloc( sizeof( *cl_statement ));

            if ( !cl_statement )
            {
                cl_connection -> dh.dm_log_write( "CL " __FILE__,
                        __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        "Error: IM001" );

                cl_connection -> dh.__post_internal_error( &connection -> error,
                        ERROR_HY001, NULL,
                        connection -> environment -> requested_version );

                return SQL_ERROR;
            }

            memset( cl_statement, 0, sizeof( *cl_statement ));
            cl_statement -> cl_connection = cl_connection;
            cl_statement -> dm_statement = ( DMHSTMT ) dm_handle; 
            cl_statement -> error_count = 0;
	    cl_statement -> fetch_statement = SQL_NULL_HSTMT;

            ret = SQLALLOCHANDLE( cl_connection, 
                    handle_type,
                    cl_connection -> driver_dbc,
                    &cl_statement -> driver_stmt,
                    NULL );

            if ( SQL_SUCCEEDED( ret ))
            {
                *output_handle = ( SQLHSTMT ) cl_statement;
            }
            else
            {
                free( cl_statement );
            }
            return ret;
          }
          break;

        case SQL_HANDLE_DESC:
          {
            CLHDBC cl_connection = (CLHDBC) input_handle;

            return SQLALLOCHANDLE( cl_connection,
                  handle_type,
                  input_handle,
                  output_handle,
                  NULL );
           }
           break;
    }

	return SQL_ERROR;
}
