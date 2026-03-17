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
 * $Id: SQLFreeStmt.c,v 1.3 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLFreeStmt.c,v $
 * Revision 1.3  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.2  2004/07/24 17:55:38  lurcher
 * Sync up CVS
 *
 * Revision 1.1.1.1  2001/10/17 16:40:15  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.2  1999/10/03 23:05:17  ngorham
 *
 * First public outing of the cursor lib
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

SQLRETURN CLFreeStmt( SQLHSTMT statement_handle,
           SQLUSMALLINT option )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 
    SQLRETURN ret = SQL_SUCCESS;

    /*
     * call the driver
     */

    if ( !cl_statement -> driver_stmt_closed )
    {
        ret = SQLFREESTMT( cl_statement -> cl_connection,
                cl_statement -> driver_stmt,
                option );
    }

    if ( SQL_SUCCEEDED( ret ))
    {
        if ( option == SQL_DROP )
        {
			if ( cl_statement -> fetch_statement != SQL_NULL_HSTMT )
			{
            	ret = SQLFREESTMT( cl_statement -> cl_connection,
            		cl_statement -> fetch_statement,
            		SQL_DROP );

				cl_statement -> fetch_statement = SQL_NULL_HSTMT;
			}
            /*
             * free all bound columns
             */
            free_bound_columns( cl_statement );

            /*
             * free up any rowset
             */
            free_rowset( cl_statement );

            free( cl_statement );
        }
        else if ( option == SQL_CLOSE )
        {
            /*
             * free up any rowset
             */
            free_rowset( cl_statement );
        }
        else if ( option == SQL_UNBIND )
        {
            /*
             * free all bound columns
             */
            free_bound_columns( cl_statement );
        }
    }
    return ret;
}
