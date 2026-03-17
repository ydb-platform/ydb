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
 * $Id: SQLTables.c,v 1.2 2009/02/18 17:59:18 lurcher Exp $
 *
 * $Log: SQLTables.c,v $
 * Revision 1.2  2009/02/18 17:59:18  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.1.1.1  2001/10/17 16:40:15  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.2  1999/09/23 21:46:37  ngorham
 *
 * Added cursor support for metadata functions
 *
 * Revision 1.1  1999/09/19 22:22:51  ngorham
 *
 *
 * Added first cursor library work, read only at the moment and only works
 * with selects with no where clause
 *
 *
 **********************************************************************/

#include <config.h>
#include "cursorlibrary.h"

SQLRETURN CLTables( SQLHSTMT statement_handle,
           SQLCHAR *catalog_name,
           SQLSMALLINT name_length1,
           SQLCHAR *schema_name,
           SQLSMALLINT name_length2,
           SQLCHAR *table_name,
           SQLSMALLINT name_length3,
           SQLCHAR *table_type,
           SQLSMALLINT name_length4 )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 
    SQLRETURN ret;

    ret = SQLTABLES( cl_statement -> cl_connection,
           cl_statement -> driver_stmt,
           catalog_name,
           name_length1,
           schema_name,
           name_length2,
           table_name,
           name_length3,
           table_type,
           name_length4 );

    if ( SQL_SUCCEEDED( ret ))
    {
        SQLSMALLINT column_count;

        ret = SQLNUMRESULTCOLS( cl_statement -> cl_connection,
           cl_statement -> driver_stmt,
           &column_count );

        cl_statement -> column_count = column_count;
        cl_statement -> first_fetch_done = 0;
        cl_statement -> not_from_select = 1;

        if ( column_count > 0 )
        {
            ret = get_column_names( cl_statement );
        }
    }
    return ret;
}
