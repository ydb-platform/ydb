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
 * $Id: SQLGetStmtOption.c,v 1.4 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLGetStmtOption.c,v $
 * Revision 1.4  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.3  2009/02/17 09:47:45  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.2  2005/10/27 17:54:49  lurcher
 * fix what I suspect is a typo in qt.m4
 *
 * Revision 1.1.1.1  2001/10/17 16:40:15  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
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

SQLRETURN CLGetStmtOption( SQLHSTMT statement_handle,
           SQLUSMALLINT option,
           SQLPOINTER value )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 

    switch( option )
    {
      case SQL_CONCURRENCY:
        *(( SQLUINTEGER * ) value ) = cl_statement -> concurrency;
        break;
        
      case SQL_CURSOR_TYPE:
        *(( SQLUINTEGER * ) value ) = cl_statement -> cursor_type;
        break;

      case SQL_BIND_TYPE:
        *(( SQLUINTEGER * ) value ) = cl_statement -> row_bind_type;
        break;

      case SQL_GET_BOOKMARK:
        *(( SQLUINTEGER * ) value ) = cl_statement -> use_bookmarks;
        break;

      case SQL_ROWSET_SIZE:
        *(( SQLUINTEGER * ) value ) = cl_statement -> rowset_size;
        break;

      case SQL_SIMULATE_CURSOR:
        *(( SQLUINTEGER * ) value ) = cl_statement -> simulate_cursor;
        break;

      case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
        *(( SQLPOINTER * ) value ) = cl_statement -> param_bind_offset_ptr;
        break;

      case SQL_ATTR_PARAM_BIND_TYPE:
        *(( SQLUINTEGER * ) value ) = cl_statement -> concurrency;
        break;

      case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        *(( SQLPOINTER * ) value ) = cl_statement -> row_bind_offset_ptr;
        break;

      case SQL_ATTR_ROW_ARRAY_SIZE:
        *(( SQLUINTEGER * ) value ) = cl_statement -> rowset_array_size;
        break;

      case SQL_ATTR_ROW_STATUS_PTR:
        *(( SQLUSMALLINT ** ) value ) = cl_statement -> row_status_ptr;
        break;

      case SQL_ATTR_ROWS_FETCHED_PTR:
        *(( SQLULEN ** ) value ) = cl_statement -> rows_fetched_ptr;
        break;

      case SQL_ATTR_USE_BOOKMARKS:
        *(( SQLUINTEGER * ) value ) = cl_statement -> use_bookmarks;
        break;
            
      default:
        return SQLGETSTMTOPTION( cl_statement -> cl_connection,
               cl_statement -> driver_stmt,
               option,
               value );
    }

    return SQL_SUCCESS;
}
