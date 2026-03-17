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
 * $Id: SQLSetStmtOption.c,v 1.6 2009/02/18 17:59:18 lurcher Exp $
 *
 * $Log: SQLSetStmtOption.c,v $
 * Revision 1.6  2009/02/18 17:59:18  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.5  2009/02/17 09:47:45  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.4  2005/10/27 17:54:49  lurcher
 * fix what I suspect is a typo in qt.m4
 *
 * Revision 1.3  2005/05/03 17:16:50  lurcher
 * Backport a couple of changes from the Debian build
 *
 * Revision 1.2  2003/03/05 09:48:45  lurcher
 *
 * Add some 64 bit fixes
 *
 * Revision 1.1.1.1  2001/10/17 16:40:15  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
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

SQLRETURN CLSetStmtOption( SQLHSTMT statement_handle,
           SQLUSMALLINT option,
           SQLULEN value )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 
    SQLUINTEGER val;
    SQLRETURN ret = SQL_SUCCESS;

    switch( option )
    {
      case SQL_CONCURRENCY:
        val = ( SQLUINTEGER ) value;
        if ( cl_statement -> concurrency == SQL_CURSOR_FORWARD_ONLY )
        {
            if ( val != SQL_CONCUR_READ_ONLY )
            {
                ret = SQL_SUCCESS_WITH_INFO;
            }
        }
        else
        {
            if ( val != SQL_CONCUR_READ_ONLY &&
                    val != SQL_CONCUR_VALUES )
            {
                ret = SQL_SUCCESS_WITH_INFO;
            }
        }
        if ( ret == SQL_SUCCESS )
        {
            cl_statement -> concurrency = ( SQLUINTEGER ) value;
        }
        break;
        
      case SQL_CURSOR_TYPE:
        val = ( SQLUINTEGER ) value;
        if ( val != SQL_CURSOR_FORWARD_ONLY &&
                val != SQL_CURSOR_TYPE )
        {
            ret = SQL_SUCCESS_WITH_INFO;
        }
        else
        {
            cl_statement -> cursor_type = ( SQLUINTEGER ) value;
        }
        break;

      case SQL_BIND_TYPE:
        cl_statement -> row_bind_type = ( SQLUINTEGER ) value;
        break;

      case SQL_GET_BOOKMARK:
        cl_statement -> use_bookmarks = ( SQLUINTEGER ) value;
        break;

      case SQL_ROWSET_SIZE:
        cl_statement -> rowset_size = ( SQLUINTEGER ) value;
        break;

      case SQL_SIMULATE_CURSOR:
        val = ( SQLUINTEGER ) value;
        if ( val != SQL_SC_NON_UNIQUE )
        {
            ret = SQL_SUCCESS_WITH_INFO;
        }
        else
        {
            cl_statement -> simulate_cursor = ( SQLUINTEGER ) value;
        }
        break;

      case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
        cl_statement -> param_bind_offset_ptr = ( SQLPOINTER ) value;
        break;

      case SQL_ATTR_PARAM_BIND_TYPE:
        cl_statement -> concurrency = ( SQLUINTEGER ) value;
        break;

      case SQL_ATTR_ROW_BIND_OFFSET_PTR:
        cl_statement -> row_bind_offset_ptr = ( SQLPOINTER ) value;
        break;

      case SQL_ATTR_ROW_ARRAY_SIZE:
        cl_statement -> rowset_array_size = ( SQLUINTEGER ) value;
        break;

      case SQL_ATTR_ROW_STATUS_PTR:
        cl_statement -> row_status_ptr = ( SQLUSMALLINT * ) value;
        break;

      case SQL_ATTR_ROWS_FETCHED_PTR:
        cl_statement -> rows_fetched_ptr = ( SQLULEN * ) value;
        break;

      case SQL_ATTR_USE_BOOKMARKS:
        cl_statement -> use_bookmarks = ( SQLUINTEGER ) value;
        break;

      default:
        return SQLSETSTMTOPTION( cl_statement -> cl_connection,
               cl_statement -> driver_stmt,
               option,
               value );
    }

    if ( ret == SQL_SUCCESS_WITH_INFO )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( 
					&cl_statement -> dm_statement -> error,
                    ERROR_01S02, NULL,
                    cl_statement -> dm_statement -> connection -> 
                        environment -> requested_version );
    }
    return ret;
}
