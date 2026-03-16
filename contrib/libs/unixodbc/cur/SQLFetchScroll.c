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
 * $Id: SQLFetchScroll.c,v 1.7 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLFetchScroll.c,v $
 * Revision 1.7  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.6  2007/11/29 12:00:36  lurcher
 * Add 64 bit type changes to SQLExtendedFetch etc
 *
 * Revision 1.5  2007/11/13 15:04:57  lurcher
 * Fix 64 bit cursor lib issues
 *
 * Revision 1.4  2005/10/27 17:54:49  lurcher
 * fix what I suspect is a typo in qt.m4
 *
 * Revision 1.3  2003/12/01 16:37:17  lurcher
 *
 * Fix a bug in SQLWritePrivateProfileString
 *
 * Revision 1.2  2002/11/19 18:52:28  lurcher
 *
 * Alter the cursor lib to not require linking to the driver manager.
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

SQLRETURN CLFetchScroll( SQLHSTMT statement_handle,
           SQLSMALLINT fetch_orientation,
           SQLLEN fetch_offset )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 

    if ( !cl_statement -> bound_columns )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_SL009, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
    }

    /*
     * get the value from the bookmark pointer and add the offset
     */

    if ( fetch_orientation == SQL_FETCH_BOOKMARK )
    {
        if ( cl_statement -> fetch_bookmark_ptr )
        {
            SQLINTEGER bm_offset = *((SQLINTEGER*)cl_statement -> fetch_bookmark_ptr);
            fetch_offset += bm_offset;
        }
        else
        {
            cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    ERROR_HY111, NULL,
                    cl_statement -> dm_statement -> connection ->
                        environment -> requested_version );
        }
    }

    return do_fetch_scroll( cl_statement,
            fetch_orientation, 
            fetch_offset,
            cl_statement -> row_status_ptr,
            cl_statement -> rows_fetched_ptr,
			0 );
}
