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
 * $Id: SQLSetScrollOptions.c,v 1.5 2009/02/18 17:59:18 lurcher Exp $
 *
 * $Log: SQLSetScrollOptions.c,v $
 * Revision 1.5  2009/02/18 17:59:18  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.4  2007/11/13 15:04:57  lurcher
 * Fix 64 bit cursor lib issues
 *
 * Revision 1.3  2005/10/27 17:54:49  lurcher
 * fix what I suspect is a typo in qt.m4
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

SQLRETURN CLSetScrollOptions(
    SQLHSTMT           statement_handle,
    SQLUSMALLINT       f_concurrency,
    SQLLEN             crow_keyset,
    SQLUSMALLINT       crow_rowset )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 

    if ( crow_keyset != SQL_SCROLL_FORWARD_ONLY && 
             crow_keyset != SQL_SCROLL_STATIC )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1107, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
    }

    if ( f_concurrency != SQL_CONCUR_READ_ONLY &&
            f_concurrency != SQL_CONCUR_VALUES )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1108, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
    }

    cl_statement -> cursor_type = crow_keyset;
    cl_statement -> concurrency = f_concurrency;
    cl_statement -> rowset_array_size = crow_rowset;
    cl_statement -> rowset_size = crow_rowset;

    return SQL_SUCCESS;
}
