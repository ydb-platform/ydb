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
 * $Id: SQLSetPos.c,v 1.4 2009/02/18 17:59:18 lurcher Exp $
 *
 * $Log: SQLSetPos.c,v $
 * Revision 1.4  2009/02/18 17:59:18  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.3  2007/11/13 15:04:57  lurcher
 * Fix 64 bit cursor lib issues
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

SQLRETURN CLSetPos(
    SQLHSTMT           statement_handle,
    SQLSETPOSIROW      irow,
    SQLUSMALLINT       foption,
    SQLUSMALLINT       flock )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 

    /*
     * this is implemented by the cursor lib
     */

    if ( irow == 0 )
    {        
        /*
         * one day maybe, but what do you want, blood ?
         */
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_HYC00, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );
    }
    else if ( irow > cl_statement -> rowset_array_size )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1107, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );
    }
    else if ( foption != SQL_POSITION || flock != SQL_LOCK_NO_CHANGE )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_HYC00, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );
    }

    cl_statement -> cursor_pos = irow;

    return SQL_SUCCESS;
}
