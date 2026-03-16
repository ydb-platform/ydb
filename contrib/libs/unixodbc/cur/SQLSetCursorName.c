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
 * $Id: SQLSetCursorName.c,v 1.4 2009/02/18 17:59:18 lurcher Exp $
 *
 * $Log: SQLSetCursorName.c,v $
 * Revision 1.4  2009/02/18 17:59:18  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.3  2002/11/19 18:52:28  lurcher
 *
 * Alter the cursor lib to not require linking to the driver manager.
 *
 * Revision 1.2  2001/12/13 13:00:33  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
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

SQLRETURN CLSetCursorName( SQLHSTMT statement_handle,
           SQLCHAR *cursor_name,
           SQLSMALLINT name_length )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 
    SQLRETURN ret = SQL_SUCCESS;

    if ( name_length == SQL_NTS )
    {
        if ( strlen((char*) cursor_name ) > MAX_CURSOR_NAME )
        {
            memcpy( cl_statement -> cursor_name, cursor_name, 
                    MAX_CURSOR_NAME );
            cl_statement -> cursor_name[ MAX_CURSOR_NAME ] = '\0';
            ret = SQL_SUCCESS_WITH_INFO;
        }
        else
        {
            strcpy((char*) cl_statement -> cursor_name, (char*) cursor_name );
        }
    }
    else
    {
        if ( name_length > MAX_CURSOR_NAME )
        {
            memcpy( cl_statement -> cursor_name, cursor_name, 
                    MAX_CURSOR_NAME );
            cl_statement -> cursor_name[ MAX_CURSOR_NAME ] = '\0';
            ret = SQL_SUCCESS_WITH_INFO;
        }
        else
        {
            memcpy( cl_statement -> cursor_name, cursor_name, 
                    name_length );
            cl_statement -> cursor_name[ name_length ] = '\0';
        }
    }

    if ( ret == SQL_SUCCESS_WITH_INFO )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    ERROR_01004, NULL,
                    cl_statement -> dm_statement -> connection -> 
                        environment -> requested_version );
    }

    return ret;
}
