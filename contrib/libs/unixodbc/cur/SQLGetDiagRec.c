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
 * $Id: SQLGetDiagRec.c,v 1.7 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLGetDiagRec.c,v $
 * Revision 1.7  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.6  2009/02/17 09:47:45  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.5  2008/01/02 15:10:33  lurcher
 * Fix problems trying to use the cursor lib on a non select statement
 *
 * Revision 1.4  2007/02/12 11:49:35  lurcher
 * Add QT4 support to existing GUI parts
 *
 * Revision 1.3  2005/08/26 09:31:39  lurcher
 * Add call to allow the cursor lib to call SQLGetDiagRec
 *
 * Revision 1.2  2004/07/24 20:00:39  peteralexharvey
 * for OS2 port
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

SQLRETURN CLGetDiagRec( SQLSMALLINT handle_type,
        SQLHANDLE   handle,
        SQLSMALLINT rec_number,
        SQLCHAR     *sqlstate,
        SQLINTEGER  *native,
        SQLCHAR     *message_text,
        SQLSMALLINT buffer_length,
        SQLSMALLINT *text_length_ptr )
{
    CLHDBC cl_connection = (CLHDBC) handle;
    DRV_SQLHANDLE dhandle;

    switch(handle_type) {
      case SQL_HANDLE_ENV:
      {
          return SQL_NO_DATA;
      }
      case SQL_HANDLE_DBC:
      {
          dhandle = cl_connection->driver_dbc;
          break;
      }
      case SQL_HANDLE_STMT:
      {
          CLHSTMT cl_statement = (CLHSTMT)handle;
          cl_connection = cl_statement->cl_connection;
		  if ( cl_statement -> driver_stmt_closed ) {
			  return SQL_NO_DATA;
		  }
          dhandle = cl_statement->driver_stmt;
          break;
      }
    }
    return SQLGETDIAGREC(cl_connection, handle_type, dhandle, rec_number, sqlstate,
                         native, message_text, buffer_length,
                         text_length_ptr);
}
