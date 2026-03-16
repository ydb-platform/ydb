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
 * $Id: SQLEndTran.c,v 1.3 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLEndTran.c,v $
 * Revision 1.3  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.2  2009/02/17 09:47:45  lurcher
 * Clear up a number of bugs
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

SQLRETURN CLEndTran( SQLSMALLINT handle_type,
        SQLHANDLE handle,
        SQLSMALLINT completion_type )
{
    switch ( handle_type )
    {
      case SQL_HANDLE_ENV:
        /*
         * the driver manager will not call this
         */
        return SQL_ERROR;
        break;

      case SQL_HANDLE_DBC:
        {
        CLHDBC cl_connection = (CLHDBC) handle; 

            return SQLENDTRAN( cl_connection,
                SQL_HANDLE_DBC,
                cl_connection -> driver_dbc,
                completion_type );
        }
        break;

      default:
        return SQL_ERROR;
    }
}
