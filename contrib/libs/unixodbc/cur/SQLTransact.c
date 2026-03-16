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
 * $Id: SQLTransact.c,v 1.3 2009/02/18 17:59:18 lurcher Exp $
 *
 * $Log: SQLTransact.c,v $
 * Revision 1.3  2009/02/18 17:59:18  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.2  2005/07/25 16:11:22  lurcher
 * Fix swapped about args in the cursor lib
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

SQLRETURN CLTransact( SQLHENV environment_handle,
           SQLHDBC connection_handle,
           SQLUSMALLINT completion_type )
{
    if ( environment_handle )
    {
        /*
         * the driver manager will not call this
         */
        return SQL_ERROR;
    }
    else if ( connection_handle )
    {
        CLHDBC cl_connection = (CLHDBC) connection_handle; 

            return SQLTRANSACT( cl_connection,
                SQL_NULL_HENV,
                cl_connection -> driver_dbc,
                completion_type );
    }
    else
    {
        return SQL_ERROR;
    }
}
