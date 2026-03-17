/*********************************************************************
 *
 * This is based on code created by Peter Harvey,
 * (pharvey@codebydesign.com).
 *
 * Modified and extended by Nick Gorham
 * (nick@lurcher.org).
 *
 * Any bugs or problems should be considered the fault of Nick and not
 * Peter.
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
 * $Id: SQLAllocHandleStd.c,v 1.2 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLAllocHandleStd.c,v $
 * Revision 1.2  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.4  1999/07/07 18:51:54  ngorham
 *
 * Add missing '*'
 *
 * Revision 1.3  1999/07/04 21:05:06  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/21 19:59:04  ngorham
 *
 * Fix bug in SQLAllocHandleStd.c, the wrong handle was being used to
 * return the allocated env handle
 *
 * Revision 1.1.1.1  1999/05/29 13:41:05  sShandyb
 * first go at it
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.2  1999/05/09 23:27:11  nick
 * All the API done now
 *
 * Revision 1.1  1999/04/25 23:02:41  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLAllocHandleStd.c,v $ $Revision: 1.2 $";

SQLRETURN SQLAllocHandleStd(
    SQLSMALLINT        handle_type,
    SQLHANDLE          input_handle,
    SQLHANDLE          *output_handle )
{
    SQLRETURN ret;

    ret = __SQLAllocHandle( handle_type,
            input_handle,
            output_handle,
            0 );

    if ( handle_type == SQL_HANDLE_ENV &&
            SQL_SUCCEEDED( ret ))
    {
        DMHENV environment = (DMHENV) *output_handle;

        environment -> requested_version = SQL_OV_ODBC3;
        environment -> version_set = 1;
    }

    return ret;
}
