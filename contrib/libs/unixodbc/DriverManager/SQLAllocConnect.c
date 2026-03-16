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
 * $Id: SQLAllocConnect.c,v 1.2 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLAllocConnect.c,v $
 * Revision 1.2  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.1.1.1  2001/10/17 16:40:03  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.2  1999/07/04 21:05:06  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:04  sShandyb
 * first go at it
 *
 * Revision 1.4  1999/06/02 23:48:45  ngorham
 *
 * Added more 3-2 mapping
 *
 *
 * Revision 1.3  1999/06/02 20:01:00  ngorham
 *
 * Attempt to fix previous botched log message
 *
 * Revision 1.2  1999/06/02 19:57:20  ngorham
 *
 * Added code to check if a attempt is being made to compile with a C++
 * Compiler, and issue a message.
 * Start work on the ODBC2-3 conversions.
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.1  1999/04/25 23:02:41  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#ifdef __cplusplus
#error "This code must be compiled with a C compiler,"
#error "not a C++ one, this is due in part to Microsoft"
#error "defining SQLCHAR as unsigned, this means all the"
#error "standard library code confilicts."
#error ""
#error "Alter the compiler line in Common.mk to ecgs or"
#error "gcc and remake. This will correctly compile"
#error "the C++ elements"
#error ""
#endif

#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLAllocConnect.c,v $ $Revision: 1.2 $";

SQLRETURN SQLAllocConnect( SQLHENV environment_handle,
           SQLHDBC *connection_handle )
{
    return __SQLAllocHandle( SQL_HANDLE_DBC,
            environment_handle,
            connection_handle,
            SQL_OV_ODBC2 );
}
