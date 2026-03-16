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
 * $Id: SQLPrepare.c,v 1.3 2009/02/18 17:59:18 lurcher Exp $
 *
 * $Log: SQLPrepare.c,v $
 * Revision 1.3  2009/02/18 17:59:18  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
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

SQLRETURN CLPrepare( SQLHSTMT statement_handle,
           SQLCHAR *statement_text,
           SQLINTEGER text_length )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 

    /*
     * save the statement for later use
     */

    if ( cl_statement -> sql_text )
    {
        free( cl_statement -> sql_text );
    }
    if ( text_length < 0 )
    {
        cl_statement -> sql_text = strdup((char*) statement_text );
    }
    else
    {
        cl_statement -> sql_text = malloc( text_length + 1 );
        memcpy( cl_statement -> sql_text, statement_text, text_length );
        cl_statement -> sql_text[ text_length ] = '\0';
    }

    return SQLPREPARE( cl_statement -> cl_connection,
            cl_statement -> driver_stmt,
            statement_text,
            text_length );
}
