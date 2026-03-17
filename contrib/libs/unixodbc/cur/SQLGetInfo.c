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
 * $Id: SQLGetInfo.c,v 1.2 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLGetInfo.c,v $
 * Revision 1.2  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
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

SQLRETURN CLGetInfo( SQLHDBC connection_handle,
           SQLUSMALLINT info_type,
           SQLPOINTER info_value,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length )
{
    CLHDBC cl_connection = (CLHDBC) connection_handle; 
    int do_it_here = 1;
    SQLUINTEGER value;
    SQLRETURN ret;
    char *cval = NULL;

    switch( info_type )
    {
      case SQL_BOOKMARK_PERSISTENCE:
        value = 0;
        break;

      case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
        value = 0;
        break;

      case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
        value = 0;
        break;

      case SQL_FETCH_DIRECTION:
        value = SQL_FD_FETCH_ABSOLUTE | 
                    SQL_FD_FETCH_FIRST | 
                    SQL_FD_FETCH_LAST | 
                    SQL_FD_FETCH_NEXT |
                    SQL_FD_FETCH_PRIOR | 
                    SQL_FD_FETCH_RELATIVE |
                    SQL_FD_FETCH_BOOKMARK;
        break;

      case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
        value = SQL_CA1_NEXT | 
                    SQL_CA1_ABSOLUTE |
                    SQL_CA1_RELATIVE | 
                    SQL_CA1_LOCK_NO_CHANGE |
                    SQL_CA1_POS_POSITION | 
                    SQL_CA1_POSITIONED_DELETE |
                    SQL_CA1_POSITIONED_UPDATE | 
                    SQL_CA1_SELECT_FOR_UPDATE;
        break;

      case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
        value = SQL_CA2_READ_ONLY_CONCURRENCY | 
                    SQL_CA2_OPT_VALUES_CONCURRENCY | 
                    SQL_CA2_SENSITIVITY_UPDATES;
        break;

      case SQL_KEYSET_CURSOR_ATTRIBUTES1:
        value = 0;
        break;

      case SQL_KEYSET_CURSOR_ATTRIBUTES2:
        value = 0;
        break;

      case SQL_LOCK_TYPES:
        value = SQL_LCK_NO_CHANGE;
        break;

      case SQL_STATIC_CURSOR_ATTRIBUTES1:
        value = SQL_CA1_NEXT | 
                    SQL_CA1_ABSOLUTE |
                    SQL_CA1_RELATIVE |
                    SQL_CA1_BOOKMARK |
                    SQL_CA1_LOCK_NO_CHANGE | 
                    SQL_CA1_POS_POSITION | 
                    SQL_CA1_POSITIONED_DELETE |
                    SQL_CA1_POSITIONED_UPDATE | 
                    SQL_CA1_SELECT_FOR_UPDATE;
        break;

      case SQL_STATIC_CURSOR_ATTRIBUTES2:
        value = SQL_CA2_READ_ONLY_CONCURRENCY | 
                    SQL_CA2_OPT_VALUES_CONCURRENCY | 
                    SQL_CA2_SENSITIVITY_UPDATES;

        break;

      case SQL_POS_OPERATIONS:
        value = SQL_POS_POSITION;
        break;

      case SQL_POSITIONED_STATEMENTS:
        value = SQL_PS_POSITIONED_DELETE |
                    SQL_PS_POSITIONED_UPDATE | 
                    SQL_PS_SELECT_FOR_UPDATE;
        break;

      case SQL_ROW_UPDATES:
        cval = "Y";
        break;

      case SQL_SCROLL_CONCURRENCY:
        value = SQL_SCCO_READ_ONLY | 
                    SQL_SCCO_OPT_VALUES;
        break;

      case SQL_SCROLL_OPTIONS:
        value = SQL_SO_FORWARD_ONLY |
                    SQL_SO_STATIC;
        break;

      case SQL_STATIC_SENSITIVITY:
        value = SQL_SS_UPDATES;
        break;

      default:
        do_it_here = 0;
        break;
    }

    if ( do_it_here )
    {
        if ( cval )
        {
            if ( buffer_length > 2 && info_value )
            {
                strcpy( info_value, cval );
                ret = SQL_SUCCESS;
            }
            else
            {
                ret = SQL_SUCCESS_WITH_INFO;
            }
            if ( string_length )
            {
                *string_length = 1;
            }
        }
        else
        {
            *((SQLINTEGER*)info_value) = value;
            ret = SQL_SUCCESS;
        }
    }
    else
    {
        ret = SQLGETINFO( cl_connection,
               cl_connection -> driver_dbc,
               info_type,
               info_value,
               buffer_length,
               string_length );

        if ( SQL_SUCCEEDED( ret ))
        {
            if ( info_type == SQL_GETDATA_EXTENSIONS && info_value )
            {
                *((SQLINTEGER*)info_value) |= SQL_GD_BLOCK;
            }
        }
    }

    return ret;
}
