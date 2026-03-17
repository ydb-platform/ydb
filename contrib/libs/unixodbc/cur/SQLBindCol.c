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
 * $Id: SQLBindCol.c,v 1.6 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLBindCol.c,v $
 * Revision 1.6  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.5  2007/11/13 15:04:57  lurcher
 * Fix 64 bit cursor lib issues
 *
 * Revision 1.4  2005/10/21 16:49:53  lurcher
 * Fix a problem with the cursor lib and rowsets
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
 * Revision 1.2  2001/05/31 16:05:55  nick
 *
 * Fix problems with postgres closing local sockets
 * Make odbctest build with QT 3 (it doesn't work due to what I think are bugs
 * in QT 3)
 * Fix a couple of problems in the cursor lib
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.2  1999/10/03 23:05:17  ngorham
 *
 * First public outing of the cursor lib
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

/*
 * release all the bound columns
 */

int free_bound_columns( CLHSTMT cl_statement )
{
    CLBCOL *bcol;

    bcol = cl_statement -> bound_columns;

    while( bcol )
    {
        CLBCOL *next;
        
        if ( bcol -> local_buffer )
        {
            free( bcol -> local_buffer );
        }

        next = bcol -> next;
        free( bcol );
        bcol = next;
    }

    cl_statement -> bound_columns = NULL;

    return 0;
}

static int get_bound_length( int target_type, int len )
{
    switch( target_type )
    {
      case SQL_C_STINYINT:
      case SQL_C_UTINYINT:
      case SQL_C_TINYINT:
        return 1;

      case SQL_C_SBIGINT:
      case SQL_C_UBIGINT:
        return 8;

      case SQL_C_SSHORT:
      case SQL_C_USHORT:
      case SQL_C_SHORT:
        return 2;

      case SQL_C_SLONG:
      case SQL_C_ULONG:
	  case SQL_C_LONG:
        return 4;

      case SQL_C_DOUBLE:
        return 8;

      case SQL_C_FLOAT:
        return 4;

      case SQL_C_NUMERIC:
        return sizeof( SQL_NUMERIC_STRUCT );

      case SQL_C_TYPE_DATE:
      case SQL_C_DATE:
        return sizeof( SQL_DATE_STRUCT );

      case SQL_C_TYPE_TIME:
      case SQL_C_TIME:
        return sizeof( SQL_TIME_STRUCT );

      case SQL_C_TYPE_TIMESTAMP:
      case SQL_C_TIMESTAMP:
        return sizeof( SQL_TIMESTAMP_STRUCT );

      case SQL_C_INTERVAL_YEAR:
      case SQL_C_INTERVAL_MONTH:
      case SQL_C_INTERVAL_DAY:
      case SQL_C_INTERVAL_HOUR:
      case SQL_C_INTERVAL_MINUTE:
      case SQL_C_INTERVAL_SECOND:
      case SQL_C_INTERVAL_YEAR_TO_MONTH:
      case SQL_C_INTERVAL_DAY_TO_HOUR:
      case SQL_C_INTERVAL_DAY_TO_MINUTE:
      case SQL_C_INTERVAL_DAY_TO_SECOND:
      case SQL_C_INTERVAL_HOUR_TO_MINUTE:
      case SQL_C_INTERVAL_HOUR_TO_SECOND:
      case SQL_C_INTERVAL_MINUTE_TO_SECOND:
        return sizeof( SQL_INTERVAL_STRUCT );

      default:
        return len;
    }

}

SQLRETURN CLBindCol( SQLHSTMT statement_handle,
		   SQLUSMALLINT column_number,
           SQLSMALLINT target_type,
		   SQLPOINTER target_value,
           SQLLEN buffer_length,
	   	   SQLLEN *strlen_or_ind )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 
    CLBCOL *bcol;
    int b_len;
    SQLRETURN ret;

	if ( cl_statement -> not_from_select )
	{
		return  SQLBINDCOL( cl_statement -> cl_connection,
			cl_statement -> driver_stmt,
			column_number,
			target_type,
			target_value,
			buffer_length,
			strlen_or_ind );
	}

    /*
     * check in the list of bound columns for a entry
     */

    bcol = cl_statement -> bound_columns;

    while ( bcol )
    {
        if ( bcol -> column_number == column_number )
            break;

        bcol = bcol -> next;
    }

    if ( !bcol )
    {
        /*
         * do we want to bind anything
         */

        bcol = malloc( sizeof( CLBCOL ));
        if ( !bcol )
        {
            cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    ERROR_HY001, NULL,
                    cl_statement -> dm_statement -> connection -> 
                        environment -> requested_version );

            return SQL_ERROR;
        }

        memset( bcol, 0, sizeof( CLBCOL ));
        bcol -> column_number = column_number;

        /*
         * insert into to list
         */

        if ( cl_statement -> bound_columns )
        {
            CLBCOL *ptr, *prev;

            ptr = cl_statement -> bound_columns;
            prev = NULL;

            while( ptr && ptr -> column_number < column_number )
            {
                prev = ptr;
                ptr = ptr -> next;
            }

            if ( prev )
            {
                bcol -> next = ptr;
                prev -> next = bcol;
            }
            else
            {
                bcol -> next = cl_statement -> bound_columns;
                cl_statement -> bound_columns = bcol;
            }
        }
        else
        {
            bcol -> next = NULL;
            cl_statement -> bound_columns = bcol;
        }
    }

    /*
     * setup bound info
     */

    /*
     * find length
     */

    b_len = get_bound_length( target_type, buffer_length );

    if ( bcol -> local_buffer )
    {
        free( bcol -> local_buffer );
    }
    bcol -> local_buffer = NULL;

    if ( target_value && b_len > 0 )
    {
        bcol -> local_buffer = malloc( b_len );
        if ( !bcol -> local_buffer )
        {
            cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    ERROR_HY001, NULL,
                    cl_statement -> dm_statement -> connection -> 
                        environment -> requested_version );

            return SQL_ERROR;
        }
    }

    bcol -> bound_buffer = target_value;
    bcol -> bound_length = b_len;
    bcol -> bound_type = target_type;

    if ( strlen_or_ind )
    {
        bcol -> bound_ind = strlen_or_ind;
    }
    else
    {
        bcol -> bound_ind = NULL;
    }

    /*
     * call the driver to bind a column, but not bookmarks
     */

    if ( column_number > 0 )
    {
        ret = SQLBINDCOL( cl_statement -> cl_connection,
               cl_statement -> driver_stmt,
               column_number,
               target_type,
               bcol -> local_buffer,
               bcol -> bound_length,
               &bcol -> len_ind );
    }
    else
    {
        ret = SQL_SUCCESS;
    }

    /*
     * are we unbinding ?
     */

    if ( !target_value && !strlen_or_ind )
    {
        CLBCOL *ptr, *prev;
        /*
         * remove from the list
         */

        ptr = cl_statement -> bound_columns;
        prev = NULL;

        while( ptr && ptr != bcol )
        {
            prev = ptr;
            ptr = ptr -> next;
        }

        if ( prev )
        {
            prev -> next = bcol -> next;
        }
        else
        {
            cl_statement -> bound_columns = bcol -> next;
        }

        free( bcol );
    }

    return ret;
}
