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
 * $Id: SQLExtendedFetch.c,v 1.14 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLExtendedFetch.c,v $
 * Revision 1.14  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.13  2009/02/17 09:47:45  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.12  2008/01/22 17:51:54  lurcher
 * Another SQLULEN mismatch
 *
 * Revision 1.11  2007/11/29 12:00:36  lurcher
 * Add 64 bit type changes to SQLExtendedFetch etc
 *
 * Revision 1.10  2007/11/13 15:04:57  lurcher
 * Fix 64 bit cursor lib issues
 *
 * Revision 1.9  2005/10/27 17:54:49  lurcher
 * fix what I suspect is a typo in qt.m4
 *
 * Revision 1.8  2005/10/21 16:49:53  lurcher
 * Fix a problem with the cursor lib and rowsets
 *
 * Revision 1.7  2004/07/24 17:55:38  lurcher
 * Sync up CVS
 *
 * Revision 1.6  2003/12/01 16:37:17  lurcher
 *
 * Fix a bug in SQLWritePrivateProfileString
 *
 * Revision 1.5  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.3  2002/11/25 15:37:54  lurcher
 *
 * Fix problems in the cursor lib
 *
 * Revision 1.2  2002/11/19 18:52:28  lurcher
 *
 * Alter the cursor lib to not require linking to the driver manager.
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
 * Revision 1.3  1999/11/20 20:54:00  ngorham
 *
 * Asorted portability fixes
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

#define ABS(x)  (((x)>=0)?(x):(-(x)))

#define SQL_FETCH_PART_ROWSET SQL_NO_DATA + 1

SQLRETURN fetch_row( CLHSTMT cl_statement, int row_number, int offset )
{
    SQLSMALLINT ret;

    /*
     * is the row in the cache ?
     */

    if ( row_number < cl_statement -> rowset_count )
    {
        CLBCOL *cbuf;

        /*
         * read the file buffer
         */

#ifdef HAVE_FSEEKO
        if ( fseeko( cl_statement -> rowset_file,
                    cl_statement -> buffer_length * row_number,
                    SEEK_SET ))
#else
        if ( fseek( cl_statement -> rowset_file,
                    cl_statement -> buffer_length * row_number,
                    SEEK_SET ))
#endif
        {
            cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1000, 
                "General error: fseek fails",
                cl_statement -> dm_statement -> connection -> 
                    environment -> requested_version );
            return SQL_ERROR;
        }

        if ( fread( cl_statement -> rowset_buffer,
                cl_statement -> buffer_length, 
                1, 
                cl_statement -> rowset_file ) != 1 )
        {
            cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1000, 
                "General error: Unable to read from file buffer",
                cl_statement -> dm_statement -> connection -> 
                    environment -> requested_version );
            return SQL_ERROR;
        }

        /*
         * extract the data
         *
         * status ptr
         */

        memcpy( &ret, cl_statement -> rowset_buffer, 
                sizeof( SQLUSMALLINT ));

        /*
         * columninfo
         */

        cbuf = cl_statement -> bound_columns;

        while ( cbuf )
        {
            char *buffer = NULL;
            char *ind_ptr = NULL;

            /*
             * copy from the file buffer
             */

            memcpy( cbuf -> local_buffer,
                    cl_statement -> rowset_buffer + 
                    cbuf -> rs_buffer_offset,
                    cbuf -> bound_length );

            memcpy( &cbuf -> len_ind,
                    cl_statement -> rowset_buffer +
                    cbuf -> rs_ind_offset,
                    sizeof( cbuf -> len_ind ));

            if ( offset >= 0 )
            {
                /*
                 * copy to the application buffer
                 */

                if ( cl_statement -> row_bind_type )
                {
                    if ( cbuf -> bound_buffer )
                    {
                        buffer = ((char*)cbuf -> bound_buffer) +
                            cl_statement -> row_bind_type * offset;
                    }
                    if ( cbuf -> bound_ind )
                    {
                        ind_ptr = ( char * ) cbuf -> bound_ind;
                        ind_ptr += cl_statement -> row_bind_type * offset;
                    }
                }
                else
                {
                    if ( cbuf -> bound_buffer )
                    {
                        buffer = ((char*)cbuf -> bound_buffer) +
                            cbuf -> bound_length * offset;
                    }
                    if ( cbuf -> bound_ind )
                    {
                        ind_ptr = ( char * ) cbuf -> bound_ind;
                        ind_ptr += offset * sizeof( SQLULEN );
                    }
                }

                if ( buffer && cbuf -> len_ind >= 0 )
                {
                    if ( cbuf -> bound_type == SQL_C_CHAR )
                    {
                        strcpy( buffer,
                                cbuf -> local_buffer );
                    }
                    else
                    {
                        memcpy( buffer,
                                cbuf -> local_buffer,
                                cbuf -> bound_length );
                    }
                }

                if ( ind_ptr )
                {
                    memcpy( ind_ptr,
                            &cbuf -> len_ind,
                            sizeof( cbuf -> len_ind ));
                }
            }

            cbuf = cbuf -> next;
        }
        return SQL_SUCCESS;
    }
    else
    {
        if ( cl_statement -> rowset_complete )
        {
            return SQL_NO_DATA;
        }

        ret = SQLFETCH( cl_statement -> cl_connection,
                        cl_statement -> driver_stmt );

        if ( ret == SQL_NO_DATA )
        {
            /*
             * at the end
             */
            cl_statement -> rowset_complete = 1;
            cl_statement -> rowset_position = CL_AFTER_END;
        }
        else
        {
            CLBCOL *cbuf;

            /*
             * insert into the cache
             */

            /*
             * status ptr
             */

            memcpy( cl_statement -> rowset_buffer, 
                    &ret, sizeof( SQLUSMALLINT ));

            /*
             * columninfo
             */

            cbuf = cl_statement -> bound_columns;

            while ( cbuf )
            {
                char *buffer = NULL;
                char *ind_ptr = NULL;

                /*
                 * copy to the file buffer
                 */

                memcpy( cl_statement -> rowset_buffer + 
                        cbuf -> rs_buffer_offset,
                        cbuf -> local_buffer,
                        cbuf -> bound_length );

                memcpy( cl_statement -> rowset_buffer +
                        cbuf -> rs_ind_offset,
                        &cbuf -> len_ind,
                        sizeof( cbuf -> len_ind ));

                if ( offset >= 0 )
                {
                    /*
                     * copy to the application buffer
                     */

                    if ( cl_statement -> row_bind_type )
                    {
                        if ( cbuf -> bound_buffer )
                        {
                            buffer = ((char*)cbuf -> bound_buffer) +
                                cl_statement -> row_bind_type * offset;
                        }
                        if ( cbuf -> bound_ind )
                        {
                            ind_ptr = ( char * ) cbuf -> bound_ind;
                            ind_ptr += cl_statement -> row_bind_type * offset;
                        }
                    }
                    else
                    {
                        if ( cbuf -> bound_buffer )
                        {
                            buffer = ((char*)cbuf -> bound_buffer) +
                                cbuf -> bound_length * offset;
                        }
                        if ( cbuf -> bound_ind )
                        {
                            ind_ptr = ( char * ) cbuf -> bound_ind;
                            ind_ptr += offset * sizeof( SQLULEN );
                        }
                    }

                    /*
                     * Not quite sure if the check is valid, I think I can see where
                     * I got it from, but not sure if it's correct
                     * 
                    if ( buffer && cbuf -> bound_ind && *cbuf -> bound_ind >= 0 )
                    */
                    if ( buffer && cbuf -> bound_ind )
                    {
                        if ( cbuf -> bound_type == SQL_C_CHAR )
                        {
                            strcpy( buffer,
                                    cbuf -> local_buffer );
                        }
                        else
                        {
                            memcpy( buffer,
                                    cbuf -> local_buffer,
                                    cbuf -> bound_length );
                        }
                    }

                    if ( ind_ptr )
                    {
                        memcpy( ind_ptr,
                                &cbuf -> len_ind,
                                sizeof( cbuf -> len_ind ));
                    }
                }

                cbuf = cbuf -> next;
            }

            /*
             * write the file buffer
             */

#ifdef HAVE_FSEEKO
            if ( fseeko( cl_statement -> rowset_file,
                        cl_statement -> buffer_length * row_number,
                        SEEK_SET ))
#else
            if ( fseek( cl_statement -> rowset_file,
                        cl_statement -> buffer_length * row_number,
                        SEEK_SET ))
#endif
            {
                cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    ERROR_S1000, 
                    "General error: fseek fails",
                    cl_statement -> dm_statement -> connection -> 
                        environment -> requested_version );
                return SQL_ERROR;
            }

            if ( fwrite( cl_statement -> rowset_buffer,
                    cl_statement -> buffer_length, 
                    1, 
                    cl_statement -> rowset_file ) != 1 )
            {
                cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    ERROR_S1000, 
                    "General error: Unable to write to file buffer",
                    cl_statement -> dm_statement -> connection -> 
                        environment -> requested_version );
                return SQL_ERROR;
            }
            cl_statement -> rowset_count ++;
        }

        return ret;
    }
}

/*
 * read the rowset until the specied row, or the end if 0 supplied
 */

SQLRETURN complete_rowset( CLHSTMT cl_statement, int complete_to )
{
    int row;
    SQLRETURN ret;

    if ( complete_to == 0 )
    {
        row = cl_statement -> rowset_count;

        do
        {
            ret = fetch_row( cl_statement, 
                    row,
                    -1 );

            if ( SQL_SUCCEEDED( ret ))
            {
                row ++;
            }
            else if ( ret == SQL_NO_DATA )
            {
                cl_statement -> rowset_complete = 1;
                ret = SQL_SUCCESS;
                break;
            }
        }
        while ( SQL_SUCCEEDED( ret ));
    }
    else
    {
        row = cl_statement -> rowset_count;

        do
        {
            ret = fetch_row( cl_statement, 
                    row,
                    -1 );

            if ( SQL_SUCCEEDED( ret ))
            {
                row ++;
            }
            else if ( ret == SQL_NO_DATA )
            {
                cl_statement -> rowset_complete = 1;
                ret = SQL_SUCCESS;
                break;
            }
        }
        while ( SQL_SUCCEEDED( ret ) && row < complete_to );
    }

    return ret;
}

/*
 * rows_in_set is the sizeof the rowset
 * row_offset is the row number of the first row
 */

static SQLRETURN fetch_rowset( CLHSTMT cl_statement, 
        int rows_in_set,
        int row_offset,
        int *fetched_rows,
        SQLUSMALLINT *row_status_array,
        SQLULEN *rows_fetched_ptr )
{
    SQLRETURN ret;
    int row_count = 0;
    int row;

    for ( row = 0; row < rows_in_set; row ++ )
    {
        ret = fetch_row( cl_statement, 
                row + row_offset,
                row );

        if ( row_status_array )
        {
            row_status_array[ row ] = ret;
        }
        if ( SQL_SUCCEEDED( ret ))
        {
            row_count ++;
        }
        else 
        {
            break;
        }
        ret = SQL_SUCCESS;
    }

    if ( ret == SQL_NO_DATA && row > 0 )
	{
        *fetched_rows = row;
    	if ( rows_fetched_ptr )
		{
        	*rows_fetched_ptr = row_count;
		}
		ret = SQL_FETCH_PART_ROWSET;
	}

    if ( SQL_SUCCEEDED( ret ))
    {
        *fetched_rows = row;
    }

    if ( rows_fetched_ptr )
    {
        *rows_fetched_ptr = row_count;
    }

    return ret;
}

SQLRETURN do_fetch_scroll( CLHSTMT cl_statement,
        int fetch_orientation,
        SQLLEN fetch_offset,
        SQLUSMALLINT *row_status_ptr,
        SQLULEN *rows_fetched_ptr,
		int ext_fetch )
{
    SQLRETURN ret;
    int rows_in_set, row_offset, fetched_rows, info = 0;

	cl_statement -> fetch_done = 1;

    if ( !cl_statement -> first_fetch_done )
    {
        if ( cl_statement -> column_count > 0 && 
                calculate_buffers( cl_statement,
                    cl_statement -> column_count ) == SQL_ERROR )
        {
            /*
             * tidy up after a failure
             */

            SQLFREESTMT( cl_statement -> cl_connection,
                    cl_statement -> driver_stmt,
                    SQL_CLOSE );

            return SQL_ERROR;
        }
        cl_statement -> first_fetch_done = 1;
    }

	if ( ext_fetch ) 
	{
    	if ( cl_statement -> rowset_size < 1 )
       	 	rows_in_set = 1;
    	else 
        	rows_in_set = cl_statement -> rowset_size;
	}
	else 
	{
    	if ( cl_statement -> rowset_array_size < 1 )
       	 	rows_in_set = 1;
    	else 
        	rows_in_set = cl_statement -> rowset_array_size;
	}

    /*
     * I refuse to document all these conditions, you have to
     * look in the book, its just a copy of that
     */

    switch( fetch_orientation )
    {
      case SQL_FETCH_FIRST:
        cl_statement -> rowset_position = 0;
        row_offset = 0;
        cl_statement -> curr_rowset_start = cl_statement -> rowset_position;
        ret = fetch_rowset( cl_statement, rows_in_set, row_offset,
                &fetched_rows, row_status_ptr, rows_fetched_ptr );
        if ( SQL_SUCCEEDED( ret ))
        {
            cl_statement -> curr_rowset_start = 
                cl_statement -> rowset_position;
            cl_statement -> rowset_position += fetched_rows;
        }
		else if ( ret == SQL_FETCH_PART_ROWSET ) 
		{
			ret = SQL_SUCCESS;
		}
        break;

      case SQL_FETCH_NEXT:
        if ( cl_statement -> rowset_position == CL_BEFORE_START )
        {
            cl_statement -> rowset_position = 0;
            row_offset = 0;
        }
        else if ( cl_statement -> rowset_position == CL_AFTER_END )
        {
            ret = SQL_NO_DATA;
            break;
        }
        else
        {
            row_offset = cl_statement -> rowset_position;
        }

        cl_statement -> cursor_pos = 1;

        ret = fetch_rowset( cl_statement, rows_in_set, row_offset,
                &fetched_rows, row_status_ptr, rows_fetched_ptr );

        if ( SQL_SUCCEEDED( ret ))
        {
            cl_statement -> curr_rowset_start = 
                cl_statement -> rowset_position;
            cl_statement -> rowset_position += fetched_rows;
        }
		else if ( ret == SQL_FETCH_PART_ROWSET ) 
		{
        	cl_statement -> rowset_position = CL_AFTER_END;
			ret = SQL_SUCCESS;
		}
        break;

      case SQL_FETCH_PRIOR:
        if ( cl_statement -> rowset_position == CL_BEFORE_START )
        {
            ret = SQL_NO_DATA;
            break;
        }
        else if ( cl_statement -> rowset_position == CL_AFTER_END )
        {
            if ( cl_statement -> rowset_count < rows_in_set )
            {
                cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                        ERROR_01S06, NULL,
                        cl_statement -> dm_statement -> connection ->
                            environment -> requested_version );
                info = 1;
            }
            else
            {
                row_offset = cl_statement -> rowset_count - rows_in_set;
	    	cl_statement -> rowset_position = row_offset;
            }
        }
        else if ( cl_statement -> rowset_position <= rows_in_set  )
        {
            ret = SQL_NO_DATA;
            cl_statement -> rowset_position = CL_BEFORE_START;
            break;
        }
        else if ( cl_statement -> rowset_position - rows_in_set < rows_in_set )
        {
            cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    ERROR_01S06, NULL,
                    cl_statement -> dm_statement -> connection ->
                        environment -> requested_version );
            ret = SQL_SUCCESS_WITH_INFO;
            break;
        }
        else
        {
            row_offset = cl_statement -> rowset_position -= rows_in_set * 2;
        }

        cl_statement -> cursor_pos = 1;

        ret = fetch_rowset( cl_statement, rows_in_set, row_offset,
                &fetched_rows, row_status_ptr, rows_fetched_ptr );

        if ( SQL_SUCCEEDED( ret ))
        {
            cl_statement -> curr_rowset_start = 
                cl_statement -> rowset_position;
            cl_statement -> rowset_position += fetched_rows;
        }
		else if ( ret == SQL_FETCH_PART_ROWSET ) 
		{
			ret = SQL_SUCCESS;
		}
        break;

      case SQL_FETCH_RELATIVE:
        if (( cl_statement -> rowset_position == CL_BEFORE_START && 
                fetch_offset > 0 ) ||
            ( cl_statement -> rowset_position == CL_AFTER_END && 
                fetch_offset < 0 ))
        {
            return do_fetch_scroll( cl_statement,
                    SQL_FETCH_ABSOLUTE,
                    fetch_offset,
                    row_status_ptr,
                    rows_fetched_ptr, 
					ext_fetch );
        }
        else if ( cl_statement -> rowset_position == CL_BEFORE_START &&
            fetch_offset <= 0 )
        {
            ret = SQL_NO_DATA;
            cl_statement -> rowset_position = CL_BEFORE_START;
            break;
        }
        else if ( cl_statement -> curr_rowset_start == 0 &&
                fetch_offset <= 0 )
        {
            ret = SQL_NO_DATA;
            cl_statement -> rowset_position = CL_BEFORE_START;
            break;
        }
        else if ( cl_statement -> curr_rowset_start > 0 &&
                cl_statement -> curr_rowset_start + fetch_offset < 1 &&
                ABS( fetch_offset ) > rows_in_set )
        {
            ret = SQL_NO_DATA;
            cl_statement -> rowset_position = CL_BEFORE_START;
            break;
        }
        else if ( cl_statement -> curr_rowset_start > 0 &&
                cl_statement -> curr_rowset_start + fetch_offset < 1 &&
                ABS( fetch_offset ) > rows_in_set )
        {
            cl_statement -> rowset_position = 0;
        }
        else
        {
            /*
             * these conditions requires completing the rowset
             */
            if ( !cl_statement -> rowset_complete )
            {
                ret = complete_rowset( cl_statement, 0 );
                if ( !SQL_SUCCEEDED( ret ))
                    break;
            }
            if ( 1 <= cl_statement -> curr_rowset_start + fetch_offset &&
                    cl_statement -> curr_rowset_start + fetch_offset <= 
                    cl_statement -> rowset_count )
            {
                cl_statement -> curr_rowset_start = 
                    cl_statement -> rowset_position = 
                        cl_statement -> curr_rowset_start + fetch_offset;
            }
            else if ( cl_statement -> curr_rowset_start + fetch_offset > 
                    cl_statement -> rowset_count )
            {
                ret = SQL_NO_DATA;
                cl_statement -> rowset_position = CL_AFTER_END;
                break;
            }
            else if ( cl_statement -> rowset_position == CL_AFTER_END &&
                    fetch_offset >= 0 )
            {
                ret = SQL_NO_DATA;
                cl_statement -> rowset_position = CL_AFTER_END;
                break;
            }
        }

        row_offset = cl_statement -> rowset_position;
        cl_statement -> cursor_pos = 1;

        ret = fetch_rowset( cl_statement, rows_in_set, row_offset,
                &fetched_rows, row_status_ptr, rows_fetched_ptr );

        if ( SQL_SUCCEEDED( ret ))
        {
            cl_statement -> curr_rowset_start = 
                cl_statement -> rowset_position;
            cl_statement -> rowset_position += fetched_rows;
        }
		else if ( ret == SQL_FETCH_PART_ROWSET ) 
		{
			ret = SQL_SUCCESS;
		}

        break;

      /*
       * The code before this turns the bookmark into a absolute
       */
      case SQL_FETCH_BOOKMARK:
      case SQL_FETCH_ABSOLUTE:
        /*
         * close the rowset if needed
         */
        if ( fetch_offset < 0 && !cl_statement -> rowset_complete )
        {
            ret = complete_rowset( cl_statement, 0 );
            if ( !SQL_SUCCEEDED( ret ))
                break;
        }

        if ( fetch_offset < 0 && 
                ABS( fetch_offset ) <= cl_statement -> rowset_count )
        {
            cl_statement -> curr_rowset_start = 
                cl_statement -> rowset_count + fetch_offset;
            cl_statement -> rowset_position = 
                cl_statement -> curr_rowset_start;
        }
        else if ( fetch_offset < 0 && 
                    ABS( fetch_offset ) >  cl_statement -> rowset_count &&
                    ABS( fetch_offset ) > rows_in_set )
        {
            cl_statement -> rowset_position = CL_BEFORE_START;
            ret = SQL_NO_DATA;
            break;
        }
        else if ( fetch_offset < 0 && 
                    ABS( fetch_offset ) >  cl_statement -> rowset_count &&
                    ABS( fetch_offset ) <= rows_in_set )
        {
            cl_statement -> curr_rowset_start = 0;
            cl_statement -> rowset_position = 
                cl_statement -> curr_rowset_start;

            cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    ERROR_01S06, NULL,
                    cl_statement -> dm_statement -> connection ->
                        environment -> requested_version );
            info = 1;
        }
        else if ( fetch_offset == 0 )
        {
            cl_statement -> rowset_position = CL_BEFORE_START;
            ret = SQL_NO_DATA;
            break;
        }
        else if ( fetch_offset > cl_statement -> rowset_count )
        {
            ret = complete_rowset( cl_statement, fetch_offset );
            if ( ret == SQL_NO_DATA )
            {
                cl_statement -> rowset_position = CL_AFTER_END;
                break;
            }
            else if ( !SQL_SUCCEEDED( ret ))
            {
                break;
            }
            else
            {
                cl_statement -> curr_rowset_start = fetch_offset;
                cl_statement -> rowset_position = 
                    cl_statement -> curr_rowset_start;
            }
        }
        else
        {
            cl_statement -> curr_rowset_start = fetch_offset;
            cl_statement -> rowset_position = 
                cl_statement -> curr_rowset_start;
        }

        row_offset = cl_statement -> rowset_position - 1;
        cl_statement -> cursor_pos = 1;

        ret = fetch_rowset( cl_statement, rows_in_set, row_offset,
                &fetched_rows, row_status_ptr, rows_fetched_ptr );

        if ( SQL_SUCCEEDED( ret ))
        {
            cl_statement -> curr_rowset_start = 
                cl_statement -> rowset_position;
            cl_statement -> rowset_position += fetched_rows;
        }
		else if ( ret == SQL_FETCH_PART_ROWSET ) 
		{
			ret = SQL_SUCCESS;
		}

        break;

      case SQL_FETCH_LAST:
        /*
         * close the rowset if needed
         */
        if ( !cl_statement -> rowset_complete )
        {
            ret = complete_rowset( cl_statement, 0 );
            if ( !SQL_SUCCEEDED( ret ))
                break;
        }

        if ( cl_statement -> rowset_count <= rows_in_set )
        {
            cl_statement -> curr_rowset_start = 
                cl_statement -> rowset_position = 0;
        }
        else
        {
            cl_statement -> curr_rowset_start = 
                cl_statement -> rowset_position = 
                cl_statement -> rowset_count - rows_in_set;
        }
        row_offset = cl_statement -> rowset_position;
        cl_statement -> cursor_pos = 1;

        ret = fetch_rowset( cl_statement, rows_in_set, row_offset,
                &fetched_rows, row_status_ptr, rows_fetched_ptr );

        if ( SQL_SUCCEEDED( ret ))
        {
            cl_statement -> curr_rowset_start = 
                cl_statement -> rowset_position = CL_AFTER_END;
        }
		else if ( ret == SQL_FETCH_PART_ROWSET ) 
		{
			ret = SQL_SUCCESS;
		}
        
        break;
    }

    if ( ret == SQL_SUCCESS && info )
        ret = SQL_SUCCESS_WITH_INFO;

    return ret;
}

SQLRETURN CLExtendedFetch(
    SQLHSTMT           statement_handle,
    SQLUSMALLINT       f_fetch_type,
    SQLLEN             irow,
    SQLULEN            *pcrow,
    SQLUSMALLINT       *rgf_row_status )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 

    if ( !cl_statement -> bound_columns )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_SL009, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
    }

    return do_fetch_scroll( cl_statement,
            f_fetch_type, 
            irow,
            rgf_row_status,
            pcrow,
			1 );
}
