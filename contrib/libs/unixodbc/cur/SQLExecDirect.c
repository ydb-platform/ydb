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
 * $Id: SQLExecDirect.c,v 1.6 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLExecDirect.c,v $
 * Revision 1.6  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.5  2007/11/13 15:04:57  lurcher
 * Fix 64 bit cursor lib issues
 *
 * Revision 1.4  2002/11/19 18:52:28  lurcher
 *
 * Alter the cursor lib to not require linking to the driver manager.
 *
 * Revision 1.3  2002/01/21 18:00:51  lurcher
 *
 * Assorted fixed and changes, mainly UNICODE/bug fixes
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

/*
 * free the rowset info
 */

void free_rowset( CLHSTMT cl_statement )
{
    if ( cl_statement -> rowset_buffer )
    {
        free( cl_statement -> rowset_buffer );
        cl_statement -> rowset_buffer = NULL;
    }

    if ( cl_statement -> rowset_file )
    {
        fclose( cl_statement -> rowset_file );
        cl_statement -> rowset_file = NULL;
    }

    if ( cl_statement -> sql_text )
    {
        free( cl_statement -> sql_text );
        cl_statement -> sql_text = NULL;
    }

    if ( cl_statement -> column_names )
    {
        int i;

        for ( i = 0; i < cl_statement -> column_count; i ++ )
        {
            free( cl_statement -> column_names[ i ] );
        }
        free( cl_statement -> column_names );
        cl_statement -> column_names = NULL;
    }

    if ( cl_statement -> data_type )
    {
        free( cl_statement -> data_type );
        cl_statement -> data_type = NULL;
    }

    if ( cl_statement -> column_size )
    {
        free( cl_statement -> column_size );
        cl_statement -> column_size = NULL;
    }

    if ( cl_statement -> decimal_digits )
    {
        free( cl_statement -> decimal_digits );
        cl_statement -> decimal_digits = NULL;
    }
}

/*
 * run through the bound columns, calculating the offsets
 */

int calculate_buffers( CLHSTMT cl_statement, int column_count )
{
    CLBCOL *bcol;

    cl_statement -> rowset_position = CL_BEFORE_START;
    cl_statement -> rowset_count = 0;
    cl_statement -> rowset_complete = 0;
    cl_statement -> column_count = column_count;

    /*
     * row status value
     */
    cl_statement -> buffer_length = sizeof( SQLUSMALLINT );

    bcol = cl_statement -> bound_columns;
    while ( bcol )
    {
        if ( bcol -> column_number <= column_count )
        {
            bcol -> rs_buffer_offset = cl_statement -> buffer_length;
            cl_statement -> buffer_length += bcol -> bound_length;

            bcol -> rs_ind_offset = cl_statement -> buffer_length;
            cl_statement -> buffer_length += sizeof( SQLULEN );
        }

        bcol = bcol -> next;
    }

    /*
     * allocate buffer
     */

    cl_statement -> rowset_buffer = malloc( cl_statement -> buffer_length );
    if ( !cl_statement -> rowset_buffer )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    ERROR_HY001, NULL,
                    cl_statement -> dm_statement -> connection -> 
                        environment -> requested_version );

        return SQL_ERROR;
    }

    /*
     * open temp file
     */

    cl_statement -> rowset_file = tmpfile();
    if ( !cl_statement -> rowset_file )
    {
        cl_statement -> cl_connection -> dh.__post_internal_error_ex( &cl_statement -> dm_statement -> error,
                    (SQLCHAR*)"S1000", 0,
                    (SQLCHAR*)"General Error, Unable to create file buffer",
                    SUBCLASS_ODBC, SUBCLASS_ODBC );

        return SQL_ERROR;
    }

    return SQL_SUCCESS;
}

SQLRETURN get_column_names( CLHSTMT cl_statement )
{
    int i;
    char cname[ 256 ];

    /*
     * already done ?
     */

    if ( cl_statement -> column_names )
    {
        return SQL_SUCCESS;
    }

    /*
     * get the names of all the columns
     */

    cl_statement -> column_names = malloc( sizeof(char *) 
            * cl_statement -> column_count );
    if ( !cl_statement->column_names )
        return SQL_ERROR;

    cl_statement -> data_type = malloc( sizeof( SQLSMALLINT ) 
            * cl_statement -> column_count );
    if ( !cl_statement->data_type )
        return SQL_ERROR;

    cl_statement -> column_size = malloc( sizeof( SQLULEN ) 
            * cl_statement -> column_count );
    if ( !cl_statement->column_size )
        return SQL_ERROR;

    cl_statement -> decimal_digits = malloc( sizeof( SQLSMALLINT ) 
            * cl_statement -> column_count );
    if ( !cl_statement->decimal_digits )
        return SQL_ERROR;

    for ( i = 1; i <= cl_statement -> column_count; i ++ )
    {
        SQLRETURN ret;
    
        if ( !CHECK_SQLDESCRIBECOL( cl_statement -> cl_connection ))
        {
            cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> 
                    dm_statement -> error,
                    ERROR_01000, "Driver does not support SQLDescribeCol",
                    cl_statement -> dm_statement -> connection ->
                        environment -> requested_version );

            return SQL_ERROR;
        }
        ret = SQLDESCRIBECOL( cl_statement -> cl_connection,
                cl_statement -> driver_stmt,
                i,
                (SQLCHAR*) cname,
                sizeof( cname ),
                NULL,
                &cl_statement -> data_type[ i - 1 ], 
                &cl_statement -> column_size[ i - 1 ],
                &cl_statement -> decimal_digits[ i - 1 ],
                NULL );

        if ( !SQL_SUCCEEDED( ret ))
        {
            cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> 
                    dm_statement -> error,
                    ERROR_01000, "SQLDescribeCol failed in driver",
                    cl_statement -> dm_statement -> connection ->
                        environment -> requested_version );

            return SQL_ERROR;
        }

        cl_statement -> column_names[ i - 1 ] = strdup( cname );
    }

    return SQL_SUCCESS;
}

SQLRETURN CLExecDirect( SQLHSTMT statement_handle,
           SQLCHAR *statement_text,
           SQLINTEGER text_length )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 
    SQLRETURN ret;

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

    ret = SQLEXECDIRECT( cl_statement -> cl_connection,
           cl_statement -> driver_stmt,
           statement_text,
           text_length );

    if ( SQL_SUCCEEDED( ret ))
    {
        SQLSMALLINT column_count;

        ret = SQLNUMRESULTCOLS( cl_statement -> cl_connection,
           cl_statement -> driver_stmt,
           &column_count );

        cl_statement -> column_count = column_count;
        cl_statement -> first_fetch_done = 0;

        if ( column_count > 0 )
        {
            ret = get_column_names( cl_statement );
        }
    }

    return ret;
}
