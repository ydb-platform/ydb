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
 * $Id: SQLGetData.c,v 1.12 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLGetData.c,v $
 * Revision 1.12  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.11  2008/01/02 15:10:33  lurcher
 * Fix problems trying to use the cursor lib on a non select statement
 *
 * Revision 1.10  2007/11/13 15:04:57  lurcher
 * Fix 64 bit cursor lib issues
 *
 * Revision 1.9  2005/07/08 12:11:24  lurcher
 *
 * Fix a cursor lib problem (it was broken if you did metadata calls)
 * Alter the params to SQLParamOptions to use SQLULEN
 *
 * Revision 1.8  2004/12/23 14:51:04  lurcher
 * Fix problem in the cursor lib with blobs
 *
 * Revision 1.7  2004/08/17 17:17:38  lurcher
 *
 * Fix problem in the cursor lib when rereading records with NULLs in
 *
 * Revision 1.6  2004/07/24 17:55:38  lurcher
 * Sync up CVS
 *
 * Revision 1.5  2003/12/01 16:37:17  lurcher
 *
 * Fix a bug in SQLWritePrivateProfileString
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
 * Revision 1.2  2001/03/28 14:57:22  nick
 *
 * Fix bugs in corsor lib introduced bu UNCODE and other changes
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

SQLRETURN CLGetData( SQLHSTMT statement_handle,
           SQLUSMALLINT column_number,
           SQLSMALLINT target_type,
           SQLPOINTER target_value,
           SQLLEN buffer_length,
           SQLLEN *strlen_or_ind )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 
    CLHDBC cl_connection = cl_statement -> cl_connection;
    SQLRETURN ret;
    SQLCHAR sql[ 4095 ];
    CLBCOL *bound_columns;
	int next_bind, first;

	if ( cl_statement -> cursor_type == SQL_CURSOR_FORWARD_ONLY ) 
	{
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_SL008, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
	}

    if ( cl_statement -> not_from_select )
    {
    	return SQLGETDATA( cl_connection,
                cl_statement -> driver_stmt, 
                column_number,
                target_type,
                target_value,
                buffer_length,
                strlen_or_ind );
    }

    /*
     * check we have what we need
     */

    if ( !CHECK_SQLBINDPARAM( cl_connection ) && 
                !CHECK_SQLBINDPARAMETER( cl_connection ))
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1000, "Driver can not bind parameters",
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
    }
    if ( !CHECK_SQLEXECDIRECT( cl_connection ) &&
            !( CHECK_SQLPREPARE( cl_connection ) && 
                CHECK_SQLEXECUTE( cl_connection )))
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1000, "Driver can not prepare or execute",
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
    }
    if ( !CHECK_SQLFETCH( cl_connection ))
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1000, "Driver can not fetch",
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
    }
    if ( !CHECK_SQLGETDATA( cl_connection ))
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1000, "Driver can not getdata",
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
    }

    /*
     * if it's not and closed resultset, and the driver 
     * can only support one active statement, then close 
     * the result set first
     */

    if ( !cl_statement -> rowset_complete &&
            cl_statement -> cl_connection -> active_statement_allowed == 1 )
    {
        int sav_start, sav_pos;

        sav_start = cl_statement -> curr_rowset_start;
        sav_pos = cl_statement -> rowset_position;

        complete_rowset( cl_statement, 0 );
        
        SQLFREESTMT( cl_connection,
                cl_statement -> driver_stmt,
                SQL_DROP );

        cl_statement -> driver_stmt_closed = 1;

        /*
         * restore the position
         */
        cl_statement -> curr_rowset_start = sav_start;
        cl_statement -> rowset_position = sav_pos;
    }

    /*
     * Are we looking for the bookmark...
     */

    if ( column_number == 0 )
    {
        if ( cl_statement -> use_bookmarks )
        {
            switch( target_type )
            {
                case SQL_C_LONG:
                case SQL_C_SLONG:
                case SQL_C_ULONG:
                  if ( target_value )
                  {
                        *((SQLINTEGER*)target_value) = cl_statement -> curr_rowset_start;
                  }
                  if ( strlen_or_ind )
                  {
                      *strlen_or_ind = sizeof( SQLINTEGER );
                  }
                  return SQL_SUCCESS;

                case SQL_C_NUMERIC:
                case SQL_C_CHAR:
                case SQL_C_WCHAR:
                case SQL_C_SSHORT:
                case SQL_C_SHORT:
                case SQL_C_USHORT:
                case SQL_C_FLOAT:
                case SQL_C_DOUBLE:
                case SQL_C_BIT:
                case SQL_C_STINYINT:
                case SQL_C_TINYINT:
                case SQL_C_UTINYINT:
                case SQL_C_SBIGINT:
                case SQL_C_UBIGINT:
                case SQL_C_BINARY:
                case SQL_C_TYPE_DATE:
                case SQL_C_DATE:
                case SQL_C_TYPE_TIME:
                case SQL_C_TIME:
                case SQL_C_TYPE_TIMESTAMP:
                case SQL_C_TIMESTAMP:
                    cl_statement -> cl_connection -> dh.__post_internal_error( 
                                &cl_statement -> dm_statement -> error,
                                ERROR_S1003, NULL,
                                cl_statement -> dm_statement -> connection -> environment -> requested_version );

                    return SQL_ERROR;
            }
        }
        else
        {
            cl_statement -> cl_connection -> dh.__post_internal_error( 
                                &cl_statement -> dm_statement -> error,
                            ERROR_07009, NULL,
                            cl_statement -> dm_statement -> connection -> environment -> requested_version );

            return SQL_ERROR;
        }
    }

    /*
     * refresh the data
     */

	if ( !cl_statement -> fetch_done )
	{
    	ret = SQLGETDATA( cl_connection,
                cl_statement -> fetch_statement, 
                column_number,
                target_type,
                target_value,
                buffer_length,
                strlen_or_ind );

    	if ( !SQL_SUCCEEDED( ret ) && ret != SQL_NO_DATA )
    	{
        	SQLCHAR sqlstate[ 6 ];
        	SQLINTEGER native_error;
        	SQLSMALLINT ind;
        	SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
        	SQLRETURN ret;
			int rec = 1;
	
        	/*
         	* get the error from the driver before
         	* loseing the connection
         	*/
	
			do
        	{
    			if ( CHECK_SQLERROR( cl_connection ))
				{
            		ret = SQLERROR( cl_connection,
                    		SQL_NULL_HENV,
                    		SQL_NULL_HSTMT,
                    		cl_statement -> fetch_statement,
                    		sqlstate,
                    		&native_error,
                    		message_text,
                    		sizeof( message_text ),
                    		&ind );
				}
				else if ( CHECK_SQLGETDIAGREC( cl_connection ))
				{
            		ret = SQLGETDIAGREC( cl_connection,
                    		SQL_HANDLE_STMT,
                    		cl_statement -> fetch_statement,
							rec ++,
                    		sqlstate,
                    		&native_error,
                    		message_text,
                    		sizeof( message_text ),
                    		&ind );
				}
				else
				{
					ret = SQL_NO_DATA;
				}
	
				if ( ret != SQL_NO_DATA )
				{
            		cl_statement -> cl_connection -> dh.__post_internal_error_ex( &cl_statement -> dm_statement -> error,
                    		sqlstate, native_error, message_text,
                    	SUBCLASS_ODBC, SUBCLASS_ODBC );
        		}
        	}
        	while ( SQL_SUCCEEDED( ret ));
    	}

		if ( !SQL_SUCCEEDED( ret ) && ret != SQL_NO_DATA )
		{
        	SQLFREESTMT( cl_connection,
                cl_statement -> fetch_statement,
                SQL_DROP );

			cl_statement -> fetch_statement = SQL_NULL_HSTMT;
		}

    	return ret;
	}

	cl_statement -> fetch_done = 0;

    ret = fetch_row( cl_statement, 
            cl_statement -> curr_rowset_start +
                cl_statement -> cursor_pos - 1,
            -1 );

	if ( cl_statement -> fetch_statement != SQL_NULL_HSTMT )
	{
        SQLFREESTMT( cl_connection,
                cl_statement -> fetch_statement,
                SQL_DROP );

		cl_statement -> fetch_statement = SQL_NULL_HSTMT;
	}

	ret = SQLALLOCSTMT( cl_connection, 
            cl_connection -> driver_dbc,
            (SQLHSTMT) &cl_statement -> fetch_statement,
            NULL );

    if ( !SQL_SUCCEEDED( ret ))
    {       
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_S1000, "SQLAllocStmt failed in driver",
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

        return SQL_ERROR;
    }

    /*
     * append the cryterior to the end of the statement, binding
     * the comumns while we are at it
     */

    strcpy((char*) sql, cl_statement -> sql_text );

    /*
     * This is somewhat simplistic, but workable
     */

    if ( strstr( (char*) sql, "WHERE" ))
    {
    	strcat((char*) sql, " AND" );
    }
    else
    {
    	strcat((char*) sql, " WHERE" );
    }

    /*
     * this works because the bound list is in column order
     */

    bound_columns = cl_statement -> bound_columns;

	first = 1;
	next_bind = 0;

	while( bound_columns ) 
    {
        char addon[ 256 ];
		int col = bound_columns -> column_number;

		/*
		 * Don't bind long types
		 */

		if ( cl_statement -> data_type[ col ] == SQL_LONGVARCHAR || 
						cl_statement -> data_type[ col ] == SQL_LONGVARBINARY )
		{
		}
		else
		{
			if ( bound_columns -> len_ind == -1 )
			{
        		sprintf( addon, " %s IS NULL", cl_statement -> column_names[ col - 1 ] );

        		if ( !first )
        		{
            		strcat((char*) sql, " AND" );
        		}

        		strcat((char*) sql, addon );
				first = 0;
			}
			else
			{	
        		sprintf( addon, " %s = ?", cl_statement -> column_names[ col - 1 ] );

        		if ( !first )
        		{
            		strcat((char*) sql, " AND" );
        		}
		
        		strcat((char*) sql, addon );
				first = 0;
		
        		if ( CHECK_SQLBINDPARAMETER( cl_connection ))
        		{
            		ret = SQLBINDPARAMETER( cl_connection,
                    		cl_statement -> fetch_statement,
                    		next_bind + 1,
                    		SQL_PARAM_INPUT,
                    		bound_columns -> bound_type,
                    		cl_statement -> data_type[ col - 1 ],
                    		cl_statement -> column_size[ col - 1 ],
                    		cl_statement -> decimal_digits[ col - 1 ],
                    		bound_columns -> local_buffer,
                    		bound_columns -> bound_length,
                    		&bound_columns -> len_ind );
        		}
        		else
        		{
            		ret = SQLBINDPARAM( cl_connection,
                    		cl_statement -> fetch_statement,
                    		next_bind + 1,
                    		bound_columns -> bound_type,
                    		cl_statement -> data_type[ col - 1 ],
                    		cl_statement -> column_size[ col - 1 ],
                    		cl_statement -> decimal_digits[ col - 1 ],
                    		bound_columns -> local_buffer,
                    		&bound_columns -> len_ind );
        		}
	
        		if ( !SQL_SUCCEEDED( ret ))
        		{
            		cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                    		ERROR_SL010, NULL,
                    		cl_statement -> dm_statement -> connection ->
                        		environment -> requested_version );

					if ( !SQL_SUCCEEDED( ret ) && ret != SQL_NO_DATA )
					{
						SQLCHAR sqlstate[ 6 ];
						SQLINTEGER native_error;
						SQLSMALLINT ind;
						SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
						SQLRETURN ret;
						int rec = 1;
				
						/*
						* get the error from the driver before
						* loseing the stmt
						*/
				
						do
						{
							if ( CHECK_SQLERROR( cl_connection ))
							{
								ret = SQLERROR( cl_connection,
										SQL_NULL_HENV,
										SQL_NULL_HSTMT,
										cl_statement -> fetch_statement,
										sqlstate,
										&native_error,
										message_text,
										sizeof( message_text ),
										&ind );
							}
							else if ( CHECK_SQLGETDIAGREC( cl_connection ))
							{
								ret = SQLGETDIAGREC( cl_connection,
										SQL_HANDLE_STMT,
										cl_statement -> fetch_statement,
										rec ++,
										sqlstate,
										&native_error,
										message_text,
										sizeof( message_text ),
										&ind );
							}
							else
							{
								ret = SQL_NO_DATA;
							}
				
							if ( ret != SQL_NO_DATA )
							{
								cl_statement -> cl_connection -> dh.__post_internal_error_ex( &cl_statement -> dm_statement -> error,
										sqlstate, native_error, message_text,
									SUBCLASS_ODBC, SUBCLASS_ODBC );
							}
						}
						while ( SQL_SUCCEEDED( ret ));
					}
		
            		SQLFREESTMT( cl_connection,
                    		cl_statement -> fetch_statement, 
                    		SQL_DROP );
		
					cl_statement -> fetch_statement = SQL_NULL_HSTMT;
	
            		return SQL_ERROR;
        		}
				next_bind ++;
			}
		}
	
        bound_columns = bound_columns -> next;
    }

    if ( CHECK_SQLEXECDIRECT( cl_connection ))
    {
        ret = SQLEXECDIRECT( cl_connection,
                    cl_statement -> fetch_statement,
                    sql,
                    strlen((char*) sql ));
    }
    else
    {
        ret = SQLPREPARE( cl_connection,
                    cl_statement -> fetch_statement,
                    sql,
                    strlen((char*) sql ));

        if ( SQL_SUCCEEDED( ret ))
        {
            ret = SQLEXECUTE( cl_connection,
                    cl_statement -> fetch_statement );
        }
    }

    if ( !SQL_SUCCEEDED( ret ))
    {
        cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_SL004, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );

		if ( !SQL_SUCCEEDED( ret ) && ret != SQL_NO_DATA )
		{
			SQLCHAR sqlstate[ 6 ];
			SQLINTEGER native_error;
			SQLSMALLINT ind;
			SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
			SQLRETURN ret;
			int rec = 1;
	
			/*
			* get the error from the driver before
			* loseing the stmt
			*/
	
			do
			{
				if ( CHECK_SQLERROR( cl_connection ))
				{
					ret = SQLERROR( cl_connection,
							SQL_NULL_HENV,
							SQL_NULL_HSTMT,
							cl_statement -> fetch_statement,
							sqlstate,
							&native_error,
							message_text,
							sizeof( message_text ),
							&ind );
				}
				else if ( CHECK_SQLGETDIAGREC( cl_connection ))
				{
					ret = SQLGETDIAGREC( cl_connection,
							SQL_HANDLE_STMT,
							cl_statement -> fetch_statement,
							rec ++,
							sqlstate,
							&native_error,
							message_text,
							sizeof( message_text ),
							&ind );
				}
				else
				{
					ret = SQL_NO_DATA;
				}

				if ( ret != SQL_NO_DATA )
				{
					cl_statement -> cl_connection -> dh.__post_internal_error_ex( &cl_statement -> dm_statement -> error,
							sqlstate, native_error, message_text,
						SUBCLASS_ODBC, SUBCLASS_ODBC );
				}
			}
			while ( SQL_SUCCEEDED( ret ));
		}

        SQLFREESTMT( cl_connection,
                cl_statement -> fetch_statement,
                SQL_DROP );

		cl_statement -> fetch_statement = SQL_NULL_HSTMT;

        return SQL_ERROR;
    }

    ret = SQLFETCH( cl_connection,
            cl_statement -> fetch_statement );

    if ( !SQL_SUCCEEDED( ret ))
    {
		if ( ret == SQL_NO_DATA )
		{
        	cl_statement -> cl_connection -> dh.__post_internal_error( &cl_statement -> dm_statement -> error,
                ERROR_SL004, NULL,
                cl_statement -> dm_statement -> connection ->
                    environment -> requested_version );
		}
		else
		{
        	SQLCHAR sqlstate[ 6 ];
        	SQLINTEGER native_error;
        	SQLSMALLINT ind;
        	SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
        	SQLRETURN ret;
			int rec = 1;
	
        	/*
         	 * get the error from the driver before
         	 * loseing the connection
         	 */
	
        	do
        	{
    			if ( CHECK_SQLERROR( cl_connection ))
				{
            		ret = SQLERROR( cl_connection,
                    	SQL_NULL_HENV,
                    	SQL_NULL_HSTMT,
                    	cl_statement -> fetch_statement,
                    	sqlstate,
                    	&native_error,
                    	message_text,
                    	sizeof( message_text ),
                    	&ind );
				}
				else if ( CHECK_SQLGETDIAGREC( cl_connection ))
				{
            		ret = SQLGETDIAGREC( cl_connection,
                    	SQL_HANDLE_STMT,
                    	cl_statement -> fetch_statement,
						rec ++,
                    	sqlstate,
                    	&native_error,
                    	message_text,
                    	sizeof( message_text ),
                    	&ind );
				}
				else
				{
					ret = SQL_NO_DATA;
				}

				if ( ret != SQL_NO_DATA )
				{
            		cl_statement -> cl_connection -> dh.__post_internal_error_ex( &cl_statement -> dm_statement -> error,
                    	sqlstate, native_error, message_text,
                    	SUBCLASS_ODBC, SUBCLASS_ODBC );
				}
        	}
        	while ( SQL_SUCCEEDED( ret ));
		}

        SQLFREESTMT( cl_connection,
                cl_statement -> fetch_statement,
                SQL_DROP );

		cl_statement -> fetch_statement = SQL_NULL_HSTMT;

        return SQL_ERROR;
    }

    ret = SQLGETDATA( cl_connection,
                cl_statement -> fetch_statement, 
                column_number,
                target_type,
                target_value,
                buffer_length,
                strlen_or_ind );

    if ( !SQL_SUCCEEDED( ret ))
    {
        SQLCHAR sqlstate[ 6 ];
        SQLINTEGER native_error;
        SQLSMALLINT ind;
        SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
        SQLRETURN ret;
		int rec = 1;

        /*
         * get the error from the driver before
         * loseing the connection
         */

		do
        {
    		if ( CHECK_SQLERROR( cl_connection ))
			{
            	ret = SQLERROR( cl_connection,
                    	SQL_NULL_HENV,
                    	SQL_NULL_HSTMT,
                    	cl_statement -> fetch_statement,
                    	sqlstate,
                    	&native_error,
                    	message_text,
                    	sizeof( message_text ),
                    	&ind );
			}
			else if ( CHECK_SQLGETDIAGREC( cl_connection ))
			{
            	ret = SQLGETDIAGREC( cl_connection,
                    	SQL_HANDLE_STMT,
                    	cl_statement -> fetch_statement,
						rec ++,
                    	sqlstate,
                    	&native_error,
                    	message_text,
                    	sizeof( message_text ),
                    	&ind );
			}
			else
			{
				ret = SQL_NO_DATA;
			}

			if ( ret != SQL_NO_DATA )
			{
            	cl_statement -> cl_connection -> dh.__post_internal_error_ex( &cl_statement -> dm_statement -> error,
                    	sqlstate, native_error, message_text,
                    	SUBCLASS_ODBC, SUBCLASS_ODBC );
        	}
        }
        while ( SQL_SUCCEEDED( ret ));
    }

	if ( !SQL_SUCCEEDED( ret ))
	{
        SQLFREESTMT( cl_connection,
                cl_statement -> fetch_statement,
                SQL_DROP );

		cl_statement -> fetch_statement = SQL_NULL_HSTMT;
	}

    return ret;
}
