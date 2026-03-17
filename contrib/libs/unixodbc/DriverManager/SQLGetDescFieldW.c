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
 * $Id: SQLGetDescFieldW.c,v 1.9 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetDescFieldW.c,v $
 * Revision 1.9  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.8  2008/08/29 08:01:39  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.7  2007/02/28 15:37:48  lurcher
 * deal with drivers that call internal W functions and end up in the driver manager. controlled by the --enable-handlemap configure arg
 *
 * Revision 1.6  2004/11/22 17:02:49  lurcher
 * Fix unicode/ansi conversion in the SQLGet functions
 *
 * Revision 1.5  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.4  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.3  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.2  2002/07/24 08:49:52  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.4  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.3  2001/04/17 16:29:39  nick
 *
 * More checks and autotest fixes
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLGetDescFieldW.c,v $";

SQLRETURN SQLGetDescFieldW( SQLHDESC descriptor_handle,
           SQLSMALLINT rec_number, 
           SQLSMALLINT field_identifier,
           SQLPOINTER value, 
           SQLINTEGER buffer_length,
           SQLINTEGER *string_length )
{
    /*
     * not quite sure how the descriptor can be
     * allocated to a statement, all the documentation talks
     * about state transitions on statement states, but the
     * descriptor may be allocated with more than one statement
     * at one time. Which one should I check ?
     */
    DMHDESC descriptor = (DMHDESC) descriptor_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];
    int isStrField = 0;

    /*
     * check descriptor
     */

    if ( !__validate_desc( descriptor ))
    {
        dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

#ifdef WITH_HANDLE_REDIRECT
		{
			DMHDESC parent_desc;

			parent_desc = find_parent_handle( descriptor, SQL_HANDLE_DESC );

			if ( parent_desc ) {
        		dm_log_write( __FILE__, 
                	__LINE__, 
                    	LOG_INFO, 
                    	LOG_INFO, 
                    	"Info: found parent handle" );

				if ( CHECK_SQLGETDESCFIELDW( parent_desc -> connection ))
				{
        			dm_log_write( __FILE__, 
                		__LINE__, 
                   		 	LOG_INFO, 
                   		 	LOG_INFO, 
                   		 	"Info: calling redirected driver function" );

                	return  SQLGETDESCFIELDW( parent_desc -> connection,
							descriptor,
							rec_number,
							field_identifier,
							value,
							buffer_length,
							string_length );
				}
			}
		}
#endif
        return SQL_INVALID_HANDLE;
    }

    function_entry( descriptor );

    if ( log_info.log_flag )
    {
        sprintf( descriptor -> msg, "\n\t\tEntry:\
\n\t\t\tDescriptor = %p\
\n\t\t\tRec Number = %d\
\n\t\t\tField Attr = %s\
\n\t\t\tValue = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tStrLen = %p",
                descriptor,
                rec_number,
                __desc_attr_as_string( s1, field_identifier ),
                value, 
                (int)buffer_length,
                (void*)string_length );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                descriptor -> msg );
    }

    thread_protect( SQL_HANDLE_DESC, descriptor );

    if ( descriptor -> connection -> state < STATE_C4 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &descriptor -> error,
                ERROR_HY010, NULL,
                descriptor -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DESC, descriptor, SQL_ERROR );
    }

    /*
     * check status of statements associated with this descriptor
     */

    if( __check_stmt_from_desc( descriptor, STATE_S8 ) ||
        __check_stmt_from_desc( descriptor, STATE_S9 ) ||
        __check_stmt_from_desc( descriptor, STATE_S10 ) ||
        __check_stmt_from_desc( descriptor, STATE_S11 ) ||
        __check_stmt_from_desc( descriptor, STATE_S12 ) ||
        __check_stmt_from_desc( descriptor, STATE_S13 ) ||
        __check_stmt_from_desc( descriptor, STATE_S14 ) ||
        __check_stmt_from_desc( descriptor, STATE_S15 )) {

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &descriptor -> error,
                ERROR_HY010, NULL,
                descriptor -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DESC, descriptor, SQL_ERROR );
    }

    if( __check_stmt_from_desc_ird( descriptor, STATE_S1 )) {

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY007" );

        __post_internal_error( &descriptor -> error,
                ERROR_HY007, NULL,
                descriptor -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DESC, descriptor, SQL_ERROR );
    }

    if ( rec_number < 0 )
    {
        __post_internal_error( &descriptor -> error,
                ERROR_07009, NULL,
                descriptor -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DESC, descriptor, SQL_ERROR );
    }
    
    switch ( field_identifier )
    {
    /* Fixed-length fields: buffer_length is ignored */
    case SQL_DESC_ALLOC_TYPE:
    case SQL_DESC_ARRAY_SIZE:
    case SQL_DESC_ARRAY_STATUS_PTR:
    case SQL_DESC_BIND_OFFSET_PTR:
    case SQL_DESC_BIND_TYPE:
    case SQL_DESC_COUNT:
    case SQL_DESC_ROWS_PROCESSED_PTR:
    case SQL_DESC_AUTO_UNIQUE_VALUE:
    case SQL_DESC_CASE_SENSITIVE:
    case SQL_DESC_CONCISE_TYPE:
    case SQL_DESC_DATA_PTR:
    case SQL_DESC_DATETIME_INTERVAL_CODE:
    case SQL_DESC_DATETIME_INTERVAL_PRECISION:
    case SQL_DESC_DISPLAY_SIZE:
    case SQL_DESC_FIXED_PREC_SCALE:
    case SQL_DESC_INDICATOR_PTR:
    case SQL_DESC_LENGTH:
    case SQL_DESC_NULLABLE:
    case SQL_DESC_NUM_PREC_RADIX:
    case SQL_DESC_OCTET_LENGTH:
    case SQL_DESC_OCTET_LENGTH_PTR:
    case SQL_DESC_PARAMETER_TYPE:
    case SQL_DESC_PRECISION:
    case SQL_DESC_ROWVER:
    case SQL_DESC_SCALE:
    case SQL_DESC_SEARCHABLE:
    case SQL_DESC_TYPE:
    case SQL_DESC_UNNAMED:
    case SQL_DESC_UNSIGNED:
    case SQL_DESC_UPDATABLE:
        isStrField = 0;
        break;
    /* Pointer to data: buffer_length must be valid */
    case SQL_DESC_BASE_COLUMN_NAME:
    case SQL_DESC_BASE_TABLE_NAME:
    case SQL_DESC_CATALOG_NAME:
    case SQL_DESC_LABEL:
    case SQL_DESC_LITERAL_PREFIX:
    case SQL_DESC_LITERAL_SUFFIX:
    case SQL_DESC_LOCAL_TYPE_NAME:
    case SQL_DESC_NAME:
    case SQL_DESC_SCHEMA_NAME:
    case SQL_DESC_TABLE_NAME:
    case SQL_DESC_TYPE_NAME:
        isStrField = 1;
        break;
    default:
        isStrField = buffer_length != SQL_IS_POINTER && buffer_length != SQL_IS_INTEGER
            && buffer_length != SQL_IS_UINTEGER && buffer_length != SQL_IS_SMALLINT &&
            buffer_length != SQL_IS_USMALLINT;
    }
    
    if ( isStrField && buffer_length < 0 )
    {
        __post_internal_error( &descriptor -> error,
            ERROR_HY090, NULL,
            descriptor -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_DESC, descriptor, SQL_ERROR );
    }

    if ( descriptor -> connection -> unicode_driver ||
		    CHECK_SQLGETDESCFIELDW( descriptor -> connection ))
    {
        if ( !CHECK_SQLGETDESCFIELDW( descriptor -> connection ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM001" );

            __post_internal_error( &descriptor -> error,
                    ERROR_IM001, NULL,
                    descriptor -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DESC, descriptor, SQL_ERROR );
        }

        ret = SQLGETDESCFIELDW( descriptor -> connection,
                descriptor -> driver_desc,
                rec_number, 
                field_identifier,
                value, 
                buffer_length,
                string_length );
    }
    else
    {
        SQLCHAR *as1 = NULL;

        if ( !CHECK_SQLGETDESCFIELD( descriptor -> connection ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM001" );

            __post_internal_error( &descriptor -> error,
                    ERROR_IM001, NULL,
                    descriptor -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_DESC, descriptor, SQL_ERROR );
        }

        if ( isStrField && buffer_length > 0 && value )
        {
            as1 = malloc( buffer_length + 1 );
        }

        ret = SQLGETDESCFIELD( descriptor -> connection,
                descriptor -> driver_desc,
                rec_number, 
                field_identifier,
                as1 ? as1 : value, 
                buffer_length,
                string_length );

        if ( isStrField && SQL_SUCCEEDED( ret ) && value )
        {
            if ( as1 && buffer_length > 0)
            {
                ansi_to_unicode_copy( value, (char*) as1, SQL_NTS, descriptor -> connection, NULL );
            }
        }

        if ( as1 )
        {
            free( as1 );
        }
    }

    if ( log_info.log_flag )
    {
        sprintf( descriptor -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, s1 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                descriptor -> msg );
    }

    return function_return( SQL_HANDLE_DESC, descriptor, ret, DEFER_R3 );
}
