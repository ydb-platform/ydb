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
 * $Id: SQLCopyDesc.c,v 1.8 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLCopyDesc.c,v $
 * Revision 1.8  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.7  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.6  2004/03/30 13:20:11  lurcher
 *
 *
 * Fix problem with SQLCopyDesc
 * Add additional target for iconv
 *
 * Revision 1.5  2003/10/30 18:20:45  lurcher
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
 * Revision 1.3  2002/09/18 14:49:32  lurcher
 *
 * DataManagerII additions and some more threading fixes
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.8  2001/05/26 19:11:37  ngorham
 *
 * Add SQLCopyDesc functionality and fix bug that was stopping messages
 * coming out of SQLConnect
 *
 * Revision 1.7  1999/11/13 23:40:58  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.6  1999/10/24 23:54:17  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.5  1999/09/21 22:34:24  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.4  1999/07/10 21:10:16  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.3  1999/07/04 21:05:07  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/30 23:56:54  ngorham
 *
 * Add initial thread safety code
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
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLCopyDesc.c,v $ $Revision: 1.8 $";

struct cdesc
{
    int             field_identifier;
    int             field_type;
};

/*
 * note that SQL_VARCHAR indicates a pointer type, not a string
 */

static struct cdesc header_fields[] = 
{
        { SQL_DESC_ARRAY_SIZE,          SQL_INTEGER },
        { SQL_DESC_ARRAY_STATUS_PTR,    SQL_VARCHAR },
        { SQL_DESC_BIND_OFFSET_PTR,     SQL_VARCHAR },
        { SQL_DESC_BIND_TYPE,           SQL_VARCHAR },
        { SQL_DESC_COUNT,               SQL_SMALLINT },
        { SQL_DESC_ROWS_PROCESSED_PTR,  SQL_VARCHAR }
};

static struct cdesc rec_fields[] = 
{
        { SQL_DESC_CONCISE_TYPE,                SQL_SMALLINT },
        { SQL_DESC_LENGTH,                      SQL_INTEGER },
        { SQL_DESC_OCTET_LENGTH,                SQL_INTEGER },
        { SQL_DESC_PARAMETER_TYPE,              SQL_SMALLINT },
        { SQL_DESC_NUM_PREC_RADIX,              SQL_INTEGER },
        { SQL_DESC_PRECISION,                   SQL_SMALLINT },
        { SQL_DESC_SCALE,                       SQL_SMALLINT },
        { SQL_DESC_DATETIME_INTERVAL_CODE,      SQL_SMALLINT },
        { SQL_DESC_DATETIME_INTERVAL_PRECISION, SQL_SMALLINT },
        { SQL_DESC_DATA_PTR,                    SQL_VARCHAR },
        { SQL_DESC_INDICATOR_PTR,               SQL_VARCHAR },
        { SQL_DESC_OCTET_LENGTH_PTR,            SQL_VARCHAR }
};

SQLRETURN SQLCopyDesc( SQLHDESC source_desc_handle,
           SQLHDESC target_desc_handle )
{
    DMHDESC src_descriptor = (DMHDESC)source_desc_handle;
    DMHDESC target_descriptor = (DMHDESC)target_desc_handle;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

    /*
     * check descriptor
     */

    if ( !__validate_desc( src_descriptor ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

        return SQL_INVALID_HANDLE;
    }
    if ( !__validate_desc( target_descriptor ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

        return SQL_INVALID_HANDLE;
    }

    function_entry( src_descriptor );
    function_entry( target_descriptor );

    if ( log_info.log_flag )
    {
        sprintf( src_descriptor -> msg, "\n\t\tEntry:\
\n\t\t\tSource Descriptor = %p\
\n\t\t\tTarget Descriptor = %p", 
                src_descriptor,
                target_descriptor );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                src_descriptor -> msg );
    }

	if ( src_descriptor -> associated_with ) {
    	DMHSTMT statement = (DMHSTMT) src_descriptor -> associated_with;

    	if ( statement -> state == STATE_S8 ||
            statement -> state == STATE_S9 ||
            statement -> state == STATE_S10 ||
            statement -> state == STATE_S11 ||
            statement -> state == STATE_S12 ||
            statement -> state == STATE_S13 ||
            statement -> state == STATE_S14 ||
            statement -> state == STATE_S15 )
    	{
       	 	dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        	__post_internal_error( &src_descriptor -> error,
                ERROR_HY010, NULL,
                src_descriptor -> connection -> environment -> requested_version );

			function_return_nodrv( SQL_HANDLE_DESC, target_descriptor, SQL_SUCCESS );
        	return function_return_nodrv( SQL_HANDLE_DESC, src_descriptor, SQL_ERROR );
    	}
	}

	if ( target_descriptor -> associated_with ) {
    	DMHSTMT statement = (DMHSTMT) target_descriptor -> associated_with;

    	if ( statement -> state == STATE_S8 ||
            statement -> state == STATE_S9 ||
            statement -> state == STATE_S10 ||
            statement -> state == STATE_S11 ||
            statement -> state == STATE_S12 )
    	{
       	 	dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        	__post_internal_error( &target_descriptor -> error,
                ERROR_HY010, NULL,
                target_descriptor -> connection -> environment -> requested_version );

        	return function_return_nodrv( IGNORE_THREAD, src_descriptor, SQL_ERROR );
    	}
	}

    /*
     * if both descriptors are from the same driver then we can just
     * pass it on
     */

    if ( (src_descriptor -> connection == target_descriptor -> connection ||
          !strcmp(src_descriptor -> connection -> dl_name,
                  target_descriptor -> connection -> dl_name) ) && 
            CHECK_SQLCOPYDESC( src_descriptor -> connection ))
    {
        SQLRETURN ret;

        /*
         * protect the common connection
         */

        thread_protect( SQL_HANDLE_DBC, src_descriptor -> connection );

        ret = SQLCOPYDESC( src_descriptor -> connection,
                src_descriptor -> driver_desc,
                target_descriptor -> driver_desc );

        if ( log_info.log_flag )
        {
            sprintf( target_descriptor -> msg, 
                    "\n\t\tExit:[%s]",
                        __get_return_status( ret, s1 ));

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    target_descriptor -> msg );
        }

        thread_release( SQL_HANDLE_DBC, src_descriptor -> connection );

        return function_return( IGNORE_THREAD, target_descriptor, ret, DEFER_R3 );
    }
    else
    {
        /*
         * TODO copy from one to the other
         * protect the common environment
         */

        SQLRETURN ret = SQL_SUCCESS;
        SQLSMALLINT count;
        SQLSMALLINT sval;
        SQLINTEGER ival;
        SQLPOINTER pval;

        if ( src_descriptor -> connection == target_descriptor -> connection )
            thread_protect( SQL_HANDLE_DBC, src_descriptor -> connection );
        else
            thread_protect( SQL_HANDLE_ENV, src_descriptor -> connection -> environment );

        if ( !CHECK_SQLGETDESCFIELD( src_descriptor -> connection ) ||
                !CHECK_SQLSETDESCFIELD( src_descriptor -> connection ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM001" );

            __post_internal_error( &target_descriptor -> error,
                    ERROR_IM001, NULL,
                    target_descriptor -> connection -> environment -> requested_version );

            if ( src_descriptor -> connection == target_descriptor -> connection )
                thread_release( SQL_HANDLE_DBC, src_descriptor -> connection );
            else
                thread_release( SQL_HANDLE_ENV, src_descriptor -> connection -> environment );

            return function_return_nodrv( IGNORE_THREAD, target_descriptor, SQL_ERROR );
        }

        /*
         * get the count from the source field
         */

        ret = SQLGETDESCFIELD( src_descriptor -> connection,
                                src_descriptor -> driver_desc,
                                0, 
                                SQL_DESC_COUNT,
                                &count,
                                sizeof( count ),
                                NULL );

        if ( SQL_SUCCEEDED( ret ))
        {
            /*
             * copy the header fields
             */
            int i;

            for ( i = 0; i < sizeof( header_fields ) / sizeof( header_fields[ 0 ] ); i ++ )
            {
                if ( header_fields[ i ].field_type == SQL_INTEGER )
                {
                    ret = SQLGETDESCFIELD( src_descriptor -> connection,
                                src_descriptor -> driver_desc,
                                0, 
                                header_fields[ i ].field_identifier,
                                &ival,
                                sizeof( ival ),
                                NULL );
                }
                else if ( header_fields[ i ].field_type == SQL_SMALLINT )
                {
                    ret = SQLGETDESCFIELD( src_descriptor -> connection,
                                src_descriptor -> driver_desc,
                                0, 
                                header_fields[ i ].field_identifier,
                                &sval,
                                sizeof( sval ),
                                NULL );
                }
                if ( header_fields[ i ].field_type == SQL_VARCHAR )
                {
                    ret = SQLGETDESCFIELD( src_descriptor -> connection,
                                src_descriptor -> driver_desc,
                                0, 
                                header_fields[ i ].field_identifier,
                                &pval,
                                sizeof( pval ),
                                NULL );
                }

                if ( SQL_SUCCEEDED( ret ))
                {
                    if ( header_fields[ i ].field_type == SQL_INTEGER )
                    {
                        ret = SQLSETDESCFIELD( target_descriptor -> connection,
                                        target_descriptor -> driver_desc,
                                        0, 
                                        header_fields[ i ].field_identifier,
                                        (SQLPOINTER)(intptr_t) ival,
                                        sizeof( ival ));
                    }
                    else if ( header_fields[ i ].field_type == SQL_SMALLINT )
                    {
                        ret = SQLSETDESCFIELD( target_descriptor -> connection,
                                        target_descriptor -> driver_desc,
                                        0, 
                                        header_fields[ i ].field_identifier,
                                        (SQLPOINTER)(intptr_t) sval,
                                        sizeof( sval ));
                    }
                    else if ( header_fields[ i ].field_type == SQL_VARCHAR )
                    {
                        ret = SQLSETDESCFIELD( target_descriptor -> connection,
                                        target_descriptor -> driver_desc,
                                        0, 
                                        header_fields[ i ].field_identifier,
                                        pval,
                                        sizeof( pval ));
                    }
                }

                if ( !SQL_SUCCEEDED( ret ))
                    break;
            }
        }

        if ( SQL_SUCCEEDED( ret ))
        {
            /*
             * copy the records
             */
            int i, rec;

            for ( rec = 0; rec <= count; rec ++ )
            {
                for ( i = 0; i < sizeof( rec_fields ) / sizeof( rec_fields[ 0 ] ); i ++ )
                {
                    if ( rec_fields[ i ].field_type == SQL_INTEGER )
                    {
                        ret = SQLGETDESCFIELD( src_descriptor -> connection,
                                    src_descriptor -> driver_desc,
                                    rec, 
                                    rec_fields[ i ].field_identifier,
                                    &ival,
                                    sizeof( ival ),
                                    NULL );
                    }
                    else if ( rec_fields[ i ].field_type == SQL_SMALLINT )
                    {
                        ret = SQLGETDESCFIELD( src_descriptor -> connection,
                                    src_descriptor -> driver_desc,
                                    rec, 
                                    rec_fields[ i ].field_identifier,
                                    &sval,
                                    sizeof( sval ),
                                    NULL );
                    }
                    if ( rec_fields[ i ].field_type == SQL_VARCHAR )
                    {
                        ret = SQLGETDESCFIELD( src_descriptor -> connection,
                                    src_descriptor -> driver_desc,
                                    rec, 
                                    rec_fields[ i ].field_identifier,
                                    &pval,
                                    sizeof( pval ),
                                    NULL );
                    }

                    if ( SQL_SUCCEEDED( ret ))
                    {
                        if ( rec_fields[ i ].field_type == SQL_INTEGER )
                        {
                            ret = SQLSETDESCFIELD( target_descriptor -> connection,
                                            target_descriptor -> driver_desc,
                                            rec, 
                                            rec_fields[ i ].field_identifier,
                                            (SQLPOINTER)(intptr_t) ival,
                                            sizeof( ival ));
                        }
                        else if ( rec_fields[ i ].field_type == SQL_SMALLINT )
                        {
                            ret = SQLSETDESCFIELD( target_descriptor -> connection,
                                            target_descriptor -> driver_desc,
                                            rec, 
                                            rec_fields[ i ].field_identifier,
                                            (SQLPOINTER)(intptr_t) sval,
                                            sizeof( sval ));
                        }
                        else if ( rec_fields[ i ].field_type == SQL_VARCHAR )
                        {
                            ret = SQLSETDESCFIELD( target_descriptor -> connection,
                                            target_descriptor -> driver_desc,
                                            rec, 
                                            rec_fields[ i ].field_identifier,
                                            pval,
                                            sizeof( pval ));
                        }
                    }

                    if ( !SQL_SUCCEEDED( ret ))
                        break;
                }
                if ( !SQL_SUCCEEDED( ret ))
                    break;
            }
        }

        if ( log_info.log_flag )
        {
            sprintf( src_descriptor -> msg, 
                    "\n\t\tExit:[%s]",
                        __get_return_status( ret, s1 ));

            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    src_descriptor -> msg );
        }

        if ( src_descriptor -> connection == target_descriptor -> connection )
            thread_release( SQL_HANDLE_DBC, src_descriptor -> connection );
        else
            thread_release( SQL_HANDLE_ENV, src_descriptor -> connection -> environment );

        return function_return( IGNORE_THREAD, target_descriptor, ret, DEFER_R3 );
    }
}
