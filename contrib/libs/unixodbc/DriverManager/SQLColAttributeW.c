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
 * $Id: SQLColAttributeW.c,v 1.14 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLColAttributeW.c,v $
 * Revision 1.14  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.13  2008/08/29 08:01:38  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.12  2007/04/02 10:50:18  lurcher
 * Fix some 64bit problems (only when sizeof(SQLLEN) == 8 )
 *
 * Revision 1.11  2004/11/22 17:02:48  lurcher
 * Fix unicode/ansi conversion in the SQLGet functions
 *
 * Revision 1.10  2004/10/30 20:19:21  peteralexharvey
 * ODBC spec says last arg for SQLColAttribute() is SQLPOINTER not (SQLEN*).
 * So switched back to SQLPOINTER.
 *
 * Revision 1.9  2004/10/29 10:00:36  lurcher
 * Fix SQLColAttribute protype
 *
 * Revision 1.8  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.7  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.6  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.5  2002/08/19 09:11:49  lurcher
 *
 * Fix Maxor ineffiecny in Postgres Drivers, and fix a return state
 *
 * Revision 1.4  2002/07/24 08:49:51  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.3  2002/04/25 15:16:46  lurcher
 *
 * Fix bug with SQLCOlAttribute(s)(W) where a column of zero could not be
 * used to get the count value
 *
 * Revision 1.2  2001/11/16 11:39:17  lurcher
 *
 * Add mapping between ODBC 2 and ODBC 3 types for SQLColAttribute(s)(W)
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.3  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.2  2001/04/12 17:43:35  nick
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

static char const rcsid[]= "$RCSfile: SQLColAttributeW.c,v $";

SQLRETURN SQLColAttributeW ( SQLHSTMT statement_handle,
           SQLUSMALLINT column_number,
           SQLUSMALLINT field_identifier,
           SQLPOINTER character_attribute,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length,
           SQLLEN *numeric_attribute )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

    /*
     * check statement
     */
    if ( !__validate_stmt( statement ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

        return SQL_INVALID_HANDLE;
    }

    function_entry( statement );

    if ( log_info.log_flag )
    {
        sprintf( statement -> msg, "\n\t\tEntry:\
\n\t\t\tStatement = %p\
\n\t\t\tColumn Number = %d\
\n\t\t\tField Identifier = %s\
\n\t\t\tCharacter Attr = %p\
\n\t\t\tBuffer Length = %d\
\n\t\t\tString Length = %p\
\n\t\t\tNumeric Attribute = %p",
                statement,
                column_number,
                __col_attr_as_string( s1, field_identifier ),
                character_attribute,
                buffer_length,
                string_length,
                numeric_attribute );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    thread_protect( SQL_HANDLE_STMT, statement );

    if ( column_number == 0 &&
            statement -> bookmarks_on == SQL_UB_OFF && statement -> connection -> bookmarks_on == SQL_UB_OFF &&
            field_identifier != SQL_DESC_COUNT )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 07009" );

        __post_internal_error_api( &statement -> error,
                ERROR_07009, NULL,
                statement -> connection -> environment -> requested_version,
                SQL_API_SQLCOLATTRIBUTE );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

	/*
	 * Commented out for now because most drivers can not calc num cols
	 * before Execute (they have no parse). - PAH
	 *
	
    if ( field_identifier != SQL_DESC_COUNT &&
            statement -> numcols < column_number )
    {
        __post_internal_error( &statement -> error,
                ERROR_07009, NULL,
                statement -> connection -> environment -> requested_version );
        return function_return( statement, SQL_ERROR );
    }
	
	*/

    /*
     * check states
     */
    if ( statement -> state == STATE_S1 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &statement -> error,
                ERROR_HY010, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
    /*
    else if ( statement -> state == STATE_S2 &&
            field_identifier != SQL_DESC_COUNT )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 07005" );

        __post_internal_error( &statement -> error,
                ERROR_07005, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
    */
    else if ( statement -> state == STATE_S4 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 24000" );

        __post_internal_error( &statement -> error,
                ERROR_24000, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }
    else if ( statement -> state == STATE_S8 ||
            statement -> state == STATE_S9 ||
            statement -> state == STATE_S10 ||
            statement -> state == STATE_S13 ||
            statement -> state == STATE_S14 ||
            statement -> state == STATE_S15 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &statement -> error,
                ERROR_HY010, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

    if ( statement -> state == STATE_S11 ||
            statement -> state == STATE_S12 )
    {
        if ( statement -> interupted_func != SQL_API_SQLCOLATTRIBUTE )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: HY010" );

            __post_internal_error( &statement -> error,
                    ERROR_HY010, NULL,
                    statement -> connection -> environment -> requested_version );

            return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
        }
    }

    switch ( field_identifier )
    {
        case SQL_COLUMN_QUALIFIER_NAME:
        case SQL_COLUMN_NAME:
        case SQL_COLUMN_LABEL:
        case SQL_COLUMN_OWNER_NAME:
        case SQL_COLUMN_TABLE_NAME:
        case SQL_COLUMN_TYPE_NAME:
        case SQL_DESC_BASE_COLUMN_NAME:
        case SQL_DESC_BASE_TABLE_NAME:
        case SQL_DESC_LITERAL_PREFIX:
        case SQL_DESC_LITERAL_SUFFIX:
        case SQL_DESC_LOCAL_TYPE_NAME:
        case SQL_DESC_NAME:
            if ( buffer_length < 0 && buffer_length != SQL_NTS )
            {
                __post_internal_error( &statement -> error,
                    ERROR_HY090, NULL,
                    statement -> connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
            }
    }

    if ( statement -> connection -> unicode_driver ||
		CHECK_SQLCOLATTRIBUTEW( statement -> connection ) ||
		CHECK_SQLCOLATTRIBUTESW( statement -> connection ))
    {
        if ( !CHECK_SQLCOLATTRIBUTEW( statement -> connection ))
        {
            if ( CHECK_SQLCOLATTRIBUTESW( statement -> connection ))
            {
                /*
                 * map to the ODBC2 function
                 */

                field_identifier = map_ca_odbc3_to_2( field_identifier );

                ret = SQLCOLATTRIBUTESW( statement -> connection,
                    statement -> driver_stmt,
                    column_number,
                    field_identifier,
                    character_attribute,
                    buffer_length,
                    string_length,
                    numeric_attribute );
            }
            else
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: IM001" );

                __post_internal_error( &statement -> error,
                        ERROR_IM001, NULL,
                        statement -> connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
            }
        }
        else
        {
            ret = SQLCOLATTRIBUTEW( statement -> connection,
                    statement -> driver_stmt,
                    column_number,
                    field_identifier,
                    character_attribute,
                    buffer_length,
                    string_length,
                    numeric_attribute );
        }
    }
    else
    {
        if ( !CHECK_SQLCOLATTRIBUTE( statement -> connection ))
        {
            if ( CHECK_SQLCOLATTRIBUTES( statement -> connection ))
            {
                SQLCHAR *as1 = NULL;

                /*
                 * map to the ODBC2 function
                 */

                field_identifier = map_ca_odbc3_to_2( field_identifier );

                switch( field_identifier )
                {
                  case SQL_COLUMN_QUALIFIER_NAME:
                  case SQL_COLUMN_NAME:
                  case SQL_COLUMN_LABEL:
                  case SQL_COLUMN_OWNER_NAME:
                  case SQL_COLUMN_TABLE_NAME:
                  case SQL_COLUMN_TYPE_NAME:
                  case SQL_DESC_BASE_COLUMN_NAME:
                  case SQL_DESC_BASE_TABLE_NAME:
                  case SQL_DESC_LITERAL_PREFIX:
                  case SQL_DESC_LITERAL_SUFFIX:
                  case SQL_DESC_LOCAL_TYPE_NAME:
                  case SQL_DESC_NAME:
                    buffer_length = buffer_length / 2;
                    if ( buffer_length > 0 )
                    {
                        as1 = malloc( buffer_length + 1 );
                    }
                    break;
                }

                ret = SQLCOLATTRIBUTES( statement -> connection,
                    statement -> driver_stmt,
                    column_number,
                    field_identifier,
                    as1 ? as1 : character_attribute,
                    buffer_length,
                    string_length,
                    numeric_attribute );

                switch( field_identifier )
                {
                  case SQL_COLUMN_QUALIFIER_NAME:
                  case SQL_COLUMN_NAME:
                  case SQL_COLUMN_LABEL:
                  case SQL_COLUMN_OWNER_NAME:
                  case SQL_COLUMN_TABLE_NAME:
                  case SQL_COLUMN_TYPE_NAME:
                  case SQL_DESC_BASE_COLUMN_NAME:
                  case SQL_DESC_BASE_TABLE_NAME:
                  case SQL_DESC_LITERAL_PREFIX:
                  case SQL_DESC_LITERAL_SUFFIX:
                  case SQL_DESC_LOCAL_TYPE_NAME:
                  case SQL_DESC_NAME:
                    if ( SQL_SUCCEEDED( ret ) && character_attribute && as1 )
                    {
                        ansi_to_unicode_copy( character_attribute, (char*) as1, SQL_NTS, statement -> connection, NULL );
					}
					if ( SQL_SUCCEEDED( ret ) && string_length )
					{
						*string_length *= sizeof( SQLWCHAR );
					}
                    if ( as1 ) 
                    {
                        free( as1 );
                    }
                    break;
                }
            }
            else
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: IM001" );

                __post_internal_error( &statement -> error,
                        ERROR_IM001, NULL,
                        statement -> connection -> environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
            }
        }
        else
        {
            SQLCHAR *as1 = NULL;

            switch( field_identifier )
            {
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
              case SQL_COLUMN_NAME:
                buffer_length = buffer_length / 2;
                if ( buffer_length > 0 )
                {
                    as1 = malloc( buffer_length + 1 );
                }
                break;
            }

            ret = SQLCOLATTRIBUTE( statement -> connection,
                    statement -> driver_stmt,
                    column_number,
                    field_identifier,
                    as1 ? as1 : character_attribute,
                    buffer_length,
                    string_length,
                    numeric_attribute );

            switch( field_identifier )
            {
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
              case SQL_COLUMN_NAME:
                if ( SQL_SUCCEEDED( ret ) && character_attribute && as1 )
                {
                    ansi_to_unicode_copy( character_attribute, (char*) as1, SQL_NTS, statement -> connection, NULL );
				}
				if ( SQL_SUCCEEDED( ret ) && string_length )
				{
					*string_length *= sizeof( SQLWCHAR );
                }

                if ( as1 ) 
                {
                    free( as1 );
                }
                break;

              default:
                break;
            }
        }
    }

    if ( ret == SQL_STILL_EXECUTING )
    {
        statement -> interupted_func = SQL_API_SQLCOLATTRIBUTE;
        if ( statement -> state != STATE_S11 &&
                statement -> state != STATE_S12 )
            statement -> state = STATE_S11;
    }
    else if ( SQL_SUCCEEDED( ret ))
    {
        /*
         * map ODBC 3 datetime fields to ODBC2
         */

        if ( field_identifier == SQL_COLUMN_TYPE &&
                 numeric_attribute )
        {
	      *(SQLINTEGER*)numeric_attribute=
	          __map_type(MAP_SQL_D2DM, statement->connection,
	        *(SQLINTEGER*)numeric_attribute);
        }
    }

    if ( log_info.log_flag )
    {
        sprintf( statement -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, s1 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                statement -> msg );
    }

    return function_return( SQL_HANDLE_STMT, statement, ret, DEFER_R3 );
}
