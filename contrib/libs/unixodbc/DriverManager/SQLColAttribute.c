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
 * $Id: SQLColAttribute.c,v 1.19 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLColAttribute.c,v $
 * Revision 1.19  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.18  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.17  2008/09/29 14:02:43  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.16  2007/04/02 10:50:17  lurcher
 * Fix some 64bit problems (only when sizeof(SQLLEN) == 8 )
 *
 * Revision 1.15  2006/03/08 09:18:41  lurcher
 * fix silly typo that was using sizeof( SQL_WCHAR ) instead of SQLWCHAR
 *
 * Revision 1.14  2004/11/22 17:02:48  lurcher
 * Fix unicode/ansi conversion in the SQLGet functions
 *
 * Revision 1.13  2004/10/30 20:19:21  peteralexharvey
 * ODBC spec says last arg for SQLColAttribute() is SQLPOINTER not (SQLEN*).
 * So switched back to SQLPOINTER.
 *
 * Revision 1.12  2004/10/29 10:00:35  lurcher
 * Fix SQLColAttribute protype
 *
 * Revision 1.11  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.10  2003/04/10 13:45:51  lurcher
 *
 * Alter the way that SQLDataSources returns the description field (again)
 *
 * Revision 1.9  2003/04/09 08:42:18  lurcher
 *
 * Allow setting of odbcinstQ lib from odbcinst.ini and Environment
 *
 * Revision 1.8  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.7  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.6  2002/11/11 17:10:06  lurcher
 *
 * VMS changes
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
 * Revision 1.6  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.5  2001/07/02 17:09:37  nick
 *
 * Add some portability changes
 *
 * Revision 1.4  2001/04/12 17:43:35  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.3  2001/04/03 16:34:12  nick
 *
 * Add support for strangly broken unicode drivers
 *
 * Revision 1.2  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.8  2000/06/20 13:30:07  ngorham
 *
 * Fix problems when using bookmarks
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
 * Revision 1.5  1999/10/09 00:56:16  ngorham
 *
 * Added Manush's patch to map ODBC 3-2 datetime values
 *
 * Revision 1.4  1999/09/21 22:34:24  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.3  1999/07/10 21:10:15  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.2  1999/07/04 21:05:07  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:05  sShandyb
 * first go at it
 *
 * Revision 1.2  1999/06/03 22:20:25  ngorham
 *
 * Finished off the ODBC3-2 mapping
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.4  1999/05/03 19:50:43  nick
 * Another check point
 *
 * Revision 1.3  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.2  1999/04/29 20:47:37  nick
 * Another checkpoint
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLColAttribute.c,v $ $Revision: 1.19 $";

SQLINTEGER map_ca_odbc3_to_2( SQLINTEGER field_identifier )
{
    switch( field_identifier )
    {
      case SQL_DESC_COUNT:
        field_identifier = SQL_COLUMN_COUNT; 
        break;

      case SQL_DESC_TYPE:
        field_identifier = SQL_COLUMN_TYPE; 
        break;

      case SQL_DESC_LENGTH:
        field_identifier = SQL_COLUMN_LENGTH; 
        break;

      case SQL_DESC_PRECISION:
        field_identifier = SQL_COLUMN_PRECISION; 
        break;

      case SQL_DESC_SCALE:
        field_identifier = SQL_COLUMN_SCALE; 
        break;

      case SQL_DESC_NULLABLE:
        field_identifier = SQL_COLUMN_NULLABLE; 
        break;

      case SQL_DESC_NAME:
        field_identifier = SQL_COLUMN_NAME; 
        break;

      default:
        break;
    }

    return field_identifier;
}

SQLRETURN SQLColAttributeA( SQLHSTMT statement_handle,
           SQLSMALLINT column_number,
           SQLSMALLINT field_identifier,
           SQLPOINTER character_attribute,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length,
           SQLLEN *numeric_attribute )
{
    return SQLColAttribute( statement_handle,
                            (SQLUSMALLINT) column_number,
                            (SQLUSMALLINT) field_identifier,
                            character_attribute,
                            buffer_length,
                            string_length,
                            numeric_attribute );
}

SQLRETURN SQLColAttribute ( SQLHSTMT statement_handle,
           SQLUSMALLINT column_number,
           SQLUSMALLINT field_identifier,
           SQLPOINTER character_attribute,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length,
           SQLLEN *numeric_attribute )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret = 0;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];
    int isStringAttr;

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
    /* MS Driver manager passes this to driver
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

    switch( field_identifier )
    {
        case SQL_DESC_AUTO_UNIQUE_VALUE:
        case SQL_DESC_CASE_SENSITIVE:
        case SQL_DESC_CONCISE_TYPE:
        case SQL_DESC_COUNT:
        case SQL_DESC_DISPLAY_SIZE:
        case SQL_DESC_FIXED_PREC_SCALE:
        case SQL_DESC_LENGTH:
        case SQL_DESC_NULLABLE:
        case SQL_DESC_NUM_PREC_RADIX:
        case SQL_DESC_OCTET_LENGTH:
        case SQL_DESC_PRECISION:
        case SQL_DESC_SCALE:
        case SQL_DESC_SEARCHABLE:
        case SQL_DESC_TYPE:
        case SQL_DESC_UNNAMED:
        case SQL_DESC_UNSIGNED:
        case SQL_DESC_UPDATABLE:
            isStringAttr = 0;
            break;
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
        default:
            isStringAttr = buffer_length >= 0;
            break;
    }

    if ( statement -> connection -> unicode_driver )
    {
        if ( !CHECK_SQLCOLATTRIBUTEW( statement -> connection ))
        {
            if ( CHECK_SQLCOLATTRIBUTESW( statement -> connection ))
            {
                SQLWCHAR *s1 = NULL;

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
                    if ( SQL_SUCCEEDED( ret ) && character_attribute && buffer_length > 0 )
                    {
                        s1 = calloc( sizeof( SQLWCHAR ) * ( buffer_length + 1 ), 1);
                    }
                    break;

                  default:
                    break;
                }

                ret = SQLCOLATTRIBUTESW( statement -> connection,
                    statement -> driver_stmt,
                    column_number,
                    field_identifier,
                    s1 ? s1 : character_attribute,
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
                    if ( SQL_SUCCEEDED( ret ) && character_attribute && s1 )
                    {
                        unicode_to_ansi_copy( character_attribute, buffer_length, s1,  SQL_NTS, statement -> connection, NULL );
                    }
					if ( SQL_SUCCEEDED( ret ) && string_length ) 
					{
						*string_length /= sizeof( SQLWCHAR );	
					}
                    break;

                  default:
                    break;
                }

                if ( s1 )
                {
                    free( s1 );
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
            SQLWCHAR *s1 = NULL;

            SQLSMALLINT unibuf_len;
            if ( isStringAttr && character_attribute && buffer_length > 0 )
            {
                s1 = calloc( sizeof( SQLWCHAR ) * ( buffer_length + 1 ), 1);
                /* Do not overflow, since SQLSMALLINT can only hold -32768 <= x <= 32767 */
                unibuf_len = buffer_length > 16383 ? buffer_length : sizeof( SQLWCHAR ) * buffer_length;
            }

            ret = SQLCOLATTRIBUTEW( statement -> connection,
                    statement -> driver_stmt,
                    column_number,
                    field_identifier,
                    s1 ? s1 : character_attribute,
                    s1 ? unibuf_len : buffer_length,
                    string_length,
                    numeric_attribute );

            if ( SQL_SUCCEEDED( ret ) && isStringAttr && buffer_length > 0 )
            {
                if ( character_attribute && s1 )
                {
                    unicode_to_ansi_copy( character_attribute, buffer_length, s1, SQL_NTS, statement -> connection, NULL );
                }
                /*
                    BUGBUG: Windows DM returns the number of bytes for the Unicode string
                    but only for certain ODBC-defined string fields, and when truncating
                */
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
                    if ( ret == SQL_SUCCESS && string_length )
                    {
                        *string_length /= sizeof( SQLWCHAR );	
                    }
                    break;
                default:
                    if ( string_length )
                    {
                        *string_length /= sizeof( SQLWCHAR );	
                    }
                }
            }

            if ( s1 )
            {
                free( s1 );
            }
        }
    }
    else
    {
        if ( !CHECK_SQLCOLATTRIBUTE( statement -> connection ))
        {
            /*
             * map ODBC 3 types to ODBC 2
             */

            if ( CHECK_SQLCOLATTRIBUTES( statement -> connection ))
            {
                /*
                 * map to the ODBC2 function
                 */

                field_identifier = map_ca_odbc3_to_2( field_identifier );

                ret = SQLCOLATTRIBUTES( statement -> connection,
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
            ret = SQLCOLATTRIBUTE( statement -> connection,
                statement -> driver_stmt,
                column_number,
                field_identifier,
                character_attribute,
                buffer_length,
                string_length,
                numeric_attribute );
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
