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
 * $Id: SQLColAttributes.c,v 1.15 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLColAttributes.c,v $
 * Revision 1.15  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.14  2008/09/29 14:02:43  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.13  2006/03/08 09:18:41  lurcher
 * fix silly typo that was using sizeof( SQL_WCHAR ) instead of SQLWCHAR
 *
 * Revision 1.12  2006/01/06 18:44:35  lurcher
 * Couple of unicode fixes
 *
 * Revision 1.11  2004/11/22 17:02:48  lurcher
 * Fix unicode/ansi conversion in the SQLGet functions
 *
 * Revision 1.10  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.9  2003/08/15 17:34:43  lurcher
 *
 * Remove some unneeded ODBC2->3 attribute conversions
 *
 * Revision 1.8  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.7  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.6  2002/08/19 09:11:49  lurcher
 *
 * Fix Maxor ineffiecny in Postgres Drivers, and fix a return state
 *
 * Revision 1.5  2002/07/24 08:49:51  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.4  2002/04/25 15:16:46  lurcher
 *
 * Fix bug with SQLCOlAttribute(s)(W) where a column of zero could not be
 * used to get the count value
 *
 * Revision 1.3  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.2  2001/11/16 11:39:17  lurcher
 *
 * Add mapping between ODBC 2 and ODBC 3 types for SQLColAttribute(s)(W)
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.4  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.3  2001/04/12 17:43:35  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.11  2000/07/31 20:48:01  ngorham
 *
 * Fix bugs in SQLGetDiagField and with SQLColAttributes
 *
 * Revision 1.10  2000/07/28 16:34:52  ngorham
 *
 * Fix problems with SQLColAttributes, and SQLDescribeParam
 *
 * Revision 1.9  2000/06/20 13:30:09  ngorham
 *
 * Fix problems when using bookmarks
 *
 * Revision 1.8  1999/11/13 23:40:58  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.7  1999/11/10 03:51:33  ngorham
 *
 * Update the error reporting in the DM to enable ODBC 3 and 2 calls to
 * work at the same time
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
 * Revision 1.4  1999/07/10 21:10:15  ngorham
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
 * Revision 1.2  1999/06/03 22:20:25  ngorham
 *
 * Finished off the ODBC3-2 mapping
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLColAttributes.c,v $ $Revision: 1.15 $";

SQLINTEGER  map_ca_odbc2_to_3( SQLINTEGER field_identifier )
{
    switch( field_identifier )
    {
      case SQL_COLUMN_COUNT:
        field_identifier = SQL_DESC_COUNT; 
        break;

        /*
      case SQL_COLUMN_TYPE:
        field_identifier = SQL_DESC_TYPE; 
        break;

      case SQL_COLUMN_LENGTH:
        field_identifier = SQL_DESC_LENGTH; 
        break;

      case SQL_COLUMN_PRECISION:
        field_identifier = SQL_DESC_PRECISION; 
        break;

      case SQL_COLUMN_SCALE:
        field_identifier = SQL_DESC_SCALE; 
        break;
        */

      case SQL_COLUMN_NULLABLE:
        field_identifier = SQL_DESC_NULLABLE; 
        break;

      case SQL_COLUMN_NAME:
        field_identifier = SQL_DESC_NAME; 
        break;

      default:
        break;
    }

    return field_identifier;
}

SQLRETURN SQLColAttributesA( SQLHSTMT statement_handle,
           SQLUSMALLINT column_number,
           SQLUSMALLINT field_identifier,
           SQLPOINTER   character_attribute,
           SQLSMALLINT  buffer_length,
           SQLSMALLINT  *string_length,
           SQLLEN       *numeric_attribute )
{
    return SQLColAttributes( statement_handle,
                            column_number,
                            field_identifier,
                            character_attribute,
                            buffer_length,
                            string_length,
                            numeric_attribute );
}

SQLRETURN SQLColAttributes( SQLHSTMT statement_handle,
           SQLUSMALLINT column_number,
           SQLUSMALLINT field_identifier,
           SQLPOINTER   character_attribute,
           SQLSMALLINT  buffer_length,
           SQLSMALLINT  *string_length,
           SQLLEN       *numeric_attribute )
{
    DMHSTMT statement = (DMHSTMT) statement_handle;
    SQLRETURN ret;
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
            field_identifier != SQL_COLUMN_COUNT )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 07009" );

        __post_internal_error_api( &statement -> error,
                ERROR_07009, NULL,
                statement -> connection -> environment -> requested_version,
                
                SQL_API_SQLCOLATTRIBUTES );

        return function_return_nodrv( SQL_HANDLE_STMT, statement, SQL_ERROR );
    }

	/*
	 * Commented out for now because most drivers can not calc num cols
	 * before Execute (they have no parse). - PAH
	 *
	
    if ( field_identifier != SQL_DESC_COUNT &&
            statement -> numcols < column_number )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 07009" );

        __post_internal_error( &statement -> error,
                ERROR_07009, NULL,
                statement -> connection -> environment -> requested_version );

        return function_return( SQL_HANDLE_STMT, statement, SQL_ERROR );
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
        if ( statement -> interupted_func != SQL_API_SQLCOLATTRIBUTES )
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
        case SQL_COLUMN_AUTO_INCREMENT:
        case SQL_COLUMN_CASE_SENSITIVE:
        case SQL_COLUMN_COUNT:
        case SQL_COLUMN_DISPLAY_SIZE:
        case SQL_COLUMN_LENGTH:
        case SQL_COLUMN_MONEY:
        case SQL_COLUMN_NULLABLE:
        case SQL_COLUMN_PRECISION:
        case SQL_COLUMN_SCALE:
        case SQL_COLUMN_SEARCHABLE:
        case SQL_COLUMN_TYPE:
        case SQL_COLUMN_UNSIGNED:
        case SQL_COLUMN_UPDATABLE:
            isStringAttr = 0;
            break;
        case SQL_COLUMN_LABEL:
        case SQL_COLUMN_NAME:
        case SQL_COLUMN_OWNER_NAME:
        case SQL_COLUMN_QUALIFIER_NAME:
        case SQL_COLUMN_TABLE_NAME:
        case SQL_COLUMN_TYPE_NAME:
            if ( buffer_length < 0 )
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
        if ( !CHECK_SQLCOLATTRIBUTESW( statement -> connection ))
        {
            if ( CHECK_SQLCOLATTRIBUTEW( statement -> connection ))
            {
                SQLWCHAR *s1 = NULL;
                SQLSMALLINT unibuf_len;
                /*
                 * map to the ODBC3 function
                 */

                field_identifier = map_ca_odbc2_to_3( field_identifier );

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

                if ( SQL_SUCCEEDED( ret ) && isStringAttr && character_attribute && buffer_length > 0 && s1 )
                {
                    if ( string_length && ret == SQL_SUCCESS )
                    {
                        *string_length /= sizeof ( SQLWCHAR );
                    }
                    unicode_to_ansi_copy( character_attribute, buffer_length, s1, SQL_NTS, statement -> connection, NULL );
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

            if ( character_attribute && buffer_length > 0 )
            {
                s1 = calloc( sizeof( SQLWCHAR ) * ( buffer_length + 1 ), 1);
            }

            ret = SQLCOLATTRIBUTESW( statement -> connection,
                    statement -> driver_stmt,
                    column_number,
                    field_identifier,
                    s1 ? s1 : character_attribute,
                    buffer_length,
                    string_length,
                    numeric_attribute );

            if ( SQL_SUCCEEDED( ret ) && character_attribute )
            {
                unicode_to_ansi_copy( character_attribute, buffer_length, s1, SQL_NTS, statement -> connection, NULL );
			}
			if ( SQL_SUCCEEDED( ret ) && string_length && character_attribute ) 
			{
				*string_length /= sizeof( SQLWCHAR );	
			}

            if ( s1 )
            {
                free( s1 );
            }
        }
    }
    else
    {
        if ( !CHECK_SQLCOLATTRIBUTES( statement -> connection ))
        {
            if ( CHECK_SQLCOLATTRIBUTE( statement -> connection ))
            {
                /*
                 * map to the ODBC3 function
                 */

                field_identifier = map_ca_odbc2_to_3( field_identifier );

                ret = SQLCOLATTRIBUTE( statement -> connection,
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
            ret = SQLCOLATTRIBUTES( statement -> connection,
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
        statement -> interupted_func = SQL_API_SQLCOLATTRIBUTES;
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
                numeric_attribute &&
                statement -> connection -> driver_version == SQL_OV_ODBC2 )
        {
            SQLINTEGER na;

            memcpy( &na, numeric_attribute, sizeof( na ));

            switch( na )
            {
              case SQL_TYPE_TIME:
                na = SQL_TIME;
                memcpy( numeric_attribute, &na, sizeof( na ));
                break;

              case SQL_TYPE_DATE:
                na = SQL_DATE;
                memcpy( numeric_attribute, &na, sizeof( na ));
                break;

              case SQL_TYPE_TIMESTAMP:
                na = SQL_TIMESTAMP;
                memcpy( numeric_attribute, &na, sizeof( na ));
                break;
            }
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
