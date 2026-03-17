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
 * $Id: SQLColAttribute.c,v 1.5 2009/02/18 17:59:17 lurcher Exp $
 *
 * $Log: SQLColAttribute.c,v $
 * Revision 1.5  2009/02/18 17:59:17  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.4  2007/11/13 15:04:57  lurcher
 * Fix 64 bit cursor lib issues
 *
 * Revision 1.3  2004/06/21 10:01:14  lurcher
 *
 * Fix a couple of 64 bit issues
 *
 * Revision 1.2  2003/12/01 16:37:17  lurcher
 *
 * Fix a bug in SQLWritePrivateProfileString
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

SQLRETURN CLColAttribute ( SQLHSTMT statement_handle,
           SQLUSMALLINT column_number,
           SQLUSMALLINT field_identifier,
           SQLPOINTER character_attribute,
           SQLSMALLINT buffer_length,
           SQLSMALLINT *string_length,
           SQLLEN 	*numeric_attribute )
{
    CLHSTMT cl_statement = (CLHSTMT) statement_handle; 

    /*
     * Catch any requests for bookmark info
     */

    if ( field_identifier != SQL_DESC_COUNT &&
                            field_identifier != SQL_COLUMN_COUNT )
    {
        if ( column_number == 0 )
        {
            if ( cl_statement -> use_bookmarks )
            {
                SQLLEN  ival;

                switch( field_identifier )
                {
                  case SQL_DESC_AUTO_UNIQUE_VALUE:
                  case SQL_DESC_CASE_SENSITIVE:
                  case SQL_DESC_NULLABLE:
                  case SQL_DESC_UPDATABLE:
                  case SQL_COLUMN_NULLABLE:
                  case SQL_DESC_UNSIGNED:
                    ival = SQL_FALSE;
                    break;
        
                  case SQL_DESC_CONCISE_TYPE:
                    ival = SQL_C_SLONG;
                    break;

                  case SQL_DESC_DISPLAY_SIZE:
                    ival = 4;
                    break;

                  case SQL_DESC_FIXED_PREC_SCALE:
                  case SQL_DESC_SEARCHABLE:
                    ival = SQL_TRUE;
                    break;

                  case SQL_DESC_NUM_PREC_RADIX:
                    ival = 0;
                    break;

                  case SQL_DESC_LENGTH:
                  case SQL_COLUMN_LENGTH:
                  case SQL_DESC_OCTET_LENGTH:
                    ival = 4;
                    break;

                  case SQL_DESC_PRECISION:
                  case SQL_DESC_SCALE:
                  case SQL_COLUMN_PRECISION:
                  case SQL_COLUMN_SCALE:
                    ival = 0;
                    break;

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
                    if ( string_length )
                    {
                        *string_length = 0;
                    }
                    if ( character_attribute )
                    {
                        *((SQLCHAR*)character_attribute) = '\0';
                    }
                    return SQL_SUCCESS;

                  default:
                    return SQLCOLATTRIBUTE( cl_statement -> cl_connection,
                               cl_statement -> driver_stmt,
                               column_number,
                               field_identifier,
                               character_attribute,
                               buffer_length,
                               string_length,
                               numeric_attribute );
                }

                if ( numeric_attribute )
                {
                    *((SQLLEN*)numeric_attribute) = ival;
                }

                return SQL_SUCCESS;
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
    }

    return SQLCOLATTRIBUTE( cl_statement -> cl_connection,
           cl_statement -> driver_stmt,
           column_number,
           field_identifier,
           character_attribute,
           buffer_length,
           string_length,
           numeric_attribute );
}
