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
 * $Id: SQLDataSourcesW.c,v 1.6 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLDataSourcesW.c,v $
 * Revision 1.6  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
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
 * Revision 1.3  2002/07/24 08:49:51  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.2  2001/12/13 13:00:32  lurcher
 *
 * Remove most if not all warnings on 64 bit platforms
 * Add support for new MS 3.52 64 bit changes
 * Add override to disable the stopping of tracing
 * Add MAX_ROWS support in postgres driver
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2001/01/04 13:16:25  nick
 *
 * Add support for GNU portable threads and tidy up some UNICODE compile
 * warnings
 *
 * Revision 1.1  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLDataSourcesW.c,v $";

#define BUFFERSIZE      1024 * 4

SQLRETURN SQLDataSourcesW( SQLHENV environment_handle,
           SQLUSMALLINT direction,
           SQLWCHAR *server_name,
           SQLSMALLINT buffer_length1,
           SQLSMALLINT *name_length1,
           SQLWCHAR *description,
           SQLSMALLINT buffer_length2,
           SQLSMALLINT *name_length2 )
{
    DMHENV environment = (DMHENV) environment_handle;
    SQLRETURN ret;
    char buffer[ BUFFERSIZE + 1 ];
    char object[ INI_MAX_OBJECT_NAME + 1 ];
    char property[ INI_MAX_PROPERTY_VALUE + 1 ];
    char driver[ INI_MAX_PROPERTY_VALUE + 1 ];
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

    if ( !__validate_env( environment ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

        return SQL_INVALID_HANDLE;
    }

    function_entry( environment );

    if ( log_info.log_flag )
    {
        sprintf( environment -> msg, "\n\t\tEntry:\
\n\t\t\tEnvironment = %p",
                environment );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                environment -> msg );
    }

    thread_protect( SQL_HANDLE_ENV, environment );

    /*
     * check that a version has been requested
     */

    if ( environment -> requested_version == 0 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &environment -> error,
                ERROR_HY010, NULL,
                environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
    }

    if ( buffer_length1 < 0 || buffer_length2 < 0 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY090" );

        __post_internal_error( &environment -> error,
                ERROR_HY090, NULL,
                environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
    }

    if ( direction != SQL_FETCH_FIRST &&
            direction != SQL_FETCH_FIRST_USER &&
            direction != SQL_FETCH_FIRST_SYSTEM &&
            direction != SQL_FETCH_NEXT )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY103" );

        __post_internal_error( &environment -> error,
                ERROR_HY103, NULL,
                environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
    }

    /*
     * for this function USER = "~/.odbc.ini" and
     * SYSTEM = "/usr/odbc.ini
     */

    if ( direction == SQL_FETCH_FIRST )
    {
        environment -> fetch_mode = ODBC_BOTH_DSN;
        environment -> entry = 0;
    }
    else if ( direction == SQL_FETCH_FIRST_USER )
    {
        environment -> fetch_mode = ODBC_USER_DSN;
        environment -> entry = 0;
    }
    else if ( direction == SQL_FETCH_FIRST_SYSTEM )
    {
        environment -> fetch_mode = ODBC_SYSTEM_DSN;
        environment -> entry = 0;
    }

    /*
     * this is lifted from Peters code
     */

    memset( buffer, 0, sizeof( buffer ));
    memset( object, 0, sizeof( object ));
    SQLSetConfigMode( environment -> fetch_mode );

    SQLGetPrivateProfileString( NULL, NULL, NULL,
            buffer, sizeof( buffer ),
            "odbc.ini" );

    if ( iniElement( buffer, '\0', '\0',
                environment -> entry,
                object, sizeof( object )) != INI_SUCCESS )
    {
        environment -> entry = 0;
        ret = SQL_NO_DATA;
    }
    else
    {
        memset( buffer, 0, sizeof( buffer ));
        memset( property, 0, sizeof( property ));
        memset( driver, 0, sizeof( driver ));

        SQLGetPrivateProfileString( object, "Driver", "",
                driver, sizeof( driver ), "odbc.ini" );

        if ( strlen( driver ) > 0 )
        {
            /*
            SQLGetPrivateProfileString( driver, "Description", "",
                property, sizeof( property ), "odbcinst.ini" );
                */
            strcpy( property, driver );
        }
        else
        {
            strcpy( property, "" );
        }

        environment -> entry++;

        if (( server_name &&  buffer_length1 <= strlen( object )) ||
            ( description && buffer_length2 <= strlen( property )))
        {
            __post_internal_error( &environment -> error,
                    ERROR_01004, NULL,
                    environment -> requested_version );
            ret = SQL_SUCCESS_WITH_INFO;
        }
        else
        {
            ret = SQL_SUCCESS;
        }

        if ( server_name )
        {
            SQLWCHAR *s1;

            s1 = ansi_to_unicode_alloc((SQLCHAR*) object, SQL_NTS, NULL, NULL );

            if ( s1 )
            {
                if ( buffer_length1 <= strlen( object ))
                {
                    memcpy( server_name, s1, buffer_length1 * 2 );
                    server_name[ buffer_length1 - 1 ] = 0;
                }
                else
                {
                    wide_strcpy( server_name, s1 );
                }

                free( s1 );
            }
        }

        if ( description )
        {
            SQLWCHAR *s1;

            s1 = ansi_to_unicode_alloc((SQLCHAR*) property, SQL_NTS, NULL, NULL );

            if ( s1 )
            {
                if ( buffer_length2 <= strlen( property ))
                {
                    memcpy( description, s1, buffer_length2 * 2 );
                    description[ buffer_length2 - 1 ] = 0;
                }
                else
                {
                    wide_strcpy( description, s1 );
                }

                free( s1 );
            }
        }

        if ( name_length1 )
        {
            *name_length1 = strlen( object );
        }
        if ( name_length2 )
        {
            *name_length2 = strlen( property );
        }
    }

    /* NEVER FORGET TO RESET THIS TO ODBC_BOTH_DSN */
    SQLSetConfigMode( ODBC_BOTH_DSN );

    if ( log_info.log_flag )
    {
        sprintf( environment -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( SQL_SUCCESS, s1 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                environment -> msg );
    }

    return function_return_nodrv( SQL_HANDLE_ENV, environment, ret );
}
