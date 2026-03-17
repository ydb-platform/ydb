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
 * $Id: SQLDataSources.c,v 1.9 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLDataSources.c,v $
 * Revision 1.9  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.8  2008/06/30 08:40:48  lurcher
 * Few more tweeks towards a release
 *
 * Revision 1.7  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.6  2003/04/10 13:45:51  lurcher
 *
 * Alter the way that SQLDataSources returns the description field (again)
 *
 * Revision 1.5  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.4  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.3  2002/07/08 11:40:35  lurcher
 *
 * Merge two config tests
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
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.10  2000/08/10 15:12:17  ngorham
 *
 * Fix incorrect return from SQLDataSources
 *
 * Revision 1.9  2000/05/04 15:08:29  ngorham
 *
 * Update SQLDataSource.c
 *
 * Revision 1.8  2000/05/04 12:57:03  ngorham
 *
 * Fix problem in SQLDataSource, the description is from the Driver not the
 * DSN
 *
 * Revision 1.7  1999/11/13 23:40:59  ngorham
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

static char const rcsid[]= "$RCSfile: SQLDataSources.c,v $ $Revision: 1.9 $";

#define BUFFERSIZE      1024*4

SQLRETURN SQLDataSourcesA( SQLHENV environment_handle,
           SQLUSMALLINT direction,
           SQLCHAR *server_name,
           SQLSMALLINT buffer_length1,
           SQLSMALLINT *name_length1,
           SQLCHAR *description,
           SQLSMALLINT buffer_length2,
           SQLSMALLINT *name_length2 )
{
    return SQLDataSources( environment_handle,
                        direction,
                        server_name,
                        buffer_length1,
                        name_length1,
                        description,
                        buffer_length2,
                        name_length2 );
}

SQLRETURN SQLDataSources( SQLHENV environment_handle,
           SQLUSMALLINT direction,
           SQLCHAR *server_name,
           SQLSMALLINT buffer_length1,
           SQLSMALLINT *name_length1,
           SQLCHAR *description,
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
#ifdef HAVE_SNPRINTF
            snprintf( environment -> msg, sizeof(  environment -> msg ),
#else
            sprintf( environment -> msg, 
#endif
                "\n\t\tEntry:\
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
            "ODBC.INI" );

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
                driver, sizeof( driver ), "ODBC.INI" );

        if ( strlen( driver ) > 0 )
        {
            /*
             * Make this return the description from the driver setup, this is
             * the way its done in Windows

            SQLGetPrivateProfileString( driver, "Description", driver,
                property, sizeof( property ), "ODBCINST.INI" );
             */

            /*
             * even though the string is called description, it should 
             * actually be the driver name entry from odbcinst.ini on windows
             * there is no separate Description line
             */
             strcpy( property, driver );
        }
        else
        {
            /*
             * May as well try and get something
             */

            SQLGetPrivateProfileString( object, "Description", "",
                property, sizeof( property ), "ODBC.INI" );
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
            if ( buffer_length1 <= strlen( object ))
            {
                memcpy( server_name, object, buffer_length1 );
                server_name[ buffer_length1 - 1 ] = '\0';
            }
            else
            {
                strcpy((char*) server_name, object );
            }
        }

        if ( description )
        {
            if ( buffer_length2 <= strlen( property ))
            {
                memcpy( description, property, buffer_length2 );
                description[ buffer_length2 - 1 ] = '\0';
            }
            else
            {
                strcpy((char*) description, property );
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
#ifdef HAVE_SNPRINTF
        snprintf( environment -> msg, sizeof(  environment -> msg ),
#else
        sprintf( environment -> msg, 
#endif
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
