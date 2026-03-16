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
 * $Id: SQLGetEnvAttr.c,v 1.6 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLGetEnvAttr.c,v $
 * Revision 1.6  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.5  2008/09/29 14:02:45  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.4  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.3  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.2  2001/10/29 09:54:53  lurcher
 *
 * Add automake to libodbcinstQ
 *
 * Revision 1.1.1.1  2001/10/17 16:40:05  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.3  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.7  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.6  1999/10/24 23:54:18  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.5  1999/09/21 22:34:25  ngorham
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
 * Revision 1.2  1999/06/30 23:56:55  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:07  sShandyb
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

static char const rcsid[]= "$RCSfile: SQLGetEnvAttr.c,v $ $Revision: 1.6 $";

SQLRETURN SQLGetEnvAttr( SQLHENV environment_handle,
           SQLINTEGER attribute,
           SQLPOINTER value,
           SQLINTEGER buffer_length,
           SQLINTEGER *string_length )
{
    DMHENV environment = (DMHENV) environment_handle;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

    /*
     * check environment
     */

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
\n\t\t\tEnvironment = %p\
\n\t\t\tAttribute = %s\
\n\t\t\tValue = %p\
\n\t\t\tBuffer Len = %d\
\n\t\t\tStrLen = %p",
                environment,
                __env_attr_as_string( s1, attribute ),
                value, 
                (int)buffer_length,
                (void*)string_length );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                environment -> msg );
    }

    thread_protect( SQL_HANDLE_ENV, environment );

    switch ( attribute )
    {
      case SQL_ATTR_CONNECTION_POOLING:
        if ( value )
        {
            memcpy( value, &environment -> connection_pooling,
                    sizeof( environment -> connection_pooling ));
        }
        break;

      case SQL_ATTR_CP_MATCH:
        if ( value )
        {
            memcpy( value, &environment -> cp_match,
                    sizeof( environment -> cp_match ));
        }
        break;

      case SQL_ATTR_ODBC_VERSION:
        if ( !environment -> version_set )
        {
            __post_internal_error( &environment -> error,
                    ERROR_HY010, NULL,
                    SQL_OV_ODBC3 );

            return function_return( SQL_HANDLE_ENV, environment, SQL_ERROR, DEFER_R0 );
        }

        if ( value )
        {
            memcpy( value, &environment -> requested_version,
                    sizeof( environment -> requested_version ));
        }
        break;

      case SQL_ATTR_OUTPUT_NTS:
        if ( value )
        {
            SQLINTEGER i = SQL_TRUE;
            memcpy( value, &i, sizeof( i ));
        }
        break;

      /*
       * unixODBC additions
       */

      case SQL_ATTR_UNIXODBC_VERSION:
        if ( value )
        {
            if ( buffer_length >= strlen( VERSION )) 
            {
                strcpy( value, VERSION );
            }
            else
            {
                memcpy( value, VERSION, buffer_length );
                ((char*)value)[ buffer_length ] = '\0';
            }
            if ( string_length )
            {
                *string_length = strlen( VERSION );
            }
        }
        break;

      case SQL_ATTR_UNIXODBC_SYSPATH:
        if ( value )
        {
            char b1[ ODBC_FILENAME_MAX + 1 ];

            if ( buffer_length >= strlen( odbcinst_system_file_path( b1 ))) 
            {
                strcpy( value, odbcinst_system_file_path( b1 ));
            }
            else
            {
                memcpy( value, odbcinst_system_file_path( b1 ), buffer_length );
                ((char*)value)[ buffer_length ] = '\0';
            }
            if ( string_length )
            {
                *string_length = strlen( odbcinst_system_file_path( b1 ));
            }
        }
        break;

      default:
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY092" );

        __post_internal_error( &environment -> error,
                ERROR_HY092, NULL,
                environment -> requested_version );

        return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
    }

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

    return function_return( SQL_HANDLE_ENV, environment, SQL_SUCCESS, DEFER_R0 );
}
