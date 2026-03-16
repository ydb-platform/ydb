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
 * $Id: SQLSetEnvAttr.c,v 1.9 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLSetEnvAttr.c,v $
 * Revision 1.9  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.8  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.7  2004/06/21 10:01:11  lurcher
 *
 * Fix a couple of 64 bit issues
 *
 * Revision 1.6  2003/10/30 18:20:46  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.5  2003/03/05 09:48:45  lurcher
 *
 * Add some 64 bit fixes
 *
 * Revision 1.4  2003/01/23 15:33:25  lurcher
 *
 * Fix problems with using putenv()
 *
 * Revision 1.3  2002/12/05 17:44:31  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.2  2002/02/21 18:44:09  lurcher
 *
 * Fix bug on 32 bit platforms without long long support
 * Add option to set environment variables from the ini file
 *
 * Revision 1.1.1.1  2001/10/17 16:40:07  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.5  2001/09/27 17:05:48  nick
 *
 * Assorted fixes and tweeks
 *
 * Revision 1.4  2001/07/03 09:30:41  nick
 *
 * Add ability to alter size of displayed message in the log
 *
 * Revision 1.3  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.2  2000/12/14 18:10:19  nick
 *
 * Add connection pooling
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.7  1999/11/13 23:41:01  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.6  1999/10/24 23:54:19  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.5  1999/09/21 22:34:25  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.4  1999/07/10 21:10:17  ngorham
 *
 * Adjust error sqlstate from driver manager, depending on requested
 * version (ODBC2/3)
 *
 * Revision 1.3  1999/07/04 21:05:08  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/30 23:56:55  ngorham
 *
 * Add initial thread safety code
 *
 * Revision 1.1.1.1  1999/05/29 13:41:08  sShandyb
 * first go at it
 *
 * Revision 1.1.1.1  1999/05/27 18:23:18  pharvey
 * Imported sources
 *
 * Revision 1.3  1999/05/09 23:27:11  nick
 * All the API done now
 *
 * Revision 1.2  1999/04/30 16:22:47  nick
 * Another checkpoint
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLSetEnvAttr.c,v $ $Revision: 1.9 $";

extern int pooling_enabled;

SQLRETURN SQLSetEnvAttr( SQLHENV environment_handle,
           SQLINTEGER attribute,
           SQLPOINTER value,
           SQLINTEGER string_length )
{
    DMHENV environment = (DMHENV) environment_handle;
    SQLCHAR s1[ 100 + LOG_MESSAGE_LEN ];

    if ( !environment_handle && 
            ( attribute == SQL_ATTR_CONNECTION_POOLING || 
              attribute == SQL_ATTR_CP_MATCH ))
    {
        if ( attribute == SQL_ATTR_CONNECTION_POOLING ) {
            if ((SQLLEN) value == SQL_CP_ONE_PER_DRIVER || (SQLLEN) value == SQL_CP_ONE_PER_HENV ) {
                pooling_enabled = 1;
            }
        }
        return SQL_SUCCESS;
    }

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
\n\t\t\tStrLen = %d",
                environment,
                __env_attr_as_string( s1, attribute ),
                value, 
                (int)string_length );

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
        {
#ifdef HAVE_PTRDIFF_T
            SQLUINTEGER ptr = (ptrdiff_t) value;
#else
            SQLUINTEGER ptr = (SQLUINTEGER) value;
#endif

            if ( ptr != SQL_CP_OFF &&
                ptr != SQL_CP_ONE_PER_DRIVER &&
                ptr != SQL_CP_ONE_PER_HENV )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY024" );

                __post_internal_error( &environment -> error,
                        ERROR_HY024, NULL,
                        environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
            }

            environment -> connection_pooling = ptr;
        }
        break;

      case SQL_ATTR_CP_MATCH:
        {
#ifdef HAVE_PTRDIFF_T
            SQLUINTEGER ptr = (ptrdiff_t) value;
#else
            SQLUINTEGER ptr = (SQLUINTEGER) value;
#endif

            if ( ptr != SQL_CP_STRICT_MATCH &&
                ptr != SQL_CP_RELAXED_MATCH )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY024" );

                __post_internal_error( &environment -> error,
                        ERROR_HY024, NULL,
                        environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
            }

            environment -> cp_match = ptr;
        }
        break;

      case SQL_ATTR_ODBC_VERSION:
        {
#ifdef HAVE_PTRDIFF_T
            SQLUINTEGER ptr = (ptrdiff_t) value;
#else
            SQLUINTEGER ptr = (SQLUINTEGER) value;
#endif

            if ( ptr != SQL_OV_ODBC2 &&
                    ptr != SQL_OV_ODBC3 &&
                    ptr != SQL_OV_ODBC3_80 )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HY024" );

                __post_internal_error( &environment -> error,
                        ERROR_HY024, NULL,
                        environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
            }
            else
            {
                if ( environment -> connection_count > 0 )
                {
                    dm_log_write( __FILE__, 
                            __LINE__, 
                            LOG_INFO, 
                            LOG_INFO, 
                            "Error: S1010" );

                    __post_internal_error( &environment -> error,
                            ERROR_S1010, NULL,
                            environment -> requested_version );

                    return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
                }

                environment -> requested_version = ptr;
                environment -> version_set = 1;
            }
        }
        break;

      case SQL_ATTR_OUTPUT_NTS:
        {
#ifdef HAVE_PTRDIFF_T
            SQLUINTEGER ptr = (ptrdiff_t) value;
#else
            SQLUINTEGER ptr = (SQLUINTEGER) value;
#endif

            /*
             * this must be one of the most brain dead atribute,
             * it can be set, but only to TRUE, any other value
             * (ie FALSE) returns a error. It's almost as if it's not
             * settable :-)
             */

            if ( ptr == SQL_FALSE )
            {
                dm_log_write( __FILE__, 
                        __LINE__, 
                        LOG_INFO, 
                        LOG_INFO, 
                        "Error: HYC00" );

                __post_internal_error( &environment -> error,
                        ERROR_HYC00, NULL,
                        environment -> requested_version );

                return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
            }
        }
        break;

      /*
       * unixODBC additions
       */

      case SQL_ATTR_UNIXODBC_ENVATTR:
        if ( value )
        {
            char *str = (char*) value;

            /*
             * its a memory leak, but not much I can do, see "man putenv"
             */
            putenv( strdup( str ));

            return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
        }
        break;

        /*
         * Third party extensions
         */

      case 1064:        /* SQL_ATTR_APP_UNICODE_TYPE */
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

    return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_SUCCESS );
}
