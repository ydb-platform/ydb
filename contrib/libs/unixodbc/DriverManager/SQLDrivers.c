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
 * $Id: SQLDrivers.c,v 1.13 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLDrivers.c,v $
 * Revision 1.13  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.12  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.11  2008/09/29 14:02:45  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.10  2005/10/06 08:58:19  lurcher
 * Fix problem with SQLDrivers not returning first entry
 *
 * Revision 1.9  2005/07/17 09:11:23  lurcher
 * Fix bug in SQLDrivers that was stopping the return of the attribute length
 *
 * Revision 1.8  2004/07/25 00:42:02  peteralexharvey
 * for OS2 port
 *
 * Revision 1.7  2003/11/13 15:12:53  lurcher
 *
 * small change to ODBCConfig to have the password field in the driver
 * properties hide the password
 * Make both # and ; comments in ini files
 *
 * Revision 1.6  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.5  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.4  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.3  2002/05/21 14:19:44  lurcher
 *
 * * Update libtool to escape from AIX build problem
 * * Add fix to avoid file handle limitations
 * * Add more UNICODE changes, it looks like it is native 16 representation
 *   the old way can be reproduced by defining UCS16BE
 * * Add iusql, its just the same as isql but uses the wide functions
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
 * Revision 1.3  2001/05/15 10:57:44  nick
 *
 * Add initial support for VMS
 *
 * Revision 1.2  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.12  2000/08/16 13:06:21  ngorham
 *
 * Fix invalid return code
 *
 * Revision 1.11  2000/07/13 13:27:24  ngorham
 *
 * remove _ from odbcinst_system_file_path()
 *
 * Revision 1.10  2001/04/04 23:10:34  ngorham
 *
 * Fix a SQLDrivers problem
 *
 * Revision 1.9  2000/02/20 10:18:47  ngorham
 *
 * Add support for ODBCINI environment override for Applix.
 *
 * Revision 1.8  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.7  1999/10/24 23:54:17  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.6  1999/09/21 22:34:24  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.5  1999/09/19 22:24:33  ngorham
 *
 * Added support for the cursor library
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
 * Revision 1.1.1.1  1999/05/29 13:41:06  sShandyb
 * first go at it
 *
 * Revision 1.1.1.1  1999/05/27 18:23:17  pharvey
 * Imported sources
 *
 * Revision 1.4  1999/05/09 23:27:11  nick
 * All the API done now
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

static char const rcsid[]= "$RCSfile: SQLDrivers.c,v $ $Revision: 1.13 $";

#define BUFFERSIZE  1024

SQLRETURN SQLDriversA(
    SQLHENV            henv,
    SQLUSMALLINT       fdirection,
    SQLCHAR            *sz_driver_desc,
    SQLSMALLINT        cb_driver_desc_max,
    SQLSMALLINT        *pcb_driver_desc,
    SQLCHAR            *sz_driver_attributes,
    SQLSMALLINT        cb_drvr_attr_max,
    SQLSMALLINT        *pcb_drvr_attr )
{
    return SQLDrivers( henv,
                        fdirection,
                        sz_driver_desc,
                        cb_driver_desc_max,
                        pcb_driver_desc,
                        sz_driver_attributes,
                        cb_drvr_attr_max,
                        pcb_drvr_attr );
}

SQLRETURN SQLDrivers(
    SQLHENV            henv,
    SQLUSMALLINT       fdirection,
    SQLCHAR            *sz_driver_desc,
    SQLSMALLINT        cb_driver_desc_max,
    SQLSMALLINT        *pcb_driver_desc,
    SQLCHAR            *sz_driver_attributes,
    SQLSMALLINT        cb_drvr_attr_max,
    SQLSMALLINT        *pcb_drvr_attr )
{
    DMHENV  environment = (DMHENV) henv;
    char   	buffer[ BUFFERSIZE + 1 ];
    char    object[ INI_MAX_OBJECT_NAME + 1 ];
    SQLRETURN ret = SQL_SUCCESS;
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
\n\t\t\tEnvironment = %p\
\n\t\t\tDirection = %d",
                environment,
                (int)fdirection );

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

    if ( ! environment -> version_set )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY010" );

        __post_internal_error( &environment -> error,
                ERROR_HY010, NULL,
                SQL_OV_ODBC3 );

        return function_return_nodrv( SQL_HANDLE_ENV, environment, SQL_ERROR );
    }

    if ( cb_driver_desc_max < 0 )
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

    if ( cb_drvr_attr_max < 0
            || cb_drvr_attr_max == 1 )
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

    if ( fdirection != SQL_FETCH_FIRST &&
            fdirection != SQL_FETCH_NEXT )
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

    if ( fdirection == SQL_FETCH_FIRST )
        environment -> sql_driver_count = 0;
    else
        environment -> sql_driver_count ++;

try_again:

	memset( buffer, '\0', sizeof( buffer ));
	memset( object, '\0', sizeof( object ));
	SQLGetPrivateProfileString( NULL, NULL, 
            NULL, buffer, sizeof( buffer ), "ODBCINST.INI" );

	if ( iniElement( buffer, '\0', '\0',
                environment -> sql_driver_count,
                object, sizeof( object )) != INI_SUCCESS )
	{
		/*
		 * Set up for the next time
		 */
        environment -> sql_driver_count = -1; 
		ret = SQL_NO_DATA;
	}
	else
	{
        ret = SQL_SUCCESS;

        /*
         * this section is used for internal info
         */

        if ( strcmp( object, "ODBC" ) == 0 )
        {
            environment -> sql_driver_count ++;
            goto try_again;
        }

        if ( pcb_driver_desc )
            *pcb_driver_desc = strlen( object );

        if ( sz_driver_desc )
        {
            if ( strlen( object ) >= cb_driver_desc_max )
            {
                memcpy( sz_driver_desc, object, cb_driver_desc_max - 1 );
                sz_driver_desc[ cb_driver_desc_max - 1 ] = '\0';
                ret = SQL_SUCCESS_WITH_INFO;
            }
            else
            {
                strcpy((char*) sz_driver_desc, object );
            }
        }
        else
        {
            ret = SQL_SUCCESS;
        }

		if ( sz_driver_attributes ||
                pcb_drvr_attr )
		{
            HINI hIni;
            char szPropertyName[INI_MAX_PROPERTY_NAME+1];
            char szValue[INI_MAX_PROPERTY_NAME+1];
            char szIniName[ INI_MAX_OBJECT_NAME + 1 ];
            char buffer[ 2 * INI_MAX_PROPERTY_NAME + 3 ];
            int total_len = 0;
            char b1[ ODBC_FILENAME_MAX + 1 ], b2[ ODBC_FILENAME_MAX + 1 ];
            int found = 0;

            /*
             * enumerate the driver attributes, first in system odbcinst.ini and if not found in user odbcinst.ini
             */

            sprintf( szIniName, "%s/%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ));

			memset( buffer, '\0', sizeof( buffer ));
#ifdef __OS2__
            if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', FALSE, 1L ) == 
                    INI_SUCCESS )
#else
            if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', FALSE ) == 
                    INI_SUCCESS )
#endif
            {
                iniObjectSeek( hIni, (char *)object );
                iniPropertyFirst( hIni );
                while ( iniPropertyEOL( hIni ) != TRUE )
                {
                    iniProperty( hIni, szPropertyName );
                    iniValue( hIni, szValue );
                    sprintf( buffer, "%s=%s", szPropertyName, 
                            szValue );

                    found = 1;

                    if ( sz_driver_attributes ) {

                        if ( total_len + strlen( buffer ) + 1 > cb_drvr_attr_max )
                        {
                            ret = SQL_SUCCESS_WITH_INFO;
                        }
                        else
                        {
                            strcpy((char*) sz_driver_attributes, buffer );
                            sz_driver_attributes += strlen( buffer ) + 1;
                        }
                    }
                    total_len += strlen( buffer ) + 1;

                    iniPropertyNext( hIni );
                }
                /*
                 * add extra null 
                 */
                if ( sz_driver_attributes )
                    *sz_driver_attributes = '\0';

                if ( pcb_drvr_attr )
                {
                    *pcb_drvr_attr = total_len;
                }

                iniClose( hIni );
            }

            if ( !found ) 
            {

                sprintf( szIniName, "%s/%s", odbcinst_user_file_path( b1 ), odbcinst_user_file_name( b2 ));

                memset( buffer, '\0', sizeof( buffer ));
    #ifdef __OS2__
                if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', FALSE, 1L ) == 
                        INI_SUCCESS )
    #else
                if ( iniOpen( &hIni, szIniName, "#;", '[', ']', '=', FALSE ) == 
                        INI_SUCCESS )
    #endif
                {
                    iniObjectSeek( hIni, (char *)object );
                    iniPropertyFirst( hIni );
                    while ( iniPropertyEOL( hIni ) != TRUE )
                    {
                        iniProperty( hIni, szPropertyName );
                        iniValue( hIni, szValue );
                        sprintf( buffer, "%s=%s", szPropertyName, 
                                szValue );

                        if ( sz_driver_attributes ) {

                            if ( total_len + strlen( buffer ) + 1 > cb_drvr_attr_max )
                            {
                                ret = SQL_SUCCESS_WITH_INFO;
                            }
                            else
                            {
                                strcpy((char*) sz_driver_attributes, buffer );
                                sz_driver_attributes += strlen( buffer ) + 1;
                            }
                        }
                        total_len += strlen( buffer ) + 1;

                        iniPropertyNext( hIni );
                    }
                    /*
                     * add extra null 
                     */
                    if ( sz_driver_attributes )
                        *sz_driver_attributes = '\0';

                    if ( pcb_drvr_attr )
                    {
                        *pcb_drvr_attr = total_len;
                    }

                    iniClose( hIni );
                }
            }
		}
	}

    if ( ret == SQL_SUCCESS_WITH_INFO )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 01004" );

        __post_internal_error( &environment -> error,
                ERROR_01004, NULL,
                environment -> requested_version );
    }

    if ( log_info.log_flag )
    {
        sprintf( environment -> msg, 
                "\n\t\tExit:[%s]",
                    __get_return_status( ret, s1 ));

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                environment -> msg );
    }

    return function_return_nodrv( SQL_HANDLE_ENV, environment, ret );
}
