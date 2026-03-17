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
 * $Id: __connection.c,v 1.6 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: __connection.c,v $
 * Revision 1.6  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.5  2008/09/29 14:02:45  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.4  2004/09/08 16:38:54  lurcher
 *
 * Get ready for a 2.2.10 release
 *
 * Revision 1.3  2003/04/10 13:45:52  lurcher
 *
 * Alter the way that SQLDataSources returns the description field (again)
 *
 * Revision 1.2  2003/04/09 08:42:18  lurcher
 *
 * Allow setting of odbcinstQ lib from odbcinst.ini and Environment
 *
 * Revision 1.1.1.1  2001/10/17 16:40:07  lurcher
 *
 * First upload to SourceForge
 *
 * Revision 1.2  2001/05/15 10:57:44  nick
 *
 * Add initial support for VMS
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.7  1999/11/28 18:35:50  ngorham
 *
 * Add extra ODBC3/2 Date/Time mapping
 *
 * Revision 1.6  1999/11/13 23:41:01  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.5  1999/10/09 00:56:16  ngorham
 *
 * Added Manush's patch to map ODBC 3-2 datetime values
 *
 * Revision 1.4  1999/08/03 21:47:39  shandyb
 * Moving to automake: changed files in DriverManager
 *
 * Revision 1.3  1999/07/04 21:05:08  ngorham
 *
 * Add LGPL Headers to code
 *
 * Revision 1.2  1999/06/19 17:51:41  ngorham
 *
 * Applied assorted minor bug fixes
 *
 * Revision 1.1.1.1  1999/05/29 13:41:09  sShandyb
 * first go at it
 *
 * Revision 1.1.1.1  1999/05/27 18:23:18  pharvey
 * Imported sources
 *
 * Revision 1.1  1999/04/25 23:06:11  nick
 * Initial revision
 *
 *
 **********************************************************************/

#include <config.h>
#include "drivermanager.h"

/*
 * list of places to look, a $ at the start indicates
 * then it following text should be looked in as a env
 * variable
 */

static char const rcsid[]= "$RCSfile: __connection.c,v $ $Revision: 1.6 $";

/*
 * search for the library (.so) that the DSN points to
 */

char *__find_lib_name( char *dsn, char *lib_name, char *driver_name )
{
    char driver[ INI_MAX_PROPERTY_VALUE + 1 ];
    char driver_lib[ INI_MAX_PROPERTY_VALUE + 1 ];
    int mode;

    /*
     * this cound mess up threaded programs by changing the mode
     */

    __lock_config_mode();

    mode = __get_config_mode();

    __set_config_mode( ODBC_USER_DSN );

	/*
	 * GET DRIVER FROM ODBC.INI
	 */

    __SQLGetPrivateProfileStringNL( dsn, "Driver", "",
            driver_lib, sizeof( driver_lib ), "ODBC.INI" );

    if ( driver_lib[ 0 ] == 0 )
    {
        /*
         * if not found look in system DSN
         */

        __set_config_mode( ODBC_SYSTEM_DSN );

        __SQLGetPrivateProfileStringNL( dsn, "Driver", "",
                driver_lib, sizeof( driver_lib ), "ODBC.INI" );
        
        if ( driver_lib[ 0 ] == 0 ) {
            __set_config_mode( mode );
            __unlock_config_mode();
            return NULL;
        }

        __set_config_mode( ODBC_BOTH_DSN );
    }

	/*
	 * GET DRIVER FROM ODBCINST.INI IF ODBC.INI HAD USER FRIENDLY NAME
	 */

    strcpy( driver_name, "" );

    if ( driver_lib[ 0 ] != '/' )
	{
        strcpy( driver, driver_lib );

		/*
		 * allow the use of User odbcinst files, use no lock version as its 
         * protected by mutex
		 */

#ifdef PLATFORM64
		__SQLGetPrivateProfileStringNL( driver, "Driver64", "",
				driver_lib, sizeof( driver_lib ), "ODBCINST.INI" );

		if ( driver_lib[ 0 ] == '\0' )
		{
			__SQLGetPrivateProfileStringNL( driver, "Driver", "",
					driver_lib, sizeof( driver_lib ), "ODBCINST.INI" );
		}
#else
		__SQLGetPrivateProfileStringNL( driver, "Driver", "",
				driver_lib, sizeof( driver_lib ), "ODBCINST.INI" );
#endif

                strcpy( driver_name, driver );

		if ( driver_lib[ 0 ] == 0 ) {
            __set_config_mode( mode );
            __unlock_config_mode();
		    return NULL;
		}
	}

	strcpy( lib_name, driver_lib );

    __set_config_mode( mode );
    __unlock_config_mode();

    return lib_name;
}

static SQLSMALLINT sql_old_to_new(SQLSMALLINT type)
{
    switch(type) {
    case SQL_TIME:
      type=SQL_TYPE_TIME;
      break;
      
    case SQL_DATE:
      type=SQL_TYPE_DATE;
      break;

    case SQL_TIMESTAMP:
      type=SQL_TYPE_TIMESTAMP;
      break;
    }
    return type;
}
  
static SQLSMALLINT sql_new_to_old(SQLSMALLINT type)
{
    switch(type) {
    case SQL_TYPE_TIME:
      type=SQL_TIME;
      break;
      
    case SQL_TYPE_DATE:
      type=SQL_DATE;
      break;

    case SQL_TYPE_TIMESTAMP:
      type=SQL_TIMESTAMP;
      break;
    }
    return type;
}

static SQLSMALLINT c_old_to_new(SQLSMALLINT type)
{
    switch(type) {
    case SQL_C_TIME:
      type=SQL_C_TYPE_TIME;
      break;
      
    case SQL_C_DATE:
      type=SQL_C_TYPE_DATE;
      break;

    case SQL_C_TIMESTAMP:
      type=SQL_C_TYPE_TIMESTAMP;
      break;
    }
    return type;
}

static SQLSMALLINT c_new_to_old(SQLSMALLINT type)
{
    switch(type) {
    case SQL_C_TYPE_TIME:
      type=SQL_C_TIME;
      break;
      
    case SQL_C_TYPE_DATE:
      type=SQL_C_DATE;
      break;

    case SQL_C_TYPE_TIMESTAMP:
      type=SQL_C_TIMESTAMP;
      break;
    }
    return type;
}

SQLSMALLINT __map_type(int map, DMHDBC connection, SQLSMALLINT type)
{
  int driver_ver=connection->driver_act_ver;
  int wanted_ver=connection->environment->requested_version;

  if(driver_ver==SQL_OV_ODBC2 && wanted_ver>=SQL_OV_ODBC3) {
    switch(map) {
    case MAP_SQL_DM2D:
      type=sql_new_to_old(type);
      break;
      
    case MAP_SQL_D2DM:
      type=sql_old_to_new(type);
      break;

    case MAP_C_DM2D:
      type=c_new_to_old(type);
      break;

    case MAP_C_D2DM:
      type=c_old_to_new(type);
      break;
    }
  } else if(driver_ver>=SQL_OV_ODBC3 && wanted_ver==SQL_OV_ODBC2) {
    switch(map) {
    case MAP_SQL_DM2D:
      type=sql_old_to_new(type);
      break;
      
    case MAP_SQL_D2DM:
      type=sql_new_to_old(type);
      break;
      
    case MAP_C_DM2D:
      type=c_old_to_new(type);
      break;
      
    case MAP_C_D2DM:
      type=c_new_to_old(type);
      break;
    }
  } else if(driver_ver>=SQL_OV_ODBC3 && wanted_ver>=SQL_OV_ODBC3) {
    switch(map) {
    case MAP_SQL_DM2D:
    case MAP_SQL_D2DM:
      type=sql_old_to_new(type);
      break;
      
    case MAP_C_DM2D:
    case MAP_C_D2DM:
      type=c_old_to_new(type);
      break;
    }
  } else if(driver_ver==SQL_OV_ODBC2 && wanted_ver==SQL_OV_ODBC2) {
    switch(map) {
    case MAP_SQL_DM2D:
    case MAP_SQL_D2DM:
      type=sql_new_to_old(type);
      break;
      
    case MAP_C_DM2D:
    case MAP_C_D2DM:
      type=c_new_to_old(type);
      break;
    }
  }

  return type;
}

