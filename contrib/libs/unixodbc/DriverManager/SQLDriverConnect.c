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
 * $Id: SQLDriverConnect.c,v 1.28 2009/02/18 17:59:08 lurcher Exp $
 *
 * $Log: SQLDriverConnect.c,v $
 * Revision 1.28  2009/02/18 17:59:08  lurcher
 * Shift to using config.h, the compile lines were making it hard to spot warnings
 *
 * Revision 1.27  2009/02/17 09:47:44  lurcher
 * Clear up a number of bugs
 *
 * Revision 1.26  2009/01/16 11:02:38  lurcher
 * Interface to GUI for DSN selection
 *
 * Revision 1.25  2009/01/14 10:01:42  lurcher
 * remove debug printf
 *
 * Revision 1.24  2009/01/12 15:18:15  lurcher
 * Add interface into odbcinstQ to allow for a dialog if SQLDriverConnect is called without a DSN=
 *
 * Revision 1.23  2008/11/24 12:44:23  lurcher
 * Try and tidu up the connection version checking
 *
 * Revision 1.22  2008/09/29 14:02:44  lurcher
 * Fix missing dlfcn group option
 *
 * Revision 1.21  2008/08/29 08:01:38  lurcher
 * Alter the way W functions are passed to the driver
 *
 * Revision 1.20  2007/03/05 09:49:23  lurcher
 * Get it to build on VMS again
 *
 * Revision 1.19  2004/10/22 09:10:19  lurcher
 * Fix a couple of problems with FILEDSN's
 *
 * Revision 1.18  2004/09/08 16:38:54  lurcher
 *
 * Get ready for a 2.2.10 release
 *
 * Revision 1.17  2004/07/27 13:04:41  lurcher
 *
 * Strip FILEDSN from connection string before passing to driver
 *
 * Revision 1.16  2004/02/18 15:47:44  lurcher
 *
 * Fix a leak in the iconv code
 *
 * Revision 1.15  2003/10/30 18:20:45  lurcher
 *
 * Fix broken thread protection
 * Remove SQLNumResultCols after execute, lease S4/S% to driver
 * Fix string overrun in SQLDriverConnect
 * Add initial support for Interix
 *
 * Revision 1.14  2003/09/08 15:34:29  lurcher
 *
 * A couple of small but perfectly formed fixes
 *
 * Revision 1.13  2003/02/27 12:19:39  lurcher
 *
 * Add the A functions as well as the W
 *
 * Revision 1.12  2003/01/23 15:33:24  lurcher
 *
 * Fix problems with using putenv()
 *
 * Revision 1.11  2002/12/20 11:36:46  lurcher
 *
 * Update DMEnvAttr code to allow setting in the odbcinst.ini entry
 *
 * Revision 1.10  2002/12/05 17:44:30  lurcher
 *
 * Display unknown return values in return logging
 *
 * Revision 1.9  2002/10/14 09:46:10  lurcher
 *
 * Remove extra return
 *
 * Revision 1.8  2002/10/02 09:28:33  lurcher
 *
 * Fix uninitialised pointer in SQLDriverConnect.c
 *
 * Revision 1.7  2002/08/23 09:42:37  lurcher
 *
 * Fix some build warnings with casts, and a AIX linker mod, to include
 * deplib's on the link line, but not the libtool generated ones
 *
 * Revision 1.6  2002/07/25 09:30:26  lurcher
 *
 * Additional unicode and iconv changes
 *
 * Revision 1.5  2002/07/24 08:49:51  lurcher
 *
 * Alter UNICODE support to use iconv for UNICODE-ANSI conversion
 *
 * Revision 1.4  2002/05/24 12:42:50  lurcher
 *
 * Alter NEWS and ChangeLog to match their correct usage
 * Additional UNICODE tweeks
 *
 * Revision 1.3  2002/01/21 18:00:51  lurcher
 *
 * Assorted fixed and changes, mainly UNICODE/bug fixes
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
 * Revision 1.22  2001/10/16 10:37:32  nick
 *
 * Getting ready for 2.0.10
 *
 * Revision 1.21  2001/10/09 13:23:30  nick
 *
 * Add filedsn support to ODBCConfig
 *
 * Revision 1.20  2001/10/08 13:38:35  nick
 *
 * Add support for FILEDSN's
 *
 * Revision 1.19  2001/08/08 17:05:17  nick
 *
 * Add support for attribute setting in the ini files
 *
 * Revision 1.18  2001/07/20 13:20:44  nick
 * *** empty log message ***
 *
 * Revision 1.17  2001/07/20 12:35:09  nick
 *
 * Fix SQLBrowseConnect operation
 *
 * Revision 1.16  2001/05/15 10:57:44  nick
 *
 * Add initial support for VMS
 *
 * Revision 1.15  2001/04/18 15:03:37  nick
 *
 * Fix problem when going to DB2 unicode driver
 *
 * Revision 1.14  2001/04/16 22:35:10  nick
 *
 * More tweeks to the AutoTest code
 *
 * Revision 1.13  2001/04/16 15:41:24  nick
 *
 * Fix some problems calling non existing error funcs
 *
 * Revision 1.12  2001/04/12 17:43:36  nick
 *
 * Change logging and added autotest to odbctest
 *
 * Revision 1.11  2001/04/03 16:34:12  nick
 *
 * Add support for strangly broken unicode drivers
 *
 * Revision 1.10  2001/01/03 12:02:03  nick
 *
 * Add missing __
 *
 * Revision 1.9  2001/01/03 11:57:26  nick
 *
 * Fix some name collisions
 *
 * Revision 1.8  2000/12/31 20:30:54  nick
 *
 * Add UNICODE support
 *
 * Revision 1.7  2000/12/18 13:02:13  nick
 *
 * More buf fixes
 *
 * Revision 1.6  2000/12/18 12:53:29  nick
 *
 * More pooling tweeks
 *
 * Revision 1.5  2000/12/18 12:27:50  nick
 *
 * Fix missing check for SQL_NTS
 *
 * Revision 1.4  2000/12/14 18:10:19  nick
 *
 * Add connection pooling
 *
 * Revision 1.3  2000/10/13 15:18:49  nick
 *
 * Change string length parameter from SQLINTEGER to SQLSMALLINT
 *
 * Revision 1.2  2000/10/06 08:49:38  nick
 *
 * Fix duplicated error messages on connect
 *
 * Revision 1.1.1.1  2000/09/04 16:42:52  nick
 * Imported Sources
 *
 * Revision 1.15  2000/08/11 12:11:27  ngorham
 *
 * Make SQLDriverConnect return all the stacked errors from the driver, not
 * just one
 *
 * Revision 1.14  2000/07/13 13:27:24  ngorham
 *
 * remove _ from odbcinst_system_file_path()
 *
 * Revision 1.13  2000/06/21 08:58:26  ngorham
 *
 * Stop SQLDriverConnect dumping core when passed a null dsn string
 *
 * Revision 1.12  2000/02/20 10:18:47  ngorham
 *
 * Add support for ODBCINI environment override for Applix.
 *
 * Revision 1.11  1999/12/14 19:02:25  ngorham
 *
 * Mask out the password fields in the logging
 *
 * Revision 1.10  1999/11/13 23:40:59  ngorham
 *
 * Alter the way DM logging works
 * Upgrade the Postgres driver to 6.4.6
 *
 * Revision 1.9  1999/10/24 23:54:17  ngorham
 *
 * First part of the changes to the error reporting
 *
 * Revision 1.8  1999/10/14 06:49:24  ngorham
 *
 * Remove @all_includes@ from Drivers/MiniSQL/Makefile.am
 *
 * Revision 1.7  1999/09/21 22:34:24  ngorham
 *
 * Improve performance by removing unneeded logging calls when logging is
 * disabled
 *
 * Revision 1.6  1999/08/17 06:20:00  ngorham
 *
 * Remove posibility of returning without clearing the connection mutex.
 *
 * Revision 1.5  1999/08/03 21:47:39  shandyb
 * Moving to automake: changed files in DriverManager
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
#include <string.h>
#include "drivermanager.h"

static char const rcsid[]= "$RCSfile: SQLDriverConnect.c,v $ $Revision: 1.28 $";

/*
 * connection pooling stuff
 */

extern int pooling_enabled;
extern int pool_wait_timeout;

void __generate_connection_string( struct con_struct *con_str, char *str, int str_len )
{
struct con_pair *cp;
char *tmp;

    str[ 0 ] = '\0';

    if ( con_str -> count == 0 )
    {
        return;
    }

    cp = con_str -> list;
    while( cp )
    {
        size_t attrlen = strlen( cp -> attribute );
        int use_esc = isspace( *(cp -> attribute ) ) || ( attrlen && isspace( cp->attribute[attrlen - 1] ));
        for ( tmp = cp -> attribute; *tmp; tmp++ )
        {
            use_esc |= (*tmp == '{') || (*tmp == '}');
            attrlen += (*tmp == '}');
        }
        tmp = malloc( strlen( cp -> keyword ) + attrlen + 10 );
        if( use_esc )
        {
            char *tmp2 = tmp + sprintf( tmp, "%s={", cp -> keyword );
            char *tmp3;
            for( tmp3 = cp -> attribute; *tmp3 ; tmp3++ )
            {
                *tmp2++ = *tmp3;
                if ( '}' == *tmp3++ )
                {
                    *tmp2++ = '}';
                }
            }
            *tmp2++ = '}';
            *tmp2++ = 0;
        }
        else
        {
            sprintf( tmp, "%s=%s;", cp -> keyword, cp -> attribute );
        }

        if ( strlen( str ) + strlen( tmp ) > str_len )
        {
            free( tmp );
            break;
        }
        else
        {
            strcat( str, tmp );
        }
        free( tmp );
                                
        cp = cp -> next;
    }
}

void __get_attr( char ** cp, char ** keyword, char ** value )
{
char * ptr;
int len;

    *keyword = *value = NULL;

    while ( isspace( **cp ) || **cp == ';' )
    {
        (*cp)++;
    }

    if ( !**cp )
        return;

    ptr = *cp;

    /* 
     * To handle the case attribute in which the attribute is of the form
     * "ATTR;" instead of "ATTR=VALUE;"
     */

    while ( **cp && **cp != '=' )
    {
        (*cp)++;
    }

    if ( !**cp )
        return;

    len = *cp - ptr;
    *keyword = malloc( len + 1 );
    memcpy( *keyword, ptr, len );
    (*keyword)[ len ] = '\0';

    (*cp)++;

    if ( **cp == '{' )
    {
        /* escaped with '{' - all characters until next '}' not followed by '}', or
           end of string, are part of the value */
        int i = 0;
        ptr = ++*cp;
        while ( **cp && (**cp != '}' || (*cp)[1] == '}') )
        {
            if ( **cp == '}' )
                (*cp)++;
            (*cp)++;
        }
        len = *cp - ptr;
        *value = malloc( len + 1 );
        while( ptr < *cp )
        {
            if ( ((*value)[i++] = *ptr++) == '}')
            {
                ptr++;
            }
        }
        (*value)[i] = 0;
        if ( **cp == '}' )
        {
            (*cp)++;
        }
    }
    else
    {
        /* non-escaped: all characters until ';' or end of string are value */
        ptr = *cp;
        while ( **cp && **cp != ';' )
        {
            (*cp)++;
        }
        len = *cp - ptr;
        *value = malloc( len + 1 );
        memcpy( *value, ptr, len );
        (*value)[ len ] = 0;
    }
}

struct con_pair * __get_pair( char ** cp )
{
char *keyword, *value;
struct con_pair * con_p;

    __get_attr( cp, &keyword, &value );
    if ( keyword )
    {
        con_p = malloc( sizeof( *con_p ));
        if ( !con_p )
        {
            free(keyword);
            if ( value )
                free(value);
            return NULL;
        }
        con_p -> keyword = keyword;
        con_p -> attribute = value;
        return con_p;
    }
    else
    {
        return NULL;
    }
}

int __append_pair( struct con_struct *con_str, char *kword, char *value )
{
struct con_pair *ptr, *end;

    /* check that the keyword is not already in the list */

    end = NULL;
    if ( con_str -> count > 0 )
    {
        ptr = con_str -> list;
        while( ptr )
        {
            if( strcasecmp( kword, ptr -> keyword ) == 0 )
            {
                free( ptr -> attribute );
                ptr -> attribute = malloc( strlen( value ) + 1 );
                strcpy( ptr -> attribute, value );
                return 0;
            }
            end = ptr;
            ptr = ptr -> next;
        }
    }

    ptr = malloc( sizeof( *ptr ));

    ptr -> keyword = malloc( strlen( kword ) + 1 );
    strcpy( ptr -> keyword, kword );

    ptr -> attribute = malloc( strlen( value ) + 1 );
    strcpy( ptr -> attribute, value );

    con_str -> count ++;

    if ( con_str -> list )
    {
        end -> next = ptr;
        ptr -> next = NULL;
    }
    else
    {
        ptr -> next = NULL;
        con_str -> list = ptr;
    }

    return 0;
}

int __parse_connection_string_ex( struct con_struct *con_str,
    char *str, int str_len, int exclude )
{
struct con_pair *cp;
char *local_str, *ptr;
int got_dsn = 0;    /* if we have a DSN then ignore any DRIVER or FILEDSN */
int got_driver = 0;    /* if we have a DRIVER or FILEDSN then ignore any DSN */

    con_str -> count = 0;
    con_str -> list = NULL;

    if ( str_len != SQL_NTS )
    {
        local_str = malloc( str_len + 1 );
        memcpy( local_str, str, str_len );
        local_str[ str_len ] = '\0';
    }
    else
    {
        local_str = str;
    }

    if ( !local_str || strlen( local_str ) == 0 ||
        ( strlen( local_str ) == 1 && *local_str == ';' ))
    {
        /* connection-string ::= empty-string [;] */
        if ( str_len != SQL_NTS )
            free( local_str );
        return 0;
    }

    ptr = local_str;

    while(( cp = __get_pair( &ptr )) != NULL )
    {
        if ( strcasecmp( cp -> keyword, "DSN" ) == 0 )
        {
            if ( got_driver && exclude ) {
                /* 11-29-2010 JM Modify to free the allocated memory before continuing. */
                free( cp -> keyword );
                free( cp -> attribute );
                free( cp );
                continue;
            }

            got_dsn = 1;
        }
        else if ( strcasecmp( cp -> keyword, "DRIVER" ) == 0 ||
            strcasecmp( cp -> keyword, "FILEDSN" ) == 0 )
        {
            if ( got_dsn && exclude ) {
                /* 11-29-2010 JM Modify to free the allocated memory before continuing. */
                free( cp -> keyword );
                free( cp -> attribute );
                free( cp );
                continue;
            }

            got_driver = 1;
        }

        __append_pair( con_str, cp -> keyword, cp -> attribute );
        free( cp -> keyword );
        free( cp -> attribute );
        free( cp );
    }

    if ( str_len != SQL_NTS )
        free( local_str );

    return 0;
}

int __parse_connection_string( struct con_struct *con_str,
    char *str, int str_len )
{
    return  __parse_connection_string_ex( con_str, str, str_len, 1 );
}

char * __get_attribute_value( struct con_struct * con_str, char * keyword )
{
struct con_pair *cp;

    if ( con_str -> count == 0 )
        return NULL;

    cp = con_str -> list;
    while( cp )
    {
        if( strcasecmp( keyword, cp -> keyword ) == 0 )
        {
            if ( cp -> attribute )
                return cp -> attribute;
            else
                return "";
        }
        cp = cp -> next;
    }
    return NULL;
}

void __release_conn( struct con_struct *con_str )
{
    struct con_pair *cp = con_str -> list;
    struct con_pair *save;

    while( cp )
    {
        free( cp -> attribute );
        free( cp -> keyword );
        save = cp;
        cp = cp -> next;
        free( save );
    }

    con_str -> count = 0;
}

void __handle_attr_extensions_cs( DMHDBC connection, struct con_struct *con_str )
{
    char *ptr;

    if (( ptr = __get_attribute_value( con_str, "DMEnvAttr" )) != NULL ) {
        __parse_attribute_string( &connection -> env_attribute, ptr, SQL_NTS );
    }
    if (( ptr = __get_attribute_value( con_str, "DMConnAttr" )) != NULL ) {
        __parse_attribute_string( &connection -> dbc_attribute, ptr, SQL_NTS );
    }
    if (( ptr = __get_attribute_value( con_str, "DMStmtAttr" )) != NULL ) {
        __parse_attribute_string( &connection -> stmt_attribute, ptr, SQL_NTS );
    }
}

SQLRETURN SQLDriverConnectA(
    SQLHDBC            hdbc,
    SQLHWND            hwnd,
    SQLCHAR            *conn_str_in,
    SQLSMALLINT        len_conn_str_in,
    SQLCHAR            *conn_str_out,
    SQLSMALLINT        conn_str_out_max,
    SQLSMALLINT        *ptr_conn_str_out,
    SQLUSMALLINT       driver_completion )
{
    return SQLDriverConnect( hdbc,
                                hwnd,
                                conn_str_in,
                                len_conn_str_in,
                                conn_str_out,
                                conn_str_out_max,
                                ptr_conn_str_out,
                                driver_completion );
}

SQLRETURN SQLDriverConnect(
    SQLHDBC            hdbc,
    SQLHWND            hwnd,
    SQLCHAR            *conn_str_in,
    SQLSMALLINT        len_conn_str_in,
    SQLCHAR            *conn_str_out,
    SQLSMALLINT        conn_str_out_max,
    SQLSMALLINT        *ptr_conn_str_out,
    SQLUSMALLINT       driver_completion )
{
    DMHDBC connection = (DMHDBC)hdbc;
    struct con_struct con_struct;
    char *driver, *dsn = NULL, *filedsn, *tsavefile, savefile[ INI_MAX_PROPERTY_VALUE + 1 ];
    char lib_name[ INI_MAX_PROPERTY_VALUE + 1 ];
    char driver_name[ INI_MAX_PROPERTY_VALUE + 1 ];
    SQLRETURN ret_from_connect;
    SQLCHAR s1[ 2048 ];
    SQLCHAR local_conn_str_in[ 2048 ];
    SQLCHAR local_out_conection[ 2048 ];
    char *save_filedsn;
    int warnings = 0;
    CPOOLHEAD *pooh = 0;

    /*
     * check connection
     */

    strcpy( driver_name, "" );

    if ( !__validate_dbc( connection ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: SQL_INVALID_HANDLE" );

        return SQL_INVALID_HANDLE;
    }

    function_entry( connection );

    /*
     * replace this if not set for use by SAVEFILE
     */

    if ( !conn_str_out )
    {
        conn_str_out = local_out_conection;
        conn_str_out_max = sizeof( local_out_conection );
    }

    if ( log_info.log_flag )
    {
        sprintf( connection -> msg, "\n\t\tEntry:\
\n\t\t\tConnection = %p\
\n\t\t\tWindow Hdl = %p\
\n\t\t\tStr In = %s\
\n\t\t\tStr Out = %p\
\n\t\t\tStr Out Max = %d\
\n\t\t\tStr Out Ptr = %p\
\n\t\t\tCompletion = %d",
                connection,
                hwnd,
                __string_with_length_hide_pwd( s1, conn_str_in, 
                    len_conn_str_in ), 
                conn_str_out,
                conn_str_out_max,
                ptr_conn_str_out,
                driver_completion );

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    thread_protect( SQL_HANDLE_DBC, connection );

    if ( len_conn_str_in < 0 && len_conn_str_in != SQL_NTS )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY090" );

        __post_internal_error( &connection -> error,
                ERROR_HY090, NULL,
                connection -> environment -> requested_version );

        return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
    }

    if ( driver_completion == SQL_DRIVER_PROMPT &&
            hwnd == NULL )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY092" );

        __post_internal_error( &connection -> error,
                ERROR_HY092, NULL,
                connection -> environment -> requested_version );

        return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
    }

    if ( driver_completion != SQL_DRIVER_PROMPT &&
            driver_completion != SQL_DRIVER_COMPLETE &&
            driver_completion != SQL_DRIVER_COMPLETE_REQUIRED &&
            driver_completion != SQL_DRIVER_NOPROMPT )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: HY110" );

        __post_internal_error( &connection -> error,
                ERROR_HY110, NULL,
                connection -> environment -> requested_version );

        return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
    }

    /*
     * check the state of the connection
     */

    if ( connection -> state != STATE_C2 )
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: 08002" );

        __post_internal_error( &connection -> error,
                ERROR_08002, NULL,
                connection -> environment -> requested_version );

        return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
    }

    /*
     * parse the connection string, and call the GUI if needed
     */

	if ( driver_completion == SQL_DRIVER_NOPROMPT ) 
	{
    	if ( !conn_str_in )
    	{
        	conn_str_in = (SQLCHAR*)"DSN=DEFAULT;";
        	len_conn_str_in = strlen((char*) conn_str_in );
		}

		__parse_connection_string( &con_struct,
				(char*)conn_str_in, len_conn_str_in );
    }
	else {
    	if ( !conn_str_in )
    	{
        	conn_str_in = (SQLCHAR*)"";
        	len_conn_str_in = strlen((char*) conn_str_in );
		}

    	__parse_connection_string( &con_struct,
            	(char*)conn_str_in, len_conn_str_in );

		if ( !__get_attribute_value( &con_struct, "DSN" ) && 
			!__get_attribute_value( &con_struct, "DRIVER" ) && 
			!__get_attribute_value( &con_struct, "FILEDSN" ))
		{
			int ret;
			SQLCHAR returned_dsn[ 1025 ], *prefix, *target;

			/*
			 * try and call GUI to obtain a DSN
			 */

			ret = _SQLDriverConnectPrompt( hwnd, returned_dsn, sizeof( returned_dsn ));
			if ( !ret || returned_dsn[ 0 ] == '\0' ) 
			{
        		__append_pair( &con_struct, "DSN", "DEFAULT" );
			}
			else 
			{
				prefix = returned_dsn;
				target = (SQLCHAR*)strchr( (char*)returned_dsn, '=' );
				if ( target ) 
				{
					*target = '\0';
					target ++;
        			__append_pair( &con_struct, (char*)prefix, (char*)target );
				}
				else {
        			__append_pair( &con_struct, "DSN", (char*)returned_dsn );
				}
			}

			/*
			 * regenerate to pass to driver
			 */
			__generate_connection_string( &con_struct, (char*)local_conn_str_in, sizeof( local_conn_str_in ));
			conn_str_in = local_conn_str_in;
        	len_conn_str_in = strlen((char*) conn_str_in );
		}
	}

    /*
     * can we find a pooled connection to use here ?
     */

    connection -> pooled_connection = NULL;

    if ( pooling_enabled )
    {
        int retpool;
        int retrying = 0;
        time_t wait_begin = time( NULL );

retry:
        retpool = search_for_pool(  connection,
                                NULL, 0,
                                NULL, 0,
                                NULL, 0,
                                conn_str_in, len_conn_str_in, &pooh, retrying );
        /*
         * found usable existing connection from pool
         */
        if ( retpool == 1 )
        {
            /*
             * copy the in string to the out string
             */

            ret_from_connect = SQL_SUCCESS;

            if ( conn_str_out )
            {
                if ( len_conn_str_in < 0 )
                {
                    len_conn_str_in = strlen((char*) conn_str_in );
                }

                if ( len_conn_str_in >= conn_str_out_max )
                {
                    memcpy( conn_str_out, conn_str_in, conn_str_out_max - 1 );
                    conn_str_out[ conn_str_out_max - 1 ] = '\0';
                    if ( ptr_conn_str_out )
                    {
                        *ptr_conn_str_out = len_conn_str_in;
                    }

                    __post_internal_error( &connection -> error,
                        ERROR_01004, NULL,
                        connection -> environment -> requested_version );

                    ret_from_connect = SQL_SUCCESS_WITH_INFO;
                }
                else
                {
                    memcpy( conn_str_out, conn_str_in, len_conn_str_in );
                    conn_str_out[ len_conn_str_in ] = '\0';
                    if ( ptr_conn_str_out )
                    {
                        *ptr_conn_str_out = len_conn_str_in;
                    }
                }
            }

            if ( log_info.log_flag )
            {
                sprintf( connection -> msg,
                        "\n\t\tExit:[%s]",
                            __get_return_status( ret_from_connect, s1 ));

                dm_log_write( __FILE__,
                            __LINE__,
                        LOG_INFO,
                        LOG_INFO,
                        connection -> msg );
            }

            connection -> state = STATE_C4;

            __release_conn( &con_struct );

            return function_return( SQL_HANDLE_DBC, connection, ret_from_connect, DEFER_R0 );
        }

        /*
         * pool is at capacity
         */
        if ( retpool == 2 )
        {
            /*
             * either no timeout or exceeded the timeout
             */
            if ( ! pool_wait_timeout || time( NULL ) - wait_begin > pool_wait_timeout )
            {
                mutex_pool_exit();
                dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    "Error: HYT02" );

                __post_internal_error( &connection -> error,
                    ERROR_HYT02, NULL,
                    connection -> environment -> requested_version );

                __release_conn( &con_struct );

                return function_return_nodrv( SQL_HANDLE_DBC, connection, SQL_ERROR );
            }

            /*
             * wait up to 1 second for a signal and try again
             */
            pool_timedwait( connection );
            retrying = 1;
            goto retry;
        }

        /*
         * 1 pool entry has been reserved. Early exits henceforth need to unreserve.
         */
    }

    /*
     * else save the info for later
     */

    if ( pooling_enabled )
    {
        connection -> dsn_length = 0;

        connection -> _server = strdup( "" );
        connection -> server_length = 0;
        connection -> _user = strdup( "" );
        connection -> user_length = 0;
        connection -> _password = strdup( "" );
        connection -> password_length = 0;

        if ( len_conn_str_in == SQL_NTS )
        {
            connection -> _driver_connect_string = strdup((char*)conn_str_in );
        }
        else
        {
            connection -> _driver_connect_string = calloc( len_conn_str_in, 1 );
            memcpy( connection -> _driver_connect_string, conn_str_in, len_conn_str_in );
        }
        connection -> dsn_length = len_conn_str_in;
    }


    /*
     * get for later
     */

    tsavefile = __get_attribute_value( &con_struct, "SAVEFILE" );
    if ( tsavefile )
    {
        if ( strlen( tsavefile ) > INI_MAX_PROPERTY_VALUE ) {
            memcpy( savefile, tsavefile, INI_MAX_PROPERTY_VALUE );
            savefile[ INI_MAX_PROPERTY_VALUE ] = '\0';
        }
        else {
            strcpy( savefile, tsavefile );
        }
    }
    else
    {
        savefile[ 0 ] = '\0';
    }

    /*
     * open the file dsn, get each entry from it, if it's not in the connection 
     * struct, add it
     */

    filedsn = __get_attribute_value( &con_struct, "FILEDSN" );
    if ( filedsn )
    {
        char str[ 1024 * 16 ];

        if ( SQLReadFileDSN( filedsn, "ODBC", NULL, str, sizeof( str ), NULL ))
        {
            struct con_struct con_struct1;

            save_filedsn = strdup( filedsn );

            if ( strlen( str ))
            {
                strcpy((char*)local_conn_str_in, (char*)conn_str_in );
                conn_str_in = local_conn_str_in;

                __parse_connection_string( &con_struct1,
                        str, strlen( str ));

				/*
		 		* Get the attributes from the original string
		 		*/

				conn_str_in[ 0 ] = '\0';

                if ( con_struct.count )
                {
                    struct con_pair *cp;

                    cp = con_struct.list;
                    while( cp )
                    {
		    			char *str1;

						/*
			 			* Don't pass FILEDSN down
			 			*/

						if ( strcasecmp( cp -> keyword, "FILEDSN" ) &&
							strcasecmp( cp -> keyword, "FILEDSN" ) )
						{
                            str1 = malloc( strlen( cp -> keyword ) + strlen( cp -> attribute ) + 10 );

                            if ( !str1 ) {
                                dm_log_write( __FILE__, 
                                        __LINE__, 
                                        LOG_INFO, 
                                        LOG_INFO, 
                                        "Error: HY001" );
                        
                                __post_internal_error( &connection -> error,
                                        ERROR_HY001, NULL,
                                        connection -> environment -> requested_version );

                                if ( save_filedsn ) {
                                    free( save_filedsn );
                                }

                                pool_unreserve( pooh );
                        
                                __release_conn( &con_struct );
                                __release_conn( &con_struct1 );

                                return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
                            }

                            if ( strlen((char*) conn_str_in ) > 0 )
                            {
                                sprintf( str1, ";%s=%s", cp -> keyword, cp -> attribute );
                            }
                            else
                            {
                                sprintf( str1, "%s=%s", cp -> keyword, cp -> attribute );
                            }

                            if ( strlen( (char*)conn_str_in ) + strlen( str1 ) < conn_str_out_max )
                            {
                                strcat((char*) conn_str_in, str1 );
                            }
                            else
                            {
                                warnings = 1;
                                __post_internal_error( &connection -> error,
                                        ERROR_01004, NULL,
                                        connection -> environment -> requested_version );
                            }

                            free( str1 );
                        }

                        cp = cp -> next;
                    }
                }

                if ( con_struct1.count )
                {
                    struct con_pair *cp;

                    cp = con_struct1.list;
                    while( cp )
                    {
                        if ( !__get_attribute_value( &con_struct, cp -> keyword ))
                        {
                            char *str1;

                            str1 = malloc( strlen( cp -> keyword ) + strlen( cp -> attribute ) + 10 );
                            if ( !str1 ) {
                                dm_log_write( __FILE__, 
                                        __LINE__, 
                                        LOG_INFO, 
                                        LOG_INFO, 
                                        "Error: HY001" );
                        
                                __post_internal_error( &connection -> error,
                                        ERROR_HY001, NULL,
                                        connection -> environment -> requested_version );

                                if ( save_filedsn ) {
                                    free( save_filedsn );
                                }

                                pool_unreserve( pooh );

                                __release_conn( &con_struct1 );

                                return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
                            }

                            if ( strlen((char*) conn_str_in ) > 0 )
                            {
                            	sprintf( str1, ";%s=%s", cp -> keyword, cp -> attribute );
                            }
                            else
                            {
                            	sprintf( str1, "%s=%s", cp -> keyword, cp -> attribute );
							}
                            if ( strlen( (char*)conn_str_in ) + strlen( str1 ) < conn_str_out_max ) {
                                strcat((char*) conn_str_in, str1 );
                            }
                            else {
                                warnings = 1;
                                __post_internal_error( &connection -> error,
                                        ERROR_01004, NULL,
                                        connection -> environment -> requested_version );
                            }
                            free( str1 );
                        }
                        cp = cp -> next;
                    }
                }

                len_conn_str_in = strlen((char*) conn_str_in );

                __release_conn( &con_struct1 );
            }

            /*
             * reparse the string
             */

            __release_conn( &con_struct );

            __parse_connection_string( &con_struct,
                    (char*)conn_str_in, len_conn_str_in );
        }
        else
        {
            save_filedsn = NULL;
        }
    }
    else
    {
	    save_filedsn = NULL;
    }

    /*
     * look for some keywords
     *
     * have we got a DRIVER= attribute
     */
    driver = __get_attribute_value( &con_struct, "DRIVER" );
    if ( driver )
    {
        /*
         * look up the driver in the ini file
         */

        if ( strlen( driver ) >= sizeof( driver_name )) {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM011" );
                        
            __post_internal_error( &connection -> error,
                    ERROR_IM011, NULL,
                    connection -> environment -> requested_version );

            if ( save_filedsn ) {
                free( save_filedsn );
            }

            pool_unreserve( pooh );

            __release_conn( &con_struct );

            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
        }

        strcpy( driver_name, driver );

#ifdef PLATFORM64
        SQLGetPrivateProfileString( driver, "Driver64", "",
                lib_name, sizeof( lib_name ), "ODBCINST.INI" );

		if ( lib_name[ 0 ] == '\0' )
		{
        	SQLGetPrivateProfileString( driver, "Driver", "",
                	lib_name, sizeof( lib_name ), "ODBCINST.INI" );
		}
#else
        SQLGetPrivateProfileString( driver, "Driver", "",
                lib_name, sizeof( lib_name ), "ODBCINST.INI" );
#endif

        /*
         * Assume if it's not in a odbcinst.ini then it's a direct reference
         */

        if ( lib_name[ 0 ] == '\0' ) {
            strcpy( lib_name, driver );
        }

        strcpy( connection -> dsn, "" );
        __handle_attr_extensions( connection, NULL, driver_name );
    }
    else
    {
        dsn = __get_attribute_value( &con_struct, "DSN" );
        if ( !dsn )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM002" );

            __post_internal_error( &connection -> error,
                    ERROR_IM002, NULL,
                    connection -> environment -> requested_version );

            __release_conn( &con_struct );

            if ( save_filedsn ) {
                free( save_filedsn );
            }

            pool_unreserve( pooh );

            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
        }

        if ( strlen( dsn ) > SQL_MAX_DSN_LENGTH )
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM012" );

            __post_internal_error( &connection -> error,
                    ERROR_IM012, NULL,
                    connection -> environment -> requested_version );

            __release_conn( &con_struct );

            if ( save_filedsn ) {
                free( save_filedsn );
            }

            pool_unreserve( pooh );

            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
        }

        /*
         * look up the dsn in the ini file
         */

        if ( !__find_lib_name( dsn, lib_name, driver_name ))
        {
            dm_log_write( __FILE__, 
                    __LINE__, 
                    LOG_INFO, 
                    LOG_INFO, 
                    "Error: IM002" );

            __post_internal_error( &connection -> error,
                    ERROR_IM002, NULL,
                    connection -> environment -> requested_version );

            __release_conn( &con_struct );

            if ( save_filedsn ) {
                free( save_filedsn );
            }

            pool_unreserve( pooh );

            return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
        }

        strcpy( connection -> dsn, dsn );
        __handle_attr_extensions( connection, dsn, driver_name );
    }


    if ( dsn )
    {
        /*
         * do we have any Environment, Connection, or Statement attributes set in the ini ?
         */

        __handle_attr_extensions( connection, dsn, driver_name );
    }
    else {
        /* 
         * the attributes may be in the connection string
         */
        __handle_attr_extensions_cs( connection, &con_struct );
    }

    __release_conn( &con_struct );

    /*
     * we have now got the name of a lib to load
     */
    if ( !__connect_part_one( connection, lib_name, driver_name, &warnings ))
    {
        if ( save_filedsn ) {
            free( save_filedsn );
        }

        __disconnect_part_four( connection );       /* release unicode handles */

        pool_unreserve( pooh );

        return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
    }

    if ( !CHECK_SQLDRIVERCONNECT( connection ) &&
        !CHECK_SQLDRIVERCONNECTW( connection ))
    {
        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                "Error: IM001" );

        __disconnect_part_one( connection );
        __disconnect_part_four( connection );       /* release unicode handles */
        __post_internal_error( &connection -> error,
                ERROR_IM001, NULL,
                connection -> environment -> requested_version );

        if ( save_filedsn ) {
            free( save_filedsn );
        }

        pool_unreserve( pooh );

        return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
    }

    if ( CHECK_SQLDRIVERCONNECT( connection ))
    {
        /*
        if ( CHECK_SQLSETCONNECTATTR( connection ))
        {
            int lret;
                
            lret = SQLSETCONNECTATTR( connection,
                    connection -> driver_dbc,
                    SQL_ATTR_ANSI_APP,
                    SQL_AA_TRUE,
                    0 );
        }
        */

        ret_from_connect = SQLDRIVERCONNECT( connection,
                connection -> driver_dbc,
                hwnd,
                conn_str_in,
                len_conn_str_in,
                conn_str_out,
                conn_str_out_max,
                ptr_conn_str_out,
                driver_completion );

        if ( ret_from_connect != SQL_SUCCESS )
        {
            SQLCHAR sqlstate[ 6 ];
            SQLINTEGER native_error;
            SQLSMALLINT ind;
            SQLCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
            SQLRETURN ret;

            /*
             * get the errors from the driver before
             * loseing the connection 
             */

            if ( CHECK_SQLERROR( connection ))
            {
                do
                {
                    ret = SQLERROR( connection,
                            SQL_NULL_HENV,
                            connection -> driver_dbc,
                            SQL_NULL_HSTMT,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );

                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex_noprefix( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                            sqlstate, message_text );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
            else if ( CHECK_SQLGETDIAGREC( connection ))
            {
                int rec = 1;

                do
                {
                    ret = SQLGETDIAGREC( connection,
                            SQL_HANDLE_DBC,
                            connection -> driver_dbc,
                            rec ++,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        __post_internal_error_ex_noprefix( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                            sqlstate, message_text );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
        }


        /* 
         * if it was a error then return now
         */

        if ( !SQL_SUCCEEDED( ret_from_connect ))
        {
            __disconnect_part_one( connection );
            __disconnect_part_four( connection );

            sprintf( connection -> msg,
                    "\n\t\tExit:[%s]",
                        __get_return_status( ret_from_connect, s1 ));

            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    connection -> msg );

            if ( save_filedsn ) {
                free( save_filedsn );
            }

            pool_unreserve( pooh );

            return function_return( SQL_HANDLE_DBC, connection, ret_from_connect, DEFER_R0 );
        }
        connection -> unicode_driver = 0;
    }
    else
    {
        SQLWCHAR *uc_conn_str_in, *s1 = NULL;
        SQLCHAR s2[ 128 ];
        int wlen;

        uc_conn_str_in = ansi_to_unicode_alloc( conn_str_in, len_conn_str_in, connection, &wlen );
        len_conn_str_in = wlen;

        if ( CHECK_SQLSETCONNECTATTR( connection ))
        {
            SQLSETCONNECTATTR( connection,
                    connection -> driver_dbc,
                    SQL_ATTR_ANSI_APP,
                    SQL_AA_FALSE,
                    0 );
        }

        if ( conn_str_out && conn_str_out_max > 0 )
        {
            s1 = malloc( sizeof( SQLWCHAR ) * ( conn_str_out_max + 1 ));
        }

        ret_from_connect = SQLDRIVERCONNECTW( connection,
                connection -> driver_dbc,
                hwnd,
                uc_conn_str_in,
                len_conn_str_in,
                s1 ? s1 : (SQLWCHAR*)conn_str_out,
                conn_str_out_max,
                ptr_conn_str_out,
                driver_completion );

        if ( uc_conn_str_in )
            free( uc_conn_str_in );

        if ( ret_from_connect != SQL_SUCCESS )
        {
            SQLWCHAR sqlstate[ 6 ];
            SQLINTEGER native_error;
            SQLSMALLINT ind;
            SQLWCHAR message_text[ SQL_MAX_MESSAGE_LENGTH + 1 ];
            SQLRETURN ret;

            /*
             * get the errors from the driver before
             * loseing the connection 
             */

            if ( CHECK_SQLERRORW( connection ))
            {
                do
                {
                    ret = SQLERRORW( connection,
                            SQL_NULL_HENV,
                            connection -> driver_dbc,
                            SQL_NULL_HSTMT,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ) / sizeof( SQLWCHAR ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        SQLCHAR *as1, *as2;

                        __post_internal_error_ex_w_noprefix( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        as1 = (SQLCHAR*) unicode_to_ansi_alloc( sqlstate, SQL_NTS, connection, NULL );
                        as2 = (SQLCHAR*) unicode_to_ansi_alloc( message_text, SQL_NTS, connection, NULL );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                                as1, as2 );

                        if ( as1 ) free( as1 );
                        if ( as2 ) free( as2 );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
            else if ( CHECK_SQLGETDIAGRECW( connection ))
            {
                int rec = 1;

                do
                {
                    ret = SQLGETDIAGRECW( connection,
                            SQL_HANDLE_DBC,
                            connection -> driver_dbc,
                            rec ++,
                            sqlstate,
                            &native_error,
                            message_text,
                            sizeof( message_text ) / sizeof( SQLWCHAR ),
                            &ind );


                    if ( SQL_SUCCEEDED( ret ))
                    {
                        SQLCHAR *as1, *as2;

                        __post_internal_error_ex_w_noprefix( &connection -> error,
                                sqlstate,
                                native_error,
                                message_text,
                                SUBCLASS_ODBC, SUBCLASS_ODBC );

                        as1 = (SQLCHAR*) unicode_to_ansi_alloc( sqlstate, SQL_NTS, connection, NULL );
                        as2 = (SQLCHAR*) unicode_to_ansi_alloc( message_text, SQL_NTS, connection, NULL );

                        sprintf( connection -> msg, "\t\tDIAG [%s] %s",
                            as1, as2 );

                        if ( as1 ) free( as1 );
                        if ( as2 ) free( as2 );

                        dm_log_write_diag( connection -> msg );
                    }
                }
                while( SQL_SUCCEEDED( ret ));
            }
        }

        /* 
         * if it was a error then return now
         */

        if ( !SQL_SUCCEEDED( ret_from_connect ))
        {
            __disconnect_part_one( connection );
            __disconnect_part_four( connection );

            sprintf( connection -> msg,
                    "\n\t\tExit:[%s]",
                        __get_return_status( ret_from_connect, s2 ));

            dm_log_write( __FILE__,
                    __LINE__,
                    LOG_INFO,
                    LOG_INFO,
                    connection -> msg );

            if ( save_filedsn ) {
                free( save_filedsn );
            }

            if ( s1 )
            {
                free( s1 );
            }

            pool_unreserve( pooh );

            return function_return( SQL_HANDLE_DBC, connection, ret_from_connect, DEFER_R0 );
        }
        else
        {
            if ( conn_str_out && s1 )
            {
                unicode_to_ansi_copy((char*) conn_str_out, conn_str_out_max, s1, SQL_NTS, connection, NULL );
            }
        }

        if ( s1 )
        {
            free( s1 );
        }

        connection -> unicode_driver = 1;
    }

    /*
     * we should be connected now
     */

    connection -> state = STATE_C4;

    /*
     * did we get the type we wanted
     */

    if ( connection -> driver_version !=
            connection -> environment -> requested_version )
    {
        connection -> driver_version =
            connection -> environment -> requested_version;

        __post_internal_error( &connection -> error,
                ERROR_01000, "Driver does not support the requested version",
                connection -> environment -> requested_version );
        ret_from_connect = SQL_SUCCESS_WITH_INFO;
    }

    if ( !__connect_part_two( connection ))
    {
        __disconnect_part_two( connection );
        __disconnect_part_one( connection );
        __disconnect_part_four( connection );

        if ( save_filedsn ) {
            free( save_filedsn );
        }

        pool_unreserve( pooh );

        return function_return( SQL_HANDLE_DBC, connection, SQL_ERROR, DEFER_R0 );
    }

    if ( log_info.log_flag )
    {
        if ( conn_str_out && strlen((char*) conn_str_out ) > 64 )
        {
            sprintf( connection -> msg, 
                    "\n\t\tExit:[%s]\
\n\t\t\tConnection Out [%.64s...]",
                        __get_return_status( ret_from_connect, s1 ),
                        conn_str_out );
        }
        else
        {
            sprintf( connection -> msg, 
                    "\n\t\tExit:[%s]\
\n\t\t\tConnection Out [%s]",
                        __get_return_status( ret_from_connect, s1 ),
                        __string_with_length_hide_pwd( s1, 
                            conn_str_out ? conn_str_out : (SQLCHAR*)"NULL", SQL_NTS ));
        }

        dm_log_write( __FILE__, 
                __LINE__, 
                LOG_INFO, 
                LOG_INFO, 
                connection -> msg );
    }

    /*
     * If we specified FILEDSN or SAVEFILE these need adding to the
     * output string
     */

    if ( strlen( savefile ))
    {
	    char *str = strdup((char*) conn_str_out );
	    strcpy((char*) conn_str_out, "SAVEFILE=" );
	    strcat((char*) conn_str_out, savefile );
	    strcat((char*) conn_str_out, ";" );
	    strcat((char*) conn_str_out, str );
	    free( str );

	    if ( ptr_conn_str_out ) 
	    {
		    *ptr_conn_str_out = strlen((char*) conn_str_out );
	    }
    }

    if ( save_filedsn && strlen( save_filedsn ))
    {
	    char *str = strdup((char*) conn_str_out );
	    strcpy((char*) conn_str_out, "FILEDSN=" );
	    strcat((char*) conn_str_out, save_filedsn );
	    strcat((char*) conn_str_out, ";" );
	    strcat((char*) conn_str_out, str );
	    free( str );

	    if ( ptr_conn_str_out ) 
	    {
		    *ptr_conn_str_out = strlen((char*) conn_str_out );
	    }
    }

    if ( save_filedsn ) {
        free( save_filedsn );
    }

    /*
     * write the connection string out to a file
     */

    if ( tsavefile )
    {
        if ( SQL_SUCCEEDED( ret_from_connect ))
        {
            __parse_connection_string_ex( &con_struct,
                    (char*)conn_str_out, conn_str_out_max, 0 );

            /*
             * remove them
             */

            SQLWriteFileDSN( savefile, "ODBC", NULL, NULL );

            if ( con_struct.count )
            {
                int has_driver = 0;
                struct con_pair *cp;

                cp = con_struct.list;
                while( cp )
                {
                    if ( strcasecmp( cp -> keyword, "PWD" ) == 0 )
                    {
                        /*
                         * don't save this
                         */
                    	cp = cp -> next;
                        continue;
                    }
		    		else if ( strcasecmp( cp -> keyword, "SAVEFILE" ) == 0 )
                    {
                        /*
                         * or this
                         */
                    	cp = cp -> next;
                        continue;
                    }
                    else if ( strcasecmp( cp -> keyword, "DSN" ) == 0 )
                    {
                        /*
                         * don't save this either, there should be enough with the added DRIVER=
                         * to make it work
                         */
                    	cp = cp -> next;
                        continue;
                    }
                    else if ( strcasecmp( cp -> keyword, "DRIVER" ) == 0 )
                    {
                        has_driver = 1;
                    }
                    SQLWriteFileDSN( savefile, "ODBC", cp -> keyword, cp -> attribute );
                    cp = cp -> next;
                }

                if ( !has_driver )
                {
                    SQLWriteFileDSN( savefile, "ODBC", "Driver", driver_name );
                }
            }

            __release_conn( &con_struct );
        }
    }

    if ( warnings && ret_from_connect == SQL_SUCCESS )
    {
        ret_from_connect = SQL_SUCCESS_WITH_INFO;
    }

    if ( pooling_enabled && !add_to_pool( connection, pooh ) )
    {
        pool_unreserve( pooh );
    }

    return function_return_nodrv( SQL_HANDLE_DBC, connection, ret_from_connect );
}
