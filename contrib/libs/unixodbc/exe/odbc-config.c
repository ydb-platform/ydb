/*********************************************************************
 *
 * Written, as part of unixODBC by Nick Gorham
 * (nick@lurcher.org).
 *
 * copyright (c) 2004 Nick Gorham
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 **********************************************************************/

#include <config.h>
#include <stdio.h>

#ifdef HAVE_STDLIB_H
#include <stdlib.h>
#endif

#ifdef HAVE_STRING_H
#include <string.h>
#endif

#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <sql.h>

static void usage( void )
{
        fprintf( stderr, "Usage: odbc_config\n\t\t[--prefix]\n\t\t[--exec-prefix]\n\t\t[--include-prefix]\n\t\t[--lib-prefix]\n\t\t[--bin-prefix]\n\t\t[--version]\n\t\t[--libs]\n\t\t[--static-libs]\n\t\t[--libtool-libs]\n\t\t[--cflags]\n\t\t[--odbcversion]\n\t\t[--longodbcversion]\n\t\t[--odbcini]\n\t\t[--odbcinstini]\n\t\t[--header]\n\t\t[--ulen]\n" );

}

static void cInc( void )
{
#ifdef HAVE_UNISTD_H
    printf( "#ifndef HAVE_UNISTD_H\n #define HAVE_UNISTD_H\n#endif\n" );
#endif

#ifdef HAVE_PWD_H
    printf( "#ifndef HAVE_PWD_H\n #define HAVE_PWD_H\n#endif\n" );
#endif

#ifdef HAVE_SYS_TYPES_H
    printf( "#ifndef HAVE_SYS_TYPES_H\n #define HAVE_SYS_TYPES_H\n#endif\n" );
#endif

#ifdef ODBC_STD
    printf( "#ifndef ODBC_STD\n #define ODBC_STD\n#endif\n" );
#endif

#ifdef UNICODE
    printf( "#ifndef UNICODE\n #define UNICODE\n#endif\n" );
#endif

#ifdef GUID_DEFINED
    printf( "#ifndef GUID_DEFINED\n #define GUID_DEFINED\n#endif\n" );
#endif

#ifdef SQL_WCHART_CONVERT
    printf( "#ifndef SQL_WCHART_CONVERT\n #define SQL_WCHART_CONVERT\n#endif\n" );
#endif

#ifdef HAVE_LONG_LONG
    printf( "#ifndef HAVE_LONG_LONG\n #define HAVE_LONG_LONG\n#endif\n" );
#endif

#ifdef ODBCINT64_TYPE
    printf( "#ifndef ODBCINT64\n #define ODBCINT64 %s\n#endif\n", ODBCINT64_TYPE );
#endif

#ifdef UODBCINT64_TYPE
    printf( "#ifndef UODBCINT64\n #define UODBCINT64 %s\n#endif\n", UODBCINT64_TYPE );
#endif

#ifdef DISABLE_INI_CACHING
    printf( "#ifndef DISABLE_INI_CACHING\n #define DISABLE_INI_CACHING\n#endif\n" );
#endif

#ifdef SIZEOF_LONG_INT
    printf( "#ifndef SIZEOF_LONG_INT\n #define SIZEOF_LONG_INT %d\n#endif\n", SIZEOF_LONG_INT );
#endif

#ifdef ALREADY_HAVE_WINDOWS_TYPE
    printf( "#ifndef ALREADY_HAVE_WINDOWS_TYPE\n #define ALREADY_HAVE_WINDOWS_TYPE\n#endif\n" );
#endif

#ifdef DONT_TD_VOID
    printf( "#ifndef DONT_TD_VOID\n #define DONT_TD_VOID\n#endif\n" );
#endif

#ifdef DO_YOU_KNOW_WHAT_YOU_ARE_DOING
    printf( "#ifndef DO_YOU_KNOW_WHAT_YOU_ARE_DOING\n #define DO_YOU_KNOW_WHAT_YOU_ARE_DOING\n#endif\n" );
#endif

#ifdef BUILD_LEGACY_64_BIT_MODE
    printf( "#ifndef BUILD_LEGACY_64_BIT_MODE\n #define BUILD_LEGACY_64_BIT_MODE\n#endif\n" );
#endif

#ifdef HAVE_ICONV
    printf( "#ifndef HAVE_ICONV\n #define HAVE_ICONV\n#endif\n" );
#endif

#ifdef ASCII_ENCODING
    printf( "#ifndef ASCII_ENCODING\n #define ASCII_ENCODING \"%s\"\n#endif\n", ASCII_ENCODING );
#endif

#ifdef UNICODE_ENCODING
    printf( "#ifndef UNICODE_ENCODING\n #define UNICODE_ENCODING \"%s\"\n#endif\n", UNICODE_ENCODING );
#endif

#ifdef ENABLE_DRIVER_ICONV
    printf( "#ifndef ENABLE_DRIVER_ICONV\n #define ENABLE_DRIVER_ICONV\n#endif\n" );
#endif
}

static void cflags( void )
{
#ifdef HAVE_UNISTD_H
    printf( "-DHAVE_UNISTD_H " );
#endif

#ifdef HAVE_PWD_H
    printf( "-DHAVE_PWD_H " );
#endif

#ifdef HAVE_SYS_TYPES_H
    printf( "-DHAVE_SYS_TYPES_H " );
#endif

#ifdef ODBC_STD
    printf( "-DODBC_STD " );
#endif

#ifdef UNICODE
    printf( "-DUNICODE " );
#endif

#ifdef GUID_DEFINED
    printf( "-DGUID_DEFINED " );
#endif

#ifdef SQL_WCHART_CONVERT
    printf( "-DSQL_WCHART_CONVERT " );
#endif

#ifdef HAVE_LONG_LONG
    printf( "-DHAVE_LONG_LONG " );
#endif

#ifdef DISABLE_INI_CACHING
    printf( "-DDISABLE_INI_CACHING " );
#endif

#ifdef SIZEOF_LONG_INT
    printf( "-DSIZEOF_LONG_INT=%d ", SIZEOF_LONG_INT );
#endif

#ifdef ALREADY_HAVE_WINDOWS_TYPE
    printf( "-DALREADY_HAVE_WINDOWS_TYPE " );
#endif

#ifdef DONT_TD_VOID
    printf( "-DDONT_TD_VOID " );
#endif

#ifdef DO_YOU_KNOW_WHAT_YOU_ARE_DOING
    printf( "-DDO_YOU_KNOW_WHAT_YOU_ARE_DOING " );
#endif

#ifdef BUILD_LEGACY_64_BIT_MODE
    printf( "-DBUILD_LEGACY_64_BIT_MODE " );
#endif

#ifdef INCLUDE_PREFIX
	printf( "-I%s ", INCLUDE_PREFIX );
#else
	printf( "-I%s/include ", PREFIX );
#endif

    printf( "\n" );
}

static void ulen( void )
{
	printf( "-DSIZEOF_SQLULEN=%d\n", (int)sizeof( SQLULEN ));
}

int main( int argc, char **argv )
{
    int i;

    if ( argc < 2 )
    {
        usage();
        exit( -1 );

    }

    for ( i = 1; i < argc; i ++ )
    {
        if ( strcmp( argv[ i ], "--prefix" ) == 0 )
        {
            printf( "%s\n", PREFIX );
        }
        else if ( strcmp( argv[ i ], "--exec-prefix" ) == 0 )
        {
            printf( "%s\n", EXEC_PREFIX );
        }
        else if ( strcmp( argv[ i ], "--bin-prefix" ) == 0 )
        {
            printf( "%s\n", BIN_PREFIX );
        }
        else if ( strcmp( argv[ i ], "--include-prefix" ) == 0 )
        {
            printf( "%s\n", INCLUDE_PREFIX );
        }
        else if ( strcmp( argv[ i ], "--lib-prefix" ) == 0 )
        {
            printf( "%s\n", LIB_PREFIX );
        }
        else if ( strcmp( argv[ i ], "--version" ) == 0 )
        {
            printf( "%s\n", VERSION );
        }
        else if ( strcmp( argv[ i ], "--libs" ) == 0 )
        {
            printf( "-L%s -lodbc\n", LIB_PREFIX );
        }
        else if ( strcmp( argv[ i ], "--static-libs" ) == 0 )
        {
            printf( "%s/libodbc.a\n", LIB_PREFIX );
        }
        else if ( strcmp( argv[ i ], "--libtool-libs" ) == 0 )
        {
            printf( "%s/libodbc.la\n", LIB_PREFIX );
        }
        else if ( strcmp( argv[ i ], "--cflags" ) == 0 )
        {
            cflags();
        }
        else if ( strcmp( argv[ i ], "--header" ) == 0 )
        {
            cInc();
        }
        else if ( strcmp( argv[ i ], "--odbcversion" ) == 0 )
        {
            printf( "3\n" );
        }
        else if ( strcmp( argv[ i ], "--longodbcversion" ) == 0 )
        {
            printf( "3.52\n" );
        }
        else if ( strcmp( argv[ i ], "--odbcini" ) == 0 )
        {
            printf( "%s/odbc.ini\n", SYSTEM_FILE_PATH );
        }
        else if ( strcmp( argv[ i ], "--odbcinstini" ) == 0 )
        {
            printf( "%s/odbcinst.ini\n", SYSTEM_FILE_PATH );
        }
        else if ( strcmp( argv[ i ], "--ulen" ) == 0 )
        {
            ulen();
        }
        else
        {
            usage();
            exit( -1 );
        }
    }

	exit(0);
}
