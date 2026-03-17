#include <stdio.h>
#include <stdlib.h>
#include <sql.h>
#include <sqlext.h>

static void show_error( SQLHANDLE henv, SQLHANDLE hdbc, SQLHANDLE hstmt, char *func )
{
    SQLSMALLINT len;
    SQLCHAR state[ 7 ];
    SQLCHAR msg[ 512 ];
    SQLINTEGER native;
    SQLRETURN ret;
    SQLSMALLINT rec;

    fprintf( stderr, "slencheck: error from driver calling %s\n", func );

    rec = 0;

    while( 1 ) {
        rec ++;

        if ( hstmt ) {
            ret = SQLGetDiagRec( SQL_HANDLE_STMT, hstmt, rec, state, &native, msg, sizeof( msg ), &len );
        }
        else if ( hdbc ) {
            ret = SQLGetDiagRec( SQL_HANDLE_DBC, hdbc, rec, state, &native, msg, sizeof( msg ), &len );
        }
        else {
            ret = SQLGetDiagRec( SQL_HANDLE_ENV, henv, rec, state, &native, msg, sizeof( msg ), &len );
        }

        if ( SQL_SUCCEEDED( ret )) {
            fprintf( stderr, "\t[%s]:%d:%s\n", state, (int)native, msg );
        }
        else {
            break;
        }
    }
}

int main( int argc, char **argv )
{
    SQLHANDLE henv, hdbc, hstmt;
    SQLRETURN ret;
    unsigned char mem[ 8 ];
    int size;

    if ( argc < 2 || argc > 4 ) {
        fprintf( stderr, "usage: slencheck dsn [uid [pwd]]\n" );
        exit( -1 );
    }

    ret = SQLAllocHandle( SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv );
    if ( ret != SQL_SUCCESS ) {
        fprintf( stderr, "slencheck: failed to call SQLAllocHandle(ENV)\n" );
        exit( -1 );
    }

    ret = SQLSetEnvAttr( henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER );
    if ( ret != SQL_SUCCESS ) {
        show_error( henv, NULL, NULL, "SQLSetEnvAttr" );
        exit( -1 );
    }

    ret = SQLAllocHandle( SQL_HANDLE_DBC, henv, &hdbc );
    if ( ret != SQL_SUCCESS ) {
        show_error( henv, NULL, NULL, "SQLAllocHandle" );
        exit( -1 );
    }

    if ( argc == 2 ) {
        ret = SQLConnect( hdbc, (SQLCHAR*) argv[ 1 ], SQL_NTS, NULL, 0, NULL, 0 );
    }
    else if ( argc == 3 ) {
        ret = SQLConnect( hdbc, (SQLCHAR*) argv[ 1 ], SQL_NTS, (SQLCHAR*) argv[ 2 ], SQL_NTS, NULL, 0 );
    }
    else {
        ret = SQLConnect( hdbc, (SQLCHAR*) argv[ 1 ], SQL_NTS, (SQLCHAR*) argv[ 2 ], SQL_NTS, (SQLCHAR*) argv[ 3 ], SQL_NTS );
    }

    if ( !SQL_SUCCEEDED( ret )) {
        show_error( henv, hdbc, NULL, "SQLConnect" );
        exit( -1 );
    }

    ret = SQLAllocHandle( SQL_HANDLE_STMT, hdbc, &hstmt );
    if ( !SQL_SUCCEEDED( ret )) {
        show_error( henv, hdbc, NULL, "SQLAllocHandle" );
        exit( -1 );
    }

    ret = SQLTables( hstmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0 );
    if ( !SQL_SUCCEEDED( ret )) {
        show_error( henv, hdbc, hstmt, "SQLTables" );
        exit( -1 );
    }

    mem[ 0 ] = 0xde;
    mem[ 1 ] = 0xad;
    mem[ 2 ] = 0xbe;
    mem[ 3 ] = 0xef;
    mem[ 4 ] = 0xde;
    mem[ 5 ] = 0xad;
    mem[ 6 ] = 0xbe;
    mem[ 7 ] = 0xef;

    ret = SQLRowCount( hstmt, (SQLLEN*) mem );
    if ( !SQL_SUCCEEDED( ret )) {
        show_error( henv, hdbc, hstmt, "SQLRowCount" );
        exit( -1 );
    }

    if ( mem[ 7 ] != 0xef && mem[ 6 ] != 0xbe ) {
        size = 8;
    }
    else if ( mem[ 7 ] == 0xef && mem[ 6 ] == 0xbe && mem[ 3 ] != 0xef && mem[ 2 ] != 0xbe ) {
        size = 4;
    }
    else {
        size = 0;
    }

    if ( size ) {
        printf( "slencheck: sizeof(SQLLEN) == %d\n", size );
        if ( size == sizeof( SQLLEN )) {
            printf( "slencheck: driver manager and driver matched\n" );
        }
        else {
            printf( "slencheck: driver manager and driver differ!!!\n" );
        }
    }
    else {
        printf( "slencheck: can't decide on sizeof(SQLLEN)\n" );
    }

    ret = SQLCloseCursor( hstmt );
    ret = SQLFreeHandle( SQL_HANDLE_STMT, hstmt );
    ret = SQLDisconnect( hdbc );
    ret = SQLFreeHandle( SQL_HANDLE_DBC, hdbc );
    ret = SQLFreeHandle( SQL_HANDLE_ENV, henv );

    return size;
}
