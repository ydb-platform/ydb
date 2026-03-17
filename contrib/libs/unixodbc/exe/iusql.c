/**************************************************
 * isql
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under GPL 18.FEB.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/

#include <config.h>
#include <ctype.h>
#define UNICODE
#include "isql.h"
#include "ini.h"
#include "sqlucode.h"
#ifdef HAVE_READLINE
    #include <readline/readline.h>
    #include <readline/history.h>
#endif
#ifdef HAVE_EDITLINE
    #include <editline/readline.h>
#endif

#ifdef HAVE_SETLOCALE
    #ifdef HAVE_LOCALE_H
        #include <locale.h>
    #endif 
#endif

static int OpenDatabase( SQLHENV *phEnv, SQLHDBC *phDbc, char *szDSN, char *szUID, char *szPWD );
static int CloseDatabase( SQLHENV hEnv, SQLHDBC hDbc );
static int ExecuteSQL( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable );
static int ExecuteHelp( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable );
static int ExecuteEcho( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable );

static void WriteHeaderHTMLTable( SQLHSTMT hStmt );
static void WriteHeaderDelimited( SQLHSTMT hStmt, char cDelimiter );
static void WriteBodyHTMLTable( SQLHSTMT hStmt );
static SQLLEN WriteBodyNormal( SQLHSTMT hStmt );
static void WriteBodyDelimited( SQLHSTMT hStmt, char cDelimiter );
static void WriteFooterHTMLTable( SQLHSTMT hStmt );

static int DumpODBCLog( SQLHENV hEnv, SQLHDBC hDbc, SQLHSTMT hStmt );


int     bVerbose                    = 0;
int     nUserWidth                  = 0;
SQLHENV hEnv                        = 0;
SQLHDBC hDbc                        = 0;
int     buseED                      = 0;

void UWriteHeaderNormal( SQLHSTMT hStmt, SQLTCHAR *szSepLine );
void UWriteFooterNormal( SQLHSTMT hStmt, SQLTCHAR *szSepLine, SQLLEN nRows );

static char * uc_to_ascii( SQLWCHAR *uc )
{
    char *ascii = (char *)uc;
    int i;

    for ( i = 0; uc[ i ]; i ++ )
    {
        ascii[ i ] = uc[ i ] & 0x00ff;
    }

    ascii[ i ] = 0;

    return ascii;
}

static void ansi_to_unicode( char *szSQL, SQLWCHAR *szUcSQL )
{
    int i;

    for ( i = 0; szSQL[ i ]; i ++ )
    {
        szUcSQL[ i ] = szSQL[ i ];
    }
    szUcSQL[ i ] = 0;
}

int main( int argc, char *argv[] )
{
    int     nArg, count;
    int     bHTMLTable                  = 0;
    int     bBatch                      = 0;
    int     cDelimiter                  = 0;
    int     bColumnNames                = 0;
    char    *szDSN;
    char    *szUID;
    char    *szPWD;
    char    *szSQL;
    char    *pEscapeChar;
    int     buffer_size = 9000;
    int     len;

    szDSN = NULL;
    szUID = NULL;
    szPWD = NULL;

    if ( argc < 2 )
    {
        fputs( szSyntax, stderr );
        exit( 1 );
    }

#ifdef HAVE_SETLOCALE
    /*
     * Default locale
     */
    setlocale( LC_ALL, "" );
#endif

    /****************************
     * PARSE ARGS
     ***************************/
    for ( nArg = 1, count = 1 ; nArg < argc; nArg++ )
    {
        if ( argv[nArg][0] == '-' )
        {
            /* Options */
            switch ( argv[nArg][1] )
            {
                case 'd':
                    cDelimiter = argv[nArg][2];
                    break;
                case 's':
                    buffer_size = atoi( &(argv[nArg][2]) );
                    break;
                case 'm':
                    nUserWidth = atoi( &(argv[nArg][2]) );
                    break;
                case 'w':
                    bHTMLTable = 1;
                    break;
                case 'b':
                    bBatch = 1;
                    break;
                case 'c':
                    bColumnNames = 1;
                    break;
                case 'v':
                    bVerbose = 1;
                    break;
                case 'e':
                    buseED = 1;
                    break;
                case '-':
                    printf( "unixODBC " VERSION "\n" );
                    exit(0);
#ifdef HAVE_STRTOL
                case 'x':
                    cDelimiter = strtol( argv[nArg]+2, NULL, 0 );
                    break;
#endif
#ifdef HAVE_SETLOCALE
                case 'l':
                    if ( !setlocale( LC_ALL, argv[nArg]+2 ))
                    {
                        fprintf( stderr, "isql: can't set locale to '%s'\n", argv[nArg]+2 );
                        exit ( -1 );
                    }
                    break;
#endif
                default:
                    fputs( szSyntax, stderr );
                    exit( 1 );
            }
            continue;
        }
        else if ( count == 1 ) {
            if ( nArg < argc ) {
                szDSN = argv[nArg];
            }
        }
        else if ( count == 2 ) {
            if ( nArg < argc ) {
                szUID = argv[nArg];
            }
        }
        else if ( count == 3 ) {
            if ( nArg < argc ) {
                szPWD = argv[nArg];
            }
        }
        count++;
    }

    szSQL = calloc( 1, buffer_size + 1 );

#ifdef HAVE_SETVBUF
    /* Ensure result lines are available to reader of whatever stdout */
    if (bBatch) {
        (void)setvbuf(stdout, NULL, _IOLBF, (size_t) 0);
    }
#endif

    /****************************
     * CONNECT
     ***************************/
    if ( !OpenDatabase( &hEnv, &hDbc, szDSN, szUID, szPWD ) )
        exit( 1 );

    /****************************
     * EXECUTE
     ***************************/
    if ( !bBatch )
    {
        printf( "+---------------------------------------+\n" );
        printf( "| Connected!                            |\n" );
        printf( "|                                       |\n" );
        printf( "| sql-statement                         |\n" );
        printf( "| help [tablename]                      |\n" );
        printf( "| echo [string]                         |\n" );
        printf( "| quit                                  |\n" );
        printf( "|                                       |\n" );
        printf( "+---------------------------------------+\n" );
    }
    do
    {
        if ( !bBatch )
#if !defined(HAVE_EDITLINE) && !defined(HAVE_READLINE)
            printf( "SQL> " );
#else
        {
            char *line;
            int malloced;

            line=readline("SQL> ");
            if ( !line )        /* EOF - ctrl D */
            {
                malloced = 1;
                line = strdup( "quit" );
            }
            else
            {
                malloced = 0;
            }
            strncpy(szSQL, line, buffer_size );
            add_history(line);
            if ( malloced )
            {
                free(line);
            }
        }
        else
#endif
        {
            char *line;
            int malloced;

            line = fgets( szSQL, buffer_size, stdin );
            if ( !line )        /* EOF - ctrl D */
            {
                malloced = 1;
                line = strdup( "quit" );
            }
            else
            {
                malloced = 0;
            }
            strncpy(szSQL, line, buffer_size );
            if ( malloced )
            {
                free(line);
            }
        }

        /* strip away escape chars */
        while ( (pEscapeChar=(char*)strchr(szSQL, '\n')) != NULL || (pEscapeChar=(char*)strchr(szSQL, '\r')) != NULL )
            *pEscapeChar = ' ';

        len = strlen( szSQL );

        /* remove trailing spaces */

        while( len > 0 ) 
        {
            len --;

            if ( szSQL[ len ] == ' ' ) 
            {
                szSQL[ len ] = '\0';
            }
            else 
            {
                break;
            }
        }

        if ( szSQL[1] != '\0' )
        {
            if ( strncmp( szSQL, "quit", 4 ) == 0 )
                szSQL[1] = '\0';
            else if ( strncmp( szSQL, "help", 4 ) == 0 )
                ExecuteHelp( hDbc, szSQL, cDelimiter, bColumnNames, bHTMLTable );
            else if ( strncmp( szSQL, "echo", 4 ) == 0 )
                ExecuteEcho( hDbc, szSQL, cDelimiter, bColumnNames, bHTMLTable );
            else if (memcmp(szSQL, "--", 2) != 0)
                ExecuteSQL( hDbc, szSQL, cDelimiter, bColumnNames, bHTMLTable );
        }

    } while ( szSQL[1] != '\0' );

    /****************************
     * DISCONNECT
     ***************************/
    CloseDatabase( hEnv, hDbc );

    exit( 0 );
}

/****************************
 * OptimalDisplayWidth
 ***************************/
static SQLUINTEGER
OptimalDisplayWidth( SQLHSTMT hStmt, SQLINTEGER nCol, int nUserWidth )
{
    SQLUINTEGER nLabelWidth                     = 10;
    SQLULEN nDataWidth                      = 10;
    SQLUINTEGER nOptimalDisplayWidth            = 10;
    SQLCHAR     szColumnName[MAX_DATA_WIDTH+1]; 

    *szColumnName = '\0';

    SQLColAttribute( hStmt, nCol, SQL_DESC_DISPLAY_SIZE, NULL, 0, NULL, (SQLLEN*)&nDataWidth );
    SQLColAttribute( hStmt, nCol, SQL_DESC_LABEL, szColumnName, sizeof(szColumnName), NULL, NULL );
    nLabelWidth = strlen((char*) szColumnName );

    /*
     * catch sqlserver var(max) types
     */

    if ( nDataWidth == 0 ) {
        nDataWidth = MAX_DATA_WIDTH;
    }

    nOptimalDisplayWidth = max( nLabelWidth, nDataWidth );

    if ( nUserWidth > 0 )
        nOptimalDisplayWidth = min( nOptimalDisplayWidth, nUserWidth );

    if ( nOptimalDisplayWidth > MAX_DATA_WIDTH )
        nOptimalDisplayWidth = MAX_DATA_WIDTH;

    return nOptimalDisplayWidth;
}

/****************************
 * OpenDatabase - do everything we have to do to get a viable connection to szDSN
 ***************************/
static int OpenDatabase( SQLHENV *phEnv, SQLHDBC *phDbc, char *szDSN, char *szUID, char *szPWD )
{
    SQLCHAR dsn[ 1024 ], uid[ 1024 ], pwd[ 1024 ];
    SQLTCHAR cstr[ 1024 ];
    char zcstr[ 1024 * 2 ], tmp[ 1024 * 8 ];
    int i;
    size_t zclen;

    if ( SQLAllocEnv( phEnv ) != SQL_SUCCESS )
    {
        fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocEnv\n" );
        return 0;
    }

    if ( SQLAllocConnect( *phEnv, phDbc ) != SQL_SUCCESS )
    {
        if ( bVerbose ) DumpODBCLog( hEnv, 0, 0 );
        fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocConnect\n" );
        SQLFreeEnv( *phEnv );
        return 0;
    }

    if ( szDSN )
    {
        size_t DSNlen=strlen( szDSN );
        for ( i = 0; i < DSNlen && i < sizeof( dsn ) - 1; i ++ )
        {
            dsn[ i ] = szDSN[ i ];
        }
        dsn[ i ] = '\0';
    }
    else
    {
        dsn[ 0 ] = '\0';
    }

    if ( szUID )
    {
        size_t UIDlen=strlen( szUID );
        for ( i = 0; i < UIDlen && i < sizeof( uid ) - 1; i ++ )
        {
            uid[ i ] = szUID[ i ];
        }
        uid[ i ] = '\0';
    }
    else
    {
        uid[ 0 ] = '\0';
    }

    if ( szPWD )
    {
        size_t PWDlen=strlen( szPWD );
        for ( i = 0; i < PWDlen && i < sizeof( pwd ) - 1; i ++ )
        {
            pwd[ i ] = szPWD[ i ];
        }
        pwd[ i ] = '\0';
    }
    else
    {
        pwd[ 0 ] = '\0';
    }

    /*
     * Allow passing in a entire connect string in the first arg
     *
     * isql "DSN={Dsn Name};PWD={Pass world};UID={User Name}"
     */

    if ( !szPWD && !szUID && ( strstr( dsn, "DSN=" ) || strstr( dsn, "DRIVER=" ) || strstr( dsn, "FILEDSN=" ))) 
    {
        strcpy( zcstr, dsn );
    }
    else 
    {
        sprintf( zcstr, "DSN=%s", dsn );
        if ( szUID )
        {
            sprintf( tmp, ";UID=%s", uid );
            strcat( zcstr, tmp );
        }
        if ( szPWD )
        {
            sprintf( tmp, ";PWD=%s", pwd );
            strcat( zcstr, tmp );
        }
    }

    zclen=strlen( zcstr );
    for ( i = 0; i < zclen; i ++ )
    {
        cstr[ i ] = zcstr[ i ];
    }
    cstr[ i ] = 0;

    if ( !SQL_SUCCEEDED( SQLDriverConnect( *phDbc, NULL, cstr, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT  )))
    {
        if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
        fprintf( stderr, "[ISQL]ERROR: Could not SQLDriverConnect\n" );
        SQLFreeConnect( *phDbc );
        SQLFreeEnv( *phEnv );
        return 0;
    }
    if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );

    return 1;
}

/****************************
 * ExecuteSQL - create a statement, execute the SQL, and get rid of the statement
 *            - show results as per request; bHTMLTable has precedence over other options
 ***************************/
static int ExecuteSQL( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable )
{
    SQLHSTMT        hStmt;
    SQLTCHAR        szSepLine[32001];   
    SQLTCHAR        szUcSQL[32001]; 
    SQLSMALLINT     cols;
    SQLINTEGER      ret;
    SQLLEN          nRows                   = 0;

    szSepLine[ 0 ] = 0;

    ansi_to_unicode( szSQL, szUcSQL );

    /****************************
     * EXECUTE SQL
     ***************************/
    if ( SQLAllocStmt( hDbc, &hStmt ) != SQL_SUCCESS )
    {
        if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
        fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocStmt\n" );
        return 0;
    }

    if ( buseED ) {
        ret = SQLExecDirect( hStmt, szUcSQL, SQL_NTS );

        if ( ret == SQL_NO_DATA )
        {
            fprintf( stderr, "[ISQL]INFO: SQLExecDirect returned SQL_NO_DATA\n" );
        }
        else if ( ret == SQL_SUCCESS_WITH_INFO )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]INFO: SQLExecDirect returned SQL_SUCCESS_WITH_INFO\n" );
        }
        else if ( ret != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLExecDirect\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            return 0;
        }
    }
    else {
        if ( SQLPrepare( hStmt, szUcSQL, SQL_NTS ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLPrepare\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            return 0;
        }
    
        ret =  SQLExecute( hStmt );
    
        if ( ret == SQL_NO_DATA )
        {
            fprintf( stderr, "[ISQL]INFO: SQLExecute returned SQL_NO_DATA\n" );
        }
        else if ( ret == SQL_SUCCESS_WITH_INFO )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]INFO: SQLExecute returned SQL_SUCCESS_WITH_INFO\n" );
        }
        else if ( ret != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLExecute\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            return 0;
        }
    }

    do 
    {
        /*
         * check to see if it has generated a result set
         */

        if ( SQLNumResultCols( hStmt, &cols ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLNumResultCols\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            return 0;
        }

        if ( cols > 0 )
        {
            /****************************
             * WRITE HEADER
             ***************************/
            if ( bHTMLTable )
                WriteHeaderHTMLTable( hStmt );
            else if ( cDelimiter == 0 )
                UWriteHeaderNormal( hStmt, szSepLine );
            else if ( cDelimiter && bColumnNames )
                WriteHeaderDelimited( hStmt, cDelimiter );

            /****************************
             * WRITE BODY
             ***************************/
            if ( bHTMLTable )
                WriteBodyHTMLTable( hStmt );
            else if ( cDelimiter == 0 )
                nRows = WriteBodyNormal( hStmt );
            else
                WriteBodyDelimited( hStmt, cDelimiter );
        }

        /****************************
         * WRITE FOOTER
         ***************************/
        if ( bHTMLTable )
            WriteFooterHTMLTable( hStmt );
        else if ( cDelimiter == 0 )
            UWriteFooterNormal( hStmt, szSepLine, nRows );
    }
    while ( SQL_SUCCEEDED( SQLMoreResults( hStmt )));

    /****************************
     * CLEANUP
     ***************************/
    SQLFreeStmt( hStmt, SQL_DROP );

    return 1;
}

/****************************
 * ExecuteHelp - create a statement, execute the SQL, and get rid of the statement
 *             - show results as per request; bHTMLTable has precedence over other options
 ***************************/
static int ExecuteHelp( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable )
{
    char            szTable[250]                        = "";
    SQLHSTMT        hStmt;
    SQLTCHAR        szSepLine[32001];   
    SQLLEN          nRows               = 0;

    szSepLine[ 0 ] = 0;

    /****************************
     * EXECUTE SQL
     ***************************/
    if ( SQLAllocStmt( hDbc, &hStmt ) != SQL_SUCCESS )
    {
        if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
        fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocStmt\n" );
        return 0;
    }

    if ( iniElement( szSQL, ' ', '\0', 1, szTable, sizeof(szTable) ) == INI_SUCCESS )
    {
        SQLWCHAR tname[ 1024 ];

        ansi_to_unicode( szTable, tname );
        /* COLUMNS */
        if ( SQLColumns( hStmt, NULL, 0, NULL, 0, tname, SQL_NTS, NULL, 0 ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLColumns\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            return 0;
        }
    }
    else
    {
        /* TABLES */
        if ( SQLTables( hStmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0 ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLTables\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            return 0;
        }
    }

    /****************************
     * WRITE HEADER
     ***************************/
    if ( bHTMLTable )
        WriteHeaderHTMLTable( hStmt );
    else if ( cDelimiter == 0 )
        UWriteHeaderNormal( hStmt, szSepLine );
    else if ( cDelimiter && bColumnNames )
        WriteHeaderDelimited( hStmt, cDelimiter );

    /****************************
     * WRITE BODY
     ***************************/
    if ( bHTMLTable )
        WriteBodyHTMLTable( hStmt );
    else if ( cDelimiter == 0 )
        nRows = WriteBodyNormal( hStmt );
    else
        WriteBodyDelimited( hStmt, cDelimiter );

    /****************************
     * WRITE FOOTER
     ***************************/
    if ( bHTMLTable )
        WriteFooterHTMLTable( hStmt );
    else if ( cDelimiter == 0 )
        UWriteFooterNormal( hStmt, szSepLine, nRows );

    /****************************
     * CLEANUP
     ***************************/
    SQLFreeStmt( hStmt, SQL_DROP );

    return 1;
}

/****************************
 * ExecuteEcho - simply write as is the string (if any) to stdout
 ***************************/
static int ExecuteEcho( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable )
{
    char *p;

    for ( p = szSQL+4; *p != '\0' && isspace((int)*p) ; ++p )
        ;
    if ( *p != '\0' && p == szSQL+4 ) {
        fprintf( stderr, "[ISQL]ERROR: incorrect echo call\n" );
        return 0;
    }

    (void)printf( "%s\n", p );

    return 1;
}

/****************************
 * CloseDatabase - cleanup in prep for exiting the program
 ***************************/
int CloseDatabase( SQLHENV hEnv, SQLHDBC hDbc )
{
    SQLDisconnect( hDbc );
    SQLFreeConnect( hDbc );
    SQLFreeEnv( hEnv );

    return 1;
}


/****************************
 * WRITE HTML
 ***************************/
static void WriteHeaderHTMLTable( SQLHSTMT hStmt )
{
    SQLINTEGER      nCol                            = 0;
    SQLSMALLINT     nColumns                        = 0;
    SQLTCHAR        szColumnName[MAX_DATA_WIDTH+1]; 

    szColumnName[ 0 ] = 0;

    printf( "<table BORDER>\n" );
    printf( "<tr BGCOLOR=#000099>\n" );

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    for ( nCol = 1; nCol <= nColumns; nCol++ )
    {
        SQLColAttribute( hStmt, nCol, SQL_DESC_LABEL, szColumnName, sizeof(szColumnName), NULL, NULL );

        printf( "<td>\n" );
        printf( "<font face=Arial,Helvetica><font color=#FFFFFF>\n" );
        printf( "%s\n", uc_to_ascii( szColumnName ));
        printf( "</font></font>\n" );
        printf( "</td>\n" );
    }
    printf( "</tr>\n" );
}

static void WriteBodyHTMLTable( SQLHSTMT hStmt )
{
    SQLINTEGER      nCol                            = 0;
    SQLSMALLINT     nColumns                        = 0;
    SQLLEN          nIndicator                      = 0;
    SQLTCHAR        szColumnValue[MAX_DATA_WIDTH+1];
    SQLRETURN       nReturn                         = 0;
    SQLRETURN       ret;

    szColumnValue[ 0 ]  = 0;

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    while ( (ret = SQLFetch( hStmt )) == SQL_SUCCESS ) /* ROWS */
    {
        printf( "<tr>\n" );

        for ( nCol = 1; nCol <= nColumns; nCol++ ) /* COLS */
        {
            printf( "<td>\n" );
            printf( "<font face=Arial,Helvetica>\n" );

            nReturn = SQLGetData( hStmt, nCol, SQL_C_WCHAR, (SQLPOINTER)szColumnValue, sizeof(szColumnValue), &nIndicator );
            if ( nReturn == SQL_SUCCESS && nIndicator != SQL_NULL_DATA )
            {
                uc_to_ascii( szColumnValue );
                fputs((char*) szColumnValue, stdout );
            }
            else if ( nReturn == SQL_ERROR )
            {
                ret = SQL_ERROR;
                break;
            }
            else
                printf( "%s\n", "" );

            printf( "</font>\n" );
            printf( "</td>\n" );
        }
        if (ret != SQL_SUCCESS)
            break;
        printf( "</tr>\n" );
    }
}

static void WriteFooterHTMLTable( SQLHSTMT hStmt )
{
    printf( "</table>\n" );
}

/****************************
 * WRITE DELIMITED
 * - this output can be used by the ODBC Text File driver
 * - last column no longer has a delimit char (it is implicit)...
 *   this is consistent with odbctxt
 ***************************/
static void WriteHeaderDelimited( SQLHSTMT hStmt, char cDelimiter )
{
    SQLINTEGER      nCol                            = 0;
    SQLSMALLINT     nColumns                        = 0;
    SQLTCHAR            szColumnName[MAX_DATA_WIDTH+1]; 

    szColumnName[ 0 ]   = 0;    

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    for ( nCol = 1; nCol <= nColumns; nCol++ )
    {
        SQLColAttribute( hStmt, nCol, SQL_DESC_LABEL, szColumnName, sizeof(szColumnName), NULL, NULL );
        fputs((char*) uc_to_ascii( szColumnName ), stdout );
        if ( nCol < nColumns )
            putchar( cDelimiter );
    }
    putchar( '\n' );
}

static void WriteBodyDelimited( SQLHSTMT hStmt, char cDelimiter )
{
    SQLINTEGER      nCol                            = 0;
    SQLSMALLINT     nColumns                        = 0;
    SQLLEN          nIndicator                      = 0;
    SQLTCHAR            szColumnValue[MAX_DATA_WIDTH+1];
    SQLRETURN       nReturn                         = 0;
    SQLRETURN       ret;

    szColumnValue[ 0 ]  = 0;

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    /* ROWS */
    while (( ret = SQLFetch( hStmt )) == SQL_SUCCESS )
    {
        /* COLS */
        for ( nCol = 1; nCol <= nColumns; nCol++ )
        {
            nReturn = SQLGetData( hStmt, nCol, SQL_C_WCHAR, (SQLPOINTER)szColumnValue, sizeof(szColumnValue), &nIndicator );
            if ( nReturn == SQL_SUCCESS && nIndicator != SQL_NULL_DATA )
            {
                uc_to_ascii( szColumnValue );
                fputs((char*) szColumnValue, stdout );
                if ( nCol < nColumns )
                    putchar( cDelimiter );
            }
            else if ( nReturn == SQL_ERROR )
            {
                ret = SQL_ERROR;
                break;
            }
            else
            {
                if ( nCol < nColumns )
                    putchar( cDelimiter );
            }
        }
        if (ret != SQL_SUCCESS)
            break;
        printf( "\n" );
    }
    if ( ret == SQL_ERROR )
    {
        if ( bVerbose ) DumpODBCLog( 0, 0, hStmt );
    }
}

/****************************
 * WRITE NORMAL
 ***************************/
void UWriteHeaderNormal( SQLHSTMT hStmt, SQLTCHAR *szSepLine )
{
    SQLINTEGER      nCol                            = 0;
    SQLSMALLINT     nColumns                        = 0;
    SQLTCHAR            szColumn[MAX_DATA_WIDTH+20];    
    SQLTCHAR            szColumnName[MAX_DATA_WIDTH+1]; 
    SQLTCHAR            szHdrLine[32001];   
    SQLUINTEGER     nOptimalDisplayWidth            = 10;

    szColumn[ 0 ]       = 0;    
    szColumnName[ 0 ]   = 0;    
    szHdrLine[ 0 ]      = 0;    

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    for ( nCol = 1; nCol <= nColumns; nCol++ )
    {
        nOptimalDisplayWidth = OptimalDisplayWidth( hStmt, nCol, nUserWidth );
        SQLColAttribute( hStmt, nCol, SQL_DESC_LABEL, szColumnName, sizeof(szColumnName), NULL, NULL );
        if ( nOptimalDisplayWidth > MAX_DATA_WIDTH ) nOptimalDisplayWidth = MAX_DATA_WIDTH;

        uc_to_ascii( szColumnName );

        /* SEP */
        memset( szColumn, '\0', sizeof(szColumn) );
        memset( szColumn, '-', max( nOptimalDisplayWidth, strlen((char*)szColumnName) ) + 1 );
        strcat((char*) szSepLine, "+" );
        strcat((char*) szSepLine,(char*) szColumn );

        /* HDR */
        sprintf((char*) szColumn, "| %-*s", (int)max( nOptimalDisplayWidth, strlen((char*)szColumnName) ), (char*)szColumnName );
        strcat((char*) szHdrLine,(char*) szColumn );
    }
    strcat((char*) szSepLine, "+\n" );
    strcat((char*) szHdrLine, "|\n" );

    puts((char*) szSepLine );
    puts((char*) szHdrLine );
    puts((char*) szSepLine );
}

static SQLLEN WriteBodyNormal( SQLHSTMT hStmt )
{
    SQLINTEGER      nCol                            = 0;
    SQLSMALLINT     nColumns                        = 0;
    SQLLEN          nIndicator                      = 0;
    SQLTCHAR        szColumn[MAX_DATA_WIDTH+20];
    SQLTCHAR        szColumnValue[MAX_DATA_WIDTH+1];
    SQLTCHAR        szColumnName[MAX_DATA_WIDTH+1]; 
    SQLRETURN       nReturn                         = 0;
    SQLRETURN       ret;
    SQLLEN          nRows                           = 0;
    SQLUINTEGER     nOptimalDisplayWidth            = 10;

    szColumn[ 0 ]       = 0;
    szColumnValue[ 0 ]  = 0;
    szColumnName[ 0 ]   = 0;    

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    /* ROWS */
    while (( ret = SQLFetch( hStmt )) == SQL_SUCCESS )
    {
        /* COLS */
        for ( nCol = 1; nCol <= nColumns; nCol++ )
        {
            SQLColAttribute( hStmt, nCol, SQL_DESC_LABEL, szColumnName, sizeof(szColumnName), NULL, NULL );
            nOptimalDisplayWidth = OptimalDisplayWidth( hStmt, nCol, nUserWidth );

            uc_to_ascii( szColumnName );

            if ( nOptimalDisplayWidth > MAX_DATA_WIDTH ) nOptimalDisplayWidth = MAX_DATA_WIDTH;
            nReturn = SQLGetData( hStmt, nCol, SQL_C_WCHAR, (SQLPOINTER)szColumnValue, sizeof(szColumnValue), &nIndicator );
            szColumnValue[MAX_DATA_WIDTH] = '\0';
            uc_to_ascii( szColumnValue );

            if ( nReturn == SQL_SUCCESS && nIndicator != SQL_NULL_DATA )
            {
                if ( strlen((char*)szColumnValue) < max( nOptimalDisplayWidth, strlen((char*)szColumnName )))
                {
                    int i;
                    size_t maxlen=max( nOptimalDisplayWidth, strlen((char*)szColumnName ));
                    strcpy((char*) szColumn, "| " );
                    strcat((char*) szColumn, (char*) szColumnValue );

                    for ( i = strlen((char*) szColumnValue ); i < maxlen; i ++ )
                    {
                        strcat((char*) szColumn, " " );
                    }
                }
                else
                {
                    strcpy((char*) szColumn, "| " );
                    strcat((char*) szColumn, (char*) szColumnValue );
                }
            }
            else if ( nReturn == SQL_ERROR )
            {
                ret = SQL_ERROR;
                break;
            }
            else
            {
                sprintf((char*)  szColumn, "| %-*s", (int)max( nOptimalDisplayWidth, strlen((char*) szColumnName) ), "" );
            }
            fputs((char*)  szColumn, stdout );
        }
        if (ret != SQL_SUCCESS)
            break;
        printf( "|\n" );
        nRows++;
    } 
    if ( ret == SQL_ERROR )
    {
        if ( bVerbose ) DumpODBCLog( 0, 0, hStmt );
    }

    return nRows;
}

void UWriteFooterNormal( SQLHSTMT hStmt, SQLTCHAR   *szSepLine, SQLLEN nRows )
{
    SQLLEN  nRowsAffected   = -1;

    puts( (char*)szSepLine );

    SQLRowCount( hStmt, &nRowsAffected );
    printf( "SQLRowCount returns %ld\n", nRowsAffected );

    if ( nRows )
    {
        printf( "%ld rows fetched\n", nRows );
    }
}



static int DumpODBCLog( SQLHENV hEnv, SQLHDBC hDbc, SQLHSTMT hStmt )
{
    SQLTCHAR        szError[501];
    SQLTCHAR        szSqlState[10];
    SQLINTEGER  nNativeError;
    SQLSMALLINT nErrorMsg;

    if ( hStmt )
    {
        while ( SQLError( hEnv, hDbc, hStmt, szSqlState, &nNativeError, szError, 500, &nErrorMsg ) == SQL_SUCCESS )
        {
            printf( "%s\n", uc_to_ascii( szError ));
        }
    }

    if ( hDbc )
    {
        while ( SQLError( hEnv, hDbc, 0, szSqlState, &nNativeError, szError, 500, &nErrorMsg ) == SQL_SUCCESS )
        {
            printf( "%s\n", uc_to_ascii( szError ));
        }
    }

    if ( hEnv )
    {
        while ( SQLError( hEnv, 0, 0, szSqlState, &nNativeError, szError, 500, &nErrorMsg ) == SQL_SUCCESS )
        {
            printf( "%s\n", uc_to_ascii( szError ));
        }
    }

    return 1;
}

