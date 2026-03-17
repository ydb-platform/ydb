
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
#include "isql.h"
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
static int ExecuteSQL( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable );
static int ExecuteHelp( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable );
static int ExecuteSlash( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable );
static int ExecuteEcho( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable );
static int	CloseDatabase( SQLHENV hEnv, SQLHDBC hDbc );

static void WriteHeaderHTMLTable( SQLHSTMT hStmt );
static void WriteHeaderNormal( SQLHSTMT hStmt, SQLCHAR	**szSepLine );
static void WriteHeaderDelimited( SQLHSTMT hStmt, char cDelimiter );
static void WriteBodyHTMLTable( SQLHSTMT hStmt );
static SQLLEN WriteBodyNormal( SQLHSTMT hStmt );
static void WriteBodyDelimited( SQLHSTMT hStmt, char cDelimiter );
static void WriteFooterHTMLTable( SQLHSTMT hStmt );
static void WriteFooterNormal( SQLHSTMT hStmt, SQLCHAR	*szSepLine, SQLLEN nRows );

static int DumpODBCLog( SQLHENV hEnv, SQLHDBC hDbc, SQLHSTMT hStmt );
static int get_args(char *string, char **args, int maxarg);
static void free_args(char **args, int maxarg);
static void output_help(void);


int     bVerbose                    = 0;
int     nUserWidth                  = 0;
SQLHENV hEnv                        = 0;
SQLHDBC hDbc                        = 0;
int     bQuote                      = 0;
int     version3                    = 0;
int     bBatch                      = 0;
int     bIntro                      = 0;
int     ac_off                      = 0;
int     bHTMLTable                  = 0;
int     cDelimiter                  = 0;
int     bColumnNames                = 0;
int     buseDC                      = 0;
int     buseED                      = 0;
int     max_col_size                = MAX_DATA_WIDTH;
SQLUSMALLINT    has_moreresults     = 1;

int main( int argc, char *argv[] )
{
    int     nArg, count;
    int     bNewStyle                   = 0;
    char    *szDSN;
    char    *szUID;
    char    *szPWD;
    char    *szSQL;
    char    *line_buffer;
    int     buffer_size = 9000;
    int     line_buffer_size = 9000;
    int     bufpos,linen;
    char    prompt[24];
#if defined(HAVE_EDITLINE) || defined(HAVE_READLINE)
    char    *rlhistory; /* readline history path */

    if (getenv("HOME")) {
    rlhistory = strdup(getenv("HOME"));
    rlhistory = realloc(rlhistory, strlen(rlhistory)+16);
    strcat(rlhistory, "/.isql_history");
    read_history(rlhistory);
    }
#endif

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
                case 'm':
                    nUserWidth = atoi( &(argv[nArg][2]) );
                    break;
                case 's':
                    buffer_size = atoi( &(argv[nArg][2]) );
                    line_buffer_size = buffer_size;
                    break;
                case 'w':
                    bHTMLTable = 1;
                    break;
                case 'b':
                    bBatch = 1;
                    break;
                case 'i':
                    bIntro = 1;
                    break;
                case 'c':
                    bColumnNames = 1;
                    break;
                case '3':
                    version3 = 1;
                    break;
                case 'v':
                    bVerbose = 1;
                    break;
                case 'q':
                    bQuote = 1;
                    break;
                case 'n':
                    bNewStyle = 1;
                    break;
                case 'k':
                    buseDC = 1;
                    break;
                case 'e':
                    buseED = 1;
                    break;
                case 'L':
                    max_col_size = atoi( &(argv[nArg][2]) );
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
        else if ( count == 1 )
            szDSN = argv[nArg];
        else if ( count == 2 )
            szUID = argv[nArg];
        else if ( count == 3 )
            szPWD = argv[nArg];
        count++;
    }

    szSQL = calloc( 1, buffer_size + 1 );
    line_buffer = calloc( 1, buffer_size + 1 );

#ifdef HAVE_SETVBUF
    /* Ensure result lines are available to reader of whatever stdout */
    if (bBatch) {
        (void)setvbuf(stdout, NULL, _IOLBF, (size_t) 0);
    }
#endif

    /****************************
     * CONNECT
     ***************************/

    if (!OpenDatabase( &hEnv, &hDbc, szDSN, szUID, szPWD ))
        exit( 1 );

    /****************************
     * EXECUTE
     ***************************/
    if ( !bBatch && !bIntro)
    {
        printf( "+---------------------------------------+\n" );
        printf( "| Connected!                            |\n" );
        printf( "|                                       |\n" );
        if ( bNewStyle )
        {
            printf( "| sql-statement(s)[;]                   |\n" );
            printf( "| go                                    |\n" );
            printf( "| \\noac                                 |\n" );
            printf( "| \\ac                                   |\n" );
            printf( "| \\commit                               |\n" );
            printf( "| \\rollback                             |\n" );
            printf( "| \\tables                               |\n" );
            printf( "| \\columns <table-name>                 |\n" );
            printf( "| \\echo [string]                        |\n" );
            printf( "| \\quit                                 |\n" );
        }
        else
        {
            printf( "| sql-statement                         |\n" );
            printf( "| help [tablename]                      |\n" );
            printf( "| echo [string]                         |\n" );
            printf( "| quit                                  |\n" );
        }
        printf( "|                                       |\n" );
        printf( "+---------------------------------------+\n" );
    }

    linen = 0;
    bufpos = 0;

    do
    {
        char *line = NULL;
        int malloced = 0;
        int len = 0;
        int dont_copy, exec_now;

        szSQL[ bufpos ] = '\0';

        if ( bNewStyle )
        {
            if ( ac_off )
            {
                sprintf( prompt, "*%d SQL> ", ++linen );
            }
            else
            {
                sprintf( prompt, "%d SQL> ", ++linen );
            }
        }
        else
        {
            sprintf( prompt, "SQL> " );
        }

        if ( !bBatch )
        {
#if defined(HAVE_EDITLINE) || defined(HAVE_READLINE)
            line=readline( prompt );
            if ( !line )        /* EOF - ctrl D */
            {
                malloced = 1;
                if ( bNewStyle )
                {
                    line = strdup( "\\quit" );
                }
                else
                {
                    line = strdup( "quit" );
                }
            }
            else
            {
                malloced = 0;
            }

            if ( strcmp(line, "quit") && strcmp(line, "\\quit") ) 
            {
                add_history(line);
            }
#else
            fputs( prompt, stdout );

            line = fgets( line_buffer, line_buffer_size, stdin );
            if ( !line )        /* EOF - ctrl D */
            {
                malloced = 1;
                if ( bNewStyle )
                {
                    line = strdup( "\\quit" );
                }
                else
                {
                    line = strdup( "quit" );
                }
            }
            else
            {
				if ( line[ 0 ] == '\n' ) 
				{
					malloced = 1;
                    if ( bNewStyle )
                    {
                        line = strdup( "\\quit" );
                    }
                    else
                    {
					    line = strdup( "quit" );
                    }
				}
				else 
				{
					malloced = 0;
				}
            }
#endif
        }
        else
        {
            line = fgets( line_buffer, line_buffer_size, stdin );
            if ( !line )        /* EOF - ctrl D */
            {
                malloced = 1;
                if ( bNewStyle )
                {
                    line = strdup( "\\quit" );
                }
                else
                {
                    line = strdup( "quit" );
                }
            }
            else
            {
				if ( line[ 0 ] == '\n' ) 
				{
					malloced = 1;
                    if ( bNewStyle )
                    {
                        line = strdup( "\\quit" );
                    }
                    else
                    {
					    line = strdup( "quit" );
                    }
				}
				else 
				{
					malloced = 0;
				}
            }
        }

        /* remove any ctrl chars, and find the length */

        len = 0;
        while ( line[ len ] )
        {
            if ( line[ len ] == '\n' )
            {
                line[ len ] = ' ';
            }
            if ( line[ len ] == '\r' )
            {
                line[ len ] = ' ';
            }
            len ++;
        }

        /* remove trailing spaces */

        while( len > 0 ) 
        {
            if ( line[ len - 1 ] == ' ' ) 
            {
                len --;
            }
            else 
            {
                break;
            }
        }

        /*
         * is it a comment? 
         */

        if ( bNewStyle )
        {
            if ( len >= 2 && line[ 0 ] == '-' && line[ 1 ] == '-' )
            {
                /* 
                 * it can't have been malloc'd
                 */
                continue;
            }
        }

        dont_copy = 0;
        exec_now = 0;

        if ( bNewStyle )
        {
            if ( len > 0 && line[ len - 1 ] == ';' )
            {
                line[ len - 1 ] = '\0';
                exec_now = 1;
                len --;
            }
            else if ( len == 3 && memcmp( line, "go", 2 ) == 0 )
            {
                exec_now = 1;
                dont_copy = 1;
            }
            else if ( len > 1 && line[ 0 ] == '\\' )
            {
                bufpos = 0;
                linen = 1;
                exec_now = 1;
            }
        }
        else
        {
            exec_now = 1;
        }

        if ( !bNewStyle )
        {
            if ( len >= 4 && memcmp( line, "quit", 4 ) == 0 )
            {
                if ( malloced )
                {
                    free(line);
                }
                break;
            }
        }

        /*
         * stop on a blank line
         */

        if ( !bNewStyle )
        {
            if ( line[ 0 ] == '\0' )
            {
                break;
            }
        }

        if ( !dont_copy )
        {
            /*
             * is there space
             */

            if ( len > 0 && bufpos + len + 2 > buffer_size )
            {
                szSQL = realloc( szSQL, bufpos + len + 2 );
                buffer_size = bufpos + len + 2;
            }

            /*
             * insert space between the lines
             * the above length check will make sure there is room for 
             * the extra space
             */
            if ( linen > 1 )
            {
                szSQL[ bufpos ] = ' ';
                bufpos ++;
            }

            memcpy( szSQL + bufpos, line, len );
            bufpos += len;
            szSQL[ bufpos ] = '\0';
        }

        if ( exec_now )
        {
            if ( bNewStyle )
            {
                if ( bufpos >= 1 && szSQL[ 0 ] == '\\' )
                {
                    if ( ExecuteSlash( hDbc, szSQL, cDelimiter, bColumnNames, bHTMLTable ) == 0 )
                    {
                        break;
                    }
                }
                else
                {
                    ExecuteSQL( hDbc, szSQL, cDelimiter, bColumnNames, bHTMLTable );
                }
            }
            else
            {
                if ( bufpos >= 4 && memcmp( szSQL, "help", 4 ) == 0 )
                {
                    ExecuteHelp( hDbc, szSQL, cDelimiter, bColumnNames, bHTMLTable );
                }
                else if ( bufpos >= 4 && memcmp( szSQL, "echo", 4 ) == 0 )
                {
                    ExecuteEcho( hDbc, szSQL, cDelimiter, bColumnNames, bHTMLTable );
                }
                else
                {
                    ExecuteSQL( hDbc, szSQL, cDelimiter, bColumnNames, bHTMLTable );
                }
            }
            linen = 0;
            bufpos = 0;
        }

    } while ( 1 );

    /****************************
     * DISCONNECT
     ***************************/

#if defined(HAVE_EDITLINE) || defined(HAVE_READLINE)
    write_history(rlhistory);
#endif

    CloseDatabase( hEnv, hDbc );

    exit( 0 );
}

static void mem_error( int line ) 
{
    if ( bVerbose ) DumpODBCLog( hEnv, 0, 0 );
    fprintf( stderr, "[ISQL]ERROR: memory allocation fail before line %d\n", line );
    exit(-1);
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
        nDataWidth = max_col_size;
    }

    nOptimalDisplayWidth = max( nLabelWidth, nDataWidth );

    if ( nUserWidth > 0 )
        nOptimalDisplayWidth = min( nOptimalDisplayWidth, nUserWidth );

    if ( nOptimalDisplayWidth > max_col_size )
        nOptimalDisplayWidth = max_col_size;

    return nOptimalDisplayWidth;
}

/****************************
 * OpenDatabase - do everything we have to do to get a viable connection to szDSN
 ***************************/
static int
OpenDatabase( SQLHENV *phEnv, SQLHDBC *phDbc, char *szDSN, char *szUID, char *szPWD )
{
    if ( version3 )
    {
        if ( SQLAllocHandle( SQL_HANDLE_ENV, NULL, phEnv ) != SQL_SUCCESS )
        {
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocHandle( SQL_HANDLE_ENV )\n" );
            return 0;
        }

        if ( SQLSetEnvAttr( *phEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) 3, 0 ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, 0, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLSetEnvAttr( SQL_HANDLE_DBC )\n" );
            SQLFreeHandle( SQL_HANDLE_ENV, *phEnv );
            return 0;
        }

        if ( SQLAllocHandle( SQL_HANDLE_DBC, *phEnv, phDbc ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, 0, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocHandle( SQL_HANDLE_DBC )\n" );
            SQLFreeHandle( SQL_HANDLE_ENV, *phEnv );
            return 0;
        }

        if ( buseDC ) {
            if ( !SQL_SUCCEEDED( SQLDriverConnect( *phDbc, NULL, (SQLCHAR*)szDSN, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT )))
            {
                if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
                fprintf( stderr, "[ISQL]ERROR: Could not SQLDriverConnect\n" );
                SQLFreeHandle( SQL_HANDLE_DBC, *phDbc );
                SQLFreeHandle( SQL_HANDLE_ENV, *phEnv );
                return 0;
            }
        }
        else {
            if ( !SQL_SUCCEEDED( SQLConnect( *phDbc, (SQLCHAR*)szDSN, SQL_NTS, (SQLCHAR*)szUID, SQL_NTS, (SQLCHAR*)szPWD, SQL_NTS )))
            {
                if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
                fprintf( stderr, "[ISQL]ERROR: Could not SQLConnect\n" );
                SQLFreeHandle( SQL_HANDLE_DBC, *phDbc );
                SQLFreeHandle( SQL_HANDLE_ENV, *phEnv );
                return 0;
            }
        }
    }
    else
    {
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

        if ( buseDC ) {
            if ( !SQL_SUCCEEDED( SQLDriverConnect( *phDbc, NULL, (SQLCHAR*)szDSN, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT )))
            {
                if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
                fprintf( stderr, "[ISQL]ERROR: Could not SQLDriverConnect\n" );
                SQLFreeConnect( *phDbc );
                SQLFreeEnv( *phEnv );
                return 0;
            }
        }
        else {
            if ( !SQL_SUCCEEDED( SQLConnect( *phDbc, (SQLCHAR*)szDSN, SQL_NTS, (SQLCHAR*)szUID, SQL_NTS, (SQLCHAR*)szPWD, SQL_NTS )))
            {
                if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
                fprintf( stderr, "[ISQL]ERROR: Could not SQLConnect\n" );
                SQLFreeConnect( *phDbc );
                SQLFreeEnv( *phEnv );
                return 0;
            }
        }
    }

    /*
     * does the driver support SQLMoreResults
     */

    if ( !SQL_SUCCEEDED( SQLGetFunctions( *phDbc, SQL_API_SQLMORERESULTS, &has_moreresults )))
    {
        has_moreresults = 0;
    }

    return 1;
}

static void display_result_set( SQLHDBC hDbc, SQLHSTMT hStmt )
{
    SQLCHAR         *szSepLine;
    SQLRETURN       ret;
    int             mr;
    SQLSMALLINT     cols;
    SQLLEN          nRows                   = 0;

    szSepLine = calloc(1, 32001);
    /*
     * Loop while SQLMoreResults returns success
     */

    mr = 0;

    do
    {
        if ( mr )
        {
            if ( ret == SQL_SUCCESS_WITH_INFO )
            {
                if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
                fprintf( stderr, "[ISQL]INFO: SQLMoreResults returned SQL_SUCCESS_WITH_INFO\n" );
            }
            else if ( ret == SQL_ERROR )
            {
                if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
                fprintf( stderr, "[ISQL]ERROR: SQLMoreResults returned SQL_ERROR\n" );
            }
        }
        mr = 1;
        strcpy ((char*) szSepLine, "" ) ;

        /*
         * check to see if it has generated a result set
         */

        if ( SQLNumResultCols( hStmt, &cols ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLNumResultCols\n" );
            break;
        }

        if ( cols > 0 )
        {
            /****************************
             * WRITE HEADER
             ***************************/
            if ( bHTMLTable )
                WriteHeaderHTMLTable( hStmt );
            else if ( cDelimiter == 0 )
                WriteHeaderNormal( hStmt, &szSepLine );
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
            WriteFooterNormal( hStmt, szSepLine, nRows );
    }
    while ( has_moreresults && ( ret = SQLMoreResults( hStmt )) != SQL_NO_DATA );

    free( szSepLine );
}

static int display_tables( SQLHDBC hDbc )
{
    SQLHSTMT hStmt;
    SQLRETURN ret;

    if ( version3 )
    {
        if ( SQLAllocHandle( SQL_HANDLE_STMT, hDbc, &hStmt ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocHandle( SQL_HANDLE_STMT )\n" );
            return 0;
        }
    }
    else
    {
        if ( SQLAllocStmt( hDbc, &hStmt ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocStmt\n" );
            return 0;
        }
    }

    ret = SQLTables( hStmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0 );
    if ( ret == SQL_ERROR )
    {
        if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
        fprintf( stderr, "[ISQL]ERROR: Could not SQLTables\n" );
    }
    else
    {
        display_result_set( hDbc, hStmt );
    }

    SQLFreeStmt( hStmt, SQL_DROP );

    return 1;
}

static int  display_columns( SQLHDBC hDbc, char *sql )
{
    SQLHSTMT hStmt;
    SQLRETURN ret;
    char *args[10];
    int n_args;
    SQLCHAR *table;
    int len;

    if ( version3 )
    {
        if ( SQLAllocHandle( SQL_HANDLE_STMT, hDbc, &hStmt ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocHandle( SQL_HANDLE_STMT )\n" );
            return 0;
        }
    }
    else
    {
        if ( SQLAllocStmt( hDbc, &hStmt ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocStmt\n" );
            return 0;
        }
    }

    n_args = get_args(sql, &args[0], sizeof(args) / sizeof(args[0]));

    if ( n_args == 0 )
    {
        table = NULL;
        len = 0;
    }
    else
    {
        table = (SQLCHAR*)args[ 0 ];
        len = SQL_NTS;
    }

    ret = SQLColumns( hStmt, NULL, 0, NULL, 0, table, len, NULL, 0 );
    if ( ret == SQL_ERROR )
    {
        if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
        fprintf( stderr, "[ISQL]ERROR: Could not SQLTables\n" );
    }
    else
    {
        display_result_set( hDbc, hStmt );
    }

    SQLFreeStmt( hStmt, SQL_DROP );
    free_args(args, sizeof(args) / sizeof(args[0]));

    return 1;
}

/****************************
 * ExecuteSlash - meta commands
 ***************************/
static int
ExecuteSlash( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable )
{
    SQLRETURN ret;

    szSQL ++;

    if ( memcmp( szSQL, "tables", 6 ) == 0 )
    {
        return display_tables( hDbc );
    }
    else if ( memcmp( szSQL, "columns", 7 ) == 0 )
    {
        return display_columns( hDbc, szSQL + 7 );
    }
    else if ( memcmp( szSQL, "ac", 2 ) == 0 )
    {
        if ( version3 )
        {
            ret = SQLSetConnectAttr( hDbc, SQL_ATTR_AUTOCOMMIT, 
                                     (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0 );

            if ( SQL_SUCCEEDED( ret ) )
            {
                ac_off = 0;
            }
        }
        else
        {
            ret = SQLSetConnectOption( hDbc, SQL_ATTR_AUTOCOMMIT, 
                                       SQL_AUTOCOMMIT_ON );

            if ( SQL_SUCCEEDED( ret ) )
            {
                ac_off = 0;
            }
        }

        if ( !bBatch )
        {
            printf( "AUTOCOMMIT ON (return status = %d)\n", ret );
        }

        if ( bVerbose && !SQL_SUCCEEDED( ret ))
        {
            DumpODBCLog( hEnv, hDbc, 0 );
        }
    }
    else if ( memcmp( szSQL, "noac", 4 ) == 0 )
    {
        if ( version3 )
        {
            ret = SQLSetConnectAttr( hDbc, SQL_ATTR_AUTOCOMMIT, 
                                     (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0 );

            if ( SQL_SUCCEEDED( ret ) )
            {
                ac_off = 1;
            }
        }
        else
        {
            ret = SQLSetConnectOption( hDbc, SQL_ATTR_AUTOCOMMIT, 
                                       SQL_AUTOCOMMIT_OFF );

            if ( SQL_SUCCEEDED( ret ) )
            {
                ac_off = 1;
            }
        }

        if ( !bBatch )
        {
            printf( "AUTOCOMMIT OFF (return status = %d)\n", ret );
        }

        if ( bVerbose && !SQL_SUCCEEDED( ret ))
        {
            DumpODBCLog( hEnv, hDbc, 0 );
        }
    }
    else if ( memcmp( szSQL, "commit", 6 ) == 0 )
    {
        if ( version3 )
        {
            ret = SQLEndTran( SQL_HANDLE_DBC, hDbc, SQL_COMMIT );
        }
        else
        {
            ret = SQLTransact( hEnv, hDbc, SQL_COMMIT );
        }

        if ( !bBatch )
        {
            printf( "COMMIT (return status = %d)\n", ret );
        }

        if ( bVerbose && !SQL_SUCCEEDED( ret ))
        {
            DumpODBCLog( hEnv, hDbc, 0 );
        }
    }
    else if ( memcmp( szSQL, "rollback", 8 ) == 0 )
    {
        if ( version3 )
        {
            ret = SQLEndTran( SQL_HANDLE_DBC, hDbc, SQL_ROLLBACK );
        }
        else
        {
            ret = SQLTransact( hEnv, hDbc, SQL_ROLLBACK );
        }

        if ( !bBatch )
        {
            printf( "ROLLBACK (return status = %d)\n", ret );
        }

        if ( bVerbose && !SQL_SUCCEEDED( ret ))
        {
            DumpODBCLog( hEnv, hDbc, 0 );
        }
    }
    else if ( memcmp( szSQL, "echo", 4 ) == 0 )
    {
        char *p;

        for ( p = szSQL+4; *p != '\0' && isspace((int)*p) ; ++p )
            ;
        if ( *p != '\0' && p == szSQL+4 ) {
            fprintf( stderr, "[ISQL]ERROR: incorrect echo call\n" );
            return 0;
        }

        (void)printf( "%s\n", p );
    }
    else if ( memcmp( szSQL, "quit", 4 ) == 0 )
    {
        return 0;
    }
    else
    {
        printf( "\nUnknown metacommand '%s'\n\n", szSQL );
    }

    return 1;
}

/****************************
 * ExecuteSQL - create a statement, execute the SQL, and get rid of the statement
 *            - show results as per request; bHTMLTable has precedence over other options
 ***************************/
static int
ExecuteSQL( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable )
{
    SQLHSTMT        hStmt;
    SQLSMALLINT     cols;
    SQLLEN          nRows                   = 0;
    SQLINTEGER      ret;
    SQLCHAR         *szSepLine;
    int             mr;

    szSepLine = calloc(1, 32001);

    /****************************
     * EXECUTE SQL
     ***************************/
    if ( version3 )
    {
        if ( SQLAllocHandle( SQL_HANDLE_STMT, hDbc, &hStmt ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocHandle( SQL_HANDLE_STMT )\n" );
            free(szSepLine);
            return 0;
        }
    }
    else
    {
        if ( SQLAllocStmt( hDbc, &hStmt ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocStmt\n" );
            free(szSepLine);
            return 0;
        }
    }

    if ( buseED ) {
        ret = SQLExecDirect( hStmt, (SQLCHAR*)szSQL, strlen( szSQL ));

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
            free(szSepLine);
            return 0;
        }
    }
    else {
        ret = SQLPrepare( hStmt, (SQLCHAR*)szSQL, strlen( szSQL ));

        if ( ret == SQL_SUCCESS_WITH_INFO )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]INFO: SQLPrepare returned SQL_SUCCESS_WITH_INFO\n" );
        }
        else if ( ret != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLPrepare\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            free(szSepLine);
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
            free(szSepLine);
            return 0;
        }
    }

    /*
     * Loop while SQLMoreResults returns success
     */

    mr = 0;

    do
    {
        if ( mr )
        {
            if ( ret == SQL_SUCCESS_WITH_INFO )
            {
                if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
                fprintf( stderr, "[ISQL]INFO: SQLMoreResults returned SQL_SUCCESS_WITH_INFO\n" );
            }
            else if ( ret == SQL_ERROR )
            {
                if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
                fprintf( stderr, "[ISQL]ERROR: SQLMoreResults returned SQL_ERROR\n" );
            }
        }
        mr = 1;
        strcpy ((char*) szSepLine, "" ) ;

        /*
         * check to see if it has generated a result set
         */

        if ( SQLNumResultCols( hStmt, &cols ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLNumResultCols\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            free(szSepLine);
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
                WriteHeaderNormal( hStmt, &szSepLine );
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
            WriteFooterNormal( hStmt, szSepLine, nRows );
    }
    while ( has_moreresults && ( ret = SQLMoreResults( hStmt )) != SQL_NO_DATA );

    /****************************
     * CLEANUP
     ***************************/
    SQLFreeStmt( hStmt, SQL_DROP );
    free(szSepLine);

    return 1;
}

/****************************
 * ExecuteHelp - create a statement, execute the SQL, and get rid of the statement
 *             - show results as per request; bHTMLTable has precedence over other options
 ***************************/
static int
ExecuteHelp( SQLHDBC hDbc, char *szSQL, char cDelimiter, int bColumnNames, int bHTMLTable )
{
    SQLHSTMT hStmt;
    SQLLEN nRows = 0;
    SQLRETURN nReturn;
    SQLCHAR *szSepLine;
    char *args[10];
    int n_args;

    if (!(szSepLine = calloc(1, 32001)))
    {
        fprintf(stderr, "[ISQL]ERROR: Failed to allocate line");
        return 0;
    }

    /****************************
     * EXECUTE SQL
     ***************************/
    if ( version3 )
    {
        if ( SQLAllocHandle( SQL_HANDLE_STMT, hDbc, &hStmt ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocHandle( SQL_HANDLE_STMT )\n" );
            free(szSepLine);
            return 0;
        }
    }
    else
    {
        if ( SQLAllocStmt( hDbc, &hStmt ) != SQL_SUCCESS )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, 0 );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLAllocStmt\n" );
            free(szSepLine);
            return 0;
        }
    }
    n_args = get_args(szSQL, &args[0], sizeof(args) / sizeof(args[0]));

    if (n_args == 2 )
    {
        if (strcmp(args[1], "help") == 0)
        {
            output_help();
            free(szSepLine);
            return 0;
        }

        /* COLUMNS */
        nReturn = SQLColumns( hStmt, NULL, 0, NULL, 0, (SQLCHAR*)args[1], SQL_NTS, NULL, 0 );
        if ( (nReturn != SQL_SUCCESS) && (nReturn != SQL_SUCCESS_WITH_INFO) )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLColumns\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            free(szSepLine);
            return 0;
        }
    }
    else
    {
        SQLCHAR *catalog = NULL;
        SQLCHAR *schema = NULL;
        SQLCHAR *table = NULL;
        SQLCHAR *type = NULL;

        if (n_args > 2)
        {
            catalog = (SQLCHAR*)args[1];
            schema = (SQLCHAR*)args[2];
            table = (SQLCHAR*)args[3];
            type = (SQLCHAR*)args[4];
        }

        /* TABLES */
        nReturn = SQLTables( hStmt, catalog, SQL_NTS, schema, SQL_NTS,
                             table, SQL_NTS, type, SQL_NTS );
        if ( (nReturn != SQL_SUCCESS) && (nReturn != SQL_SUCCESS_WITH_INFO) )
        {
            if ( bVerbose ) DumpODBCLog( hEnv, hDbc, hStmt );
            fprintf( stderr, "[ISQL]ERROR: Could not SQLTables\n" );
            SQLFreeStmt( hStmt, SQL_DROP );
            free(szSepLine);
            free_args(args, sizeof(args) / sizeof(args[0]));
            return 0;
        }
    }

    /****************************
     * WRITE HEADER
     ***************************/
    if ( bHTMLTable )
        WriteHeaderHTMLTable( hStmt );
    else if ( cDelimiter == 0 )
        WriteHeaderNormal( hStmt, &szSepLine );
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
        WriteFooterNormal( hStmt, szSepLine, nRows );

    /****************************
     * CLEANUP
     ***************************/
    SQLFreeStmt( hStmt, SQL_DROP );
    free(szSepLine);
    free_args(args, sizeof(args) / sizeof(args[0]));
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
static int CloseDatabase( SQLHENV hEnv, SQLHDBC hDbc )
{
    SQLDisconnect( hDbc );
    if ( version3 )
    {
        SQLFreeHandle( SQL_HANDLE_DBC, hDbc );
        SQLFreeHandle( SQL_HANDLE_ENV, hEnv );
    }
    else
    {
        SQLFreeConnect( hDbc );
        SQLFreeEnv( hEnv );
    }

    return 1;
}


/****************************
 * WRITE HTML
 ***************************/
static void WriteHeaderHTMLTable( SQLHSTMT hStmt )
{
    SQLINTEGER      nCol                            = 0;
    SQLSMALLINT     nColumns                        = 0;
    SQLCHAR         szColumnName[MAX_DATA_WIDTH+1]; 

    *szColumnName = '\0';

    printf( "<table BORDER>\n" );
    printf( "<tr BGCOLOR=#000099>\n" );

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    for ( nCol = 1; nCol <= nColumns; nCol++ )
    {
        SQLColAttribute( hStmt, nCol, SQL_DESC_LABEL, szColumnName, sizeof(szColumnName), NULL, NULL );
        printf( "<td>\n" );
        printf( "<font face=Arial,Helvetica><font color=#FFFFFF>\n" );
        printf( "%s\n", szColumnName );
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
    SQLCHAR         *szColumnValue;
    SQLRETURN       nReturn                         = 0;
    SQLRETURN       ret;

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    szColumnValue = malloc( max_col_size + 1 );
    if ( !szColumnValue ) {
        mem_error( __LINE__ );
    }

    while ( (ret = SQLFetch( hStmt )) == SQL_SUCCESS ) /* ROWS */
    {
        printf( "<tr>\n" );

        for ( nCol = 1; nCol <= nColumns; nCol++ ) /* COLS */
        {
            printf( "<td>\n" );
            printf( "<font face=Arial,Helvetica>\n" );

            nReturn = SQLGetData( hStmt, nCol, SQL_C_CHAR, (SQLPOINTER)szColumnValue, max_col_size + 1, &nIndicator );
            if ( nReturn == SQL_SUCCESS && nIndicator != SQL_NULL_DATA )
            {
                fputs((char*) szColumnValue, stdout );
            }
            else if ( nReturn == SQL_SUCCESS_WITH_INFO ) {
                szColumnValue[ max_col_size ] = '\0';
                fputs((char*) szColumnValue, stdout );
                fputs((char*) "...", stdout );
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

    free( szColumnValue );
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
    SQLCHAR         szColumnName[MAX_DATA_WIDTH+1]; 

    *szColumnName = '\0';

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    for ( nCol = 1; nCol <= nColumns; nCol++ )
    {
        SQLColAttribute( hStmt, nCol, SQL_DESC_LABEL, szColumnName, sizeof(szColumnName), NULL, NULL );
        fputs((char*) szColumnName, stdout );
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
    SQLCHAR         *szColumnValue;
    SQLRETURN       nReturn                         = 0;
    SQLRETURN       ret;
    SQLINTEGER      *types;

    szColumnValue = malloc( max_col_size + 1 );
    if ( !szColumnValue ) {
        mem_error( __LINE__ );
    }

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    if ( bQuote && nColumns > 0 )
    {
        types = malloc( nColumns * sizeof ( SQLINTEGER ));
        for ( nCol = 1; nCol <= nColumns && types; nCol++ )
        {
            SQLSMALLINT type = 0;
            nReturn = SQLDescribeCol( hStmt, nCol, NULL, 0, NULL, &type, NULL, NULL, NULL );
            switch ( type )
            {
                case SQL_CHAR:
                case SQL_VARCHAR:
                case SQL_WCHAR:
                case SQL_WVARCHAR:
                case SQL_LONGVARCHAR:
                case SQL_WLONGVARCHAR:
                    types[ nCol - 1 ] = 1;
                    break;

                default:
                    types[ nCol - 1 ] = 0;
                    break;
            }
        }
    }
    else
    {
        types = NULL;
    }

    /* ROWS */
    while (( ret = SQLFetch( hStmt )) == SQL_SUCCESS )
    {
        /* COLS */
        for ( nCol = 1; nCol <= nColumns; nCol++ )
        {
            nReturn = SQLGetData( hStmt, nCol, SQL_C_CHAR, (SQLPOINTER)szColumnValue, max_col_size + 1, &nIndicator );
            if ( nReturn == SQL_SUCCESS && nIndicator != SQL_NULL_DATA )
            {
                if ( types && types[ nCol - 1 ] )
                {
                    putchar( '"' );
                }
                fputs((char*) szColumnValue, stdout );
                if ( types && types[ nCol - 1 ] )
                {
                    putchar( '"' );
                }
                if ( nCol < nColumns )
                {
                    putchar( cDelimiter );
                }
            }
            else if ( nReturn == SQL_SUCCESS_WITH_INFO ) 
            {
                szColumnValue[ max_col_size ] = '\0';
                if ( types && types[ nCol - 1 ] )
                {
                    putchar( '"' );
                }
                fputs((char*) szColumnValue, stdout );
                fputs((char*) "...", stdout );
                if ( types && types[ nCol - 1 ] )
                {
                    putchar( '"' );
                }
                if ( nCol < nColumns )
                {
                    putchar( cDelimiter );
                }
            }
            else if ( nReturn == SQL_ERROR )
            {
                ret = SQL_ERROR;
                break;
            }
            else
            {
                if ( nCol < nColumns )
                {
                    putchar( cDelimiter );
                }
            }
        }
        if (ret != SQL_SUCCESS)
        {
            break;
        }

        printf( "\n" );
    }

    if ( ret == SQL_ERROR )
    {
        if ( bVerbose ) DumpODBCLog( 0, 0, hStmt );
    }

    if ( types )
    {
        free( types );
    }

    free( szColumnValue );
}

/****************************
 * WRITE NORMAL
 ***************************/
static void WriteHeaderNormal( SQLHSTMT hStmt, SQLCHAR **szSepLine )
{
    SQLINTEGER      nCol                            = 0;
    SQLSMALLINT     nColumns                        = 0;
    SQLCHAR         *szColumn;    
    SQLCHAR         *szColumnName; 
    SQLCHAR         *szHdrLine;
    SQLUINTEGER     nOptimalDisplayWidth            = 10;

    szColumnName = malloc( max_col_size + 1 );
    if ( !szColumnName ) {
        mem_error( __LINE__ );
    }
    szColumn = malloc( max_col_size + 3 );
    if ( !szColumn ) {
        free( szColumnName );
        mem_error( __LINE__ );
    }

    *szColumn = '\0';
    *szColumnName = '\0';

    if ( SQLNumResultCols( hStmt, &nColumns ) != SQL_SUCCESS )
        nColumns = -1;

    if ( nColumns > 0 ) {
        szHdrLine = calloc(1, 512 + max_col_size * nColumns );
        if ( !szHdrLine ) {
            mem_error( __LINE__ );
        }
        *szSepLine = realloc( *szSepLine, 512 + max_col_size * nColumns );
        if ( !*szSepLine ) {
            mem_error( __LINE__ );
        }
    }
    else {
        szHdrLine = calloc(1, 32001);
    }

    for ( nCol = 1; nCol <= nColumns; nCol++ )
    {
        int sret;

        nOptimalDisplayWidth = OptimalDisplayWidth( hStmt, nCol, nUserWidth );
        SQLColAttribute( hStmt, nCol, SQL_DESC_LABEL, szColumnName, max_col_size, NULL, NULL );

        /* SEP */
        memset( szColumn, '\0', max_col_size + 2 );
        memset( szColumn, '-', nOptimalDisplayWidth + 1 );
        strcat((char*) *szSepLine, "+" );
        strcat((char*) *szSepLine,(char*) szColumn );

        /* HDR */
        sret = sprintf( (char*)szColumn, "| %-*.*s",
                        (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, szColumnName );
        if (sret < 0)
            sprintf((char *)szColumn, "| %-*.*s",
                    (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, "**ERROR**");
        strcat( (char*)szHdrLine,(char*) szColumn );
    }
    strcat((char*) *szSepLine, "+\n" );
    strcat((char*) szHdrLine, "|\n" );

    fputs((char*) *szSepLine, stdout );
    fputs((char*) szHdrLine, stdout );
    fputs((char*) *szSepLine, stdout );

    free(szHdrLine);
    free( szColumnName );
    free( szColumn );
}

static SQLLEN WriteBodyNormal( SQLHSTMT hStmt )
{
    SQLINTEGER      nCol                            = 0;
    SQLSMALLINT     nColumns                        = 0;
    SQLLEN          nIndicator                      = 0;
    SQLCHAR         *szColumn;
    SQLCHAR         *szColumnValue;
    SQLRETURN       nReturn                         = 0;
    SQLLEN          nRows                           = 0;
    SQLUINTEGER     nOptimalDisplayWidth            = 10;

    szColumnValue = malloc( max_col_size + 1 );
    if ( !szColumnValue ) {
        mem_error( __LINE__ );
    }

    szColumn = malloc( max_col_size + 21 );
    if ( !szColumn ) {
        free( szColumnValue );
        mem_error( __LINE__ );
    }

    nReturn = SQLNumResultCols( hStmt, &nColumns );
    if ( nReturn != SQL_SUCCESS && nReturn != SQL_SUCCESS_WITH_INFO )
        nColumns = -1;

    /* ROWS */
    nReturn = SQLFetch( hStmt );
    while ( nReturn == SQL_SUCCESS || nReturn == SQL_SUCCESS_WITH_INFO )
    {
        /* COLS */
        for ( nCol = 1; nCol <= nColumns; nCol++ )
        {
            int sret;

            nOptimalDisplayWidth = OptimalDisplayWidth( hStmt, nCol, nUserWidth );
            nReturn = SQLGetData( hStmt, nCol, SQL_C_CHAR, (SQLPOINTER)szColumnValue, max_col_size + 1, &nIndicator );
            szColumnValue[max_col_size] = '\0';

            if ( nReturn == SQL_SUCCESS && nIndicator != SQL_NULL_DATA )
            {
                sret = sprintf( (char*)szColumn, "| %-*.*s",
                                (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, szColumnValue );
                if (sret < 0) sprintf( (char*)szColumn, "| %-*.*s",
                                       (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, "**ERROR**" );

            }
            else if ( nReturn == SQL_SUCCESS_WITH_INFO ) {
                sret = sprintf( (char*)szColumn, "| %-*.*s...",
                                (int)nOptimalDisplayWidth - 3, (int)nOptimalDisplayWidth - 3, szColumnValue );
                if (sret < 0) sprintf( (char*)szColumn, "| %-*.*s",
                                       (int)nOptimalDisplayWidth, (int)nOptimalDisplayWidth, "**ERROR**" );
            }
            else if ( nReturn == SQL_ERROR )
            {
                break;
            }
            else
            {
                sprintf( (char*)szColumn, "| %-*s", (int)nOptimalDisplayWidth, "" );
            }
            fputs( (char*)szColumn, stdout );
        } /* for columns */

        nRows++;
        printf( "|\n" );
        nReturn = SQLFetch( hStmt );
    } /* while rows */

    if ( nReturn == SQL_ERROR )
    {
        if ( bVerbose ) DumpODBCLog( 0, 0, hStmt );
    }

    free( szColumnValue );
    free( szColumn);

    return nRows;
}

static void
WriteFooterNormal( SQLHSTMT hStmt, SQLCHAR  *szSepLine, SQLLEN nRows )
{
    SQLLEN  nRowsAffected   = -1;

    fputs( (char*)szSepLine, stdout );

    SQLRowCount( hStmt, &nRowsAffected );
    printf( "SQLRowCount returns %ld\n", nRowsAffected );

    if ( nRows )
    {
        printf( "%ld rows fetched\n", nRows );
    }
}



static int DumpODBCLog( SQLHENV hEnv, SQLHDBC hDbc, SQLHSTMT hStmt )
{
    SQLCHAR     szError[501];
    SQLCHAR     szSqlState[10];
    SQLINTEGER  nNativeError;
    SQLSMALLINT nErrorMsg;

    if ( version3 )
    {
        int rec;
        if ( hStmt )
        {
            rec = 0;
            while ( SQLGetDiagRec( SQL_HANDLE_STMT, hStmt, ++rec, szSqlState, &nNativeError, szError, 500, &nErrorMsg ) == SQL_SUCCESS )
            {
                printf( "[%s]%s\n", szSqlState, szError );
            }
        }

        if ( hDbc )
        {
            rec = 0;
            while ( SQLGetDiagRec( SQL_HANDLE_DBC, hDbc, ++rec, szSqlState, &nNativeError, szError, 500, &nErrorMsg ) == SQL_SUCCESS )
            {
                printf( "[%s]%s\n", szSqlState, szError );
            }
        }

        if ( hEnv )
        {
            rec = 0;
            while ( SQLGetDiagRec( SQL_HANDLE_ENV, hEnv, ++rec, szSqlState, &nNativeError, szError, 500, &nErrorMsg ) == SQL_SUCCESS )
            {
                printf( "[%s]%s\n", szSqlState, szError );
            }
        }
    }
    else
    {
        if ( hStmt )
        {
            while ( SQL_SUCCEEDED( SQLError( hEnv, hDbc, hStmt, szSqlState, &nNativeError, szError, 500, &nErrorMsg )))
            {
                printf( "[%s]%s\n", szSqlState, szError );
            }
        }

        if ( hDbc )
        {
            while ( SQL_SUCCEEDED( SQLError( hEnv, hDbc, 0, szSqlState, &nNativeError, szError, 500, &nErrorMsg )))
            {
                printf( "[%s]%s\n", szSqlState, szError );
            }
        }

        if ( hEnv )
        {
            while ( SQL_SUCCEEDED( SQLError( hEnv, 0, 0, szSqlState, &nNativeError, szError, 500, &nErrorMsg )))
            {
                printf( "[%s]%s\n", szSqlState, szError );
            }
        }
    }

    return 1;
}

static int get_args(char *string, char **args, int maxarg) {
    int nargs = 0;
    char *copy;
    char *p;
    const char *sep = " ";
    char *arg;
    unsigned int i;

    if (!string || !args) return 0;

    if (!(copy = strdup(string))) return 0;

    for (i = 0; i < maxarg; i++)
    {
        args[i] = NULL;
    }

    p = copy;
    while ((arg = strtok(p, sep)))
    {
        p = NULL;

        if (strcmp(arg, "\"\"") == 0)
            args[nargs++] = strdup("");
        else if (strcmp(arg, "null") == 0)
            args[nargs++] = NULL;
        else
            args[nargs++] = strdup(arg);
        if (nargs > maxarg)
        {
            free(copy);
            return maxarg;
        }
    }
    free(copy);
    return nargs;
}

static void free_args(char **args, int maxarg) {
    unsigned int i;

    for (i = 0; i < maxarg; i++)
    {
        if (args[i])
        {
            free(args[i]);
            args[i] = NULL;
        }
    }
}

static void output_help(void) {
    fprintf(stderr, \
            "help usage:\n\n" \
            "help help - output this help\n" \
            "help - call SQLTables and output the result-set\n" \
            "help table_name - call SQLColumns for table_name and output the result-set\n" \
            "help catalog schema table type - call SQLTables with these arguments\n" \
            "  where any argument may be specified as \"\" (for the empty string) \n" \
            "  or null to pass a null pointer argument.\n" \
            "\n" \
            " e.g.\n" \
            " help %% \"\" \"\" \"\" - output list of catalogs\n" \
            " help \"\" %% \"\" \"\" - output list of schemas\n" \
            " help null null b%% null - output all tables beginning with b\n" \
            " help null null null VIEW - output list of views\n" \
            "\n");
}

