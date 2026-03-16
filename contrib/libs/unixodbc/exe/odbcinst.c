/********************************************
 * odbcinst - command line tool
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under GPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/
#include <config.h>
#include <odbcinstext.h>


char *szSyntax =
"\n" \
"**********************************************\n" \
"* unixODBC - odbcinst                        *\n" \
"**********************************************\n" \
"*                                            *\n" \
"* Purpose:                                   *\n" \
"*                                            *\n" \
"*      An ODBC Installer and Uninstaller.    *\n" \
"*      Updates system files, and             *\n" \
"*      increases/decreases usage counts but  *\n" \
"*      does not actually copy or remove any  *\n" \
"*      files.                                *\n" \
"*                                            *\n" \
"* Syntax:                                    *\n" \
"*                                            *\n" \
"*      odbcinst Action Object Options        *\n" \
"*                                            *\n" \
"* Action:                                    *\n" \
"*                                            *\n" \
"*      -i         install                    *\n" \
"*      -u         uninstall                  *\n" \
"*      -q         query                      *\n" \
"*      -j         print config info          *\n" \
"*      -c         call SQLCreateDataSource   *\n" \
"*      -m         call SQLManageDataSources  *\n" \
"*      --version  version                    *\n" \
"*                                            *\n" \
"* Object:                                    *\n" \
"*                                            *\n" \
"*      -d driver                             *\n" \
"*      -s data source                        *\n" \
"*                                            *\n" \
"* Options:                                   *\n" \
"*                                            *\n" \
"*      -f file name of template.ini follows  *\n" \
"*         this (valid for -i)                *\n" \
"*      -r get template.ini from stdin, not   *\n" \
"*         a template file                    *\n" \
"*      -n Driver or Data Source Name follows *\n" \
"*      -v turn verbose off (no info, warning *\n" \
"*         or error msgs)                     *\n" \
"*      -l system dsn                         *\n" \
"*      -h user dsn                           *\n" \
"*                                            *\n" \
"* Returns:                                   *\n" \
"*                                            *\n" \
"*      0   Success                           *\n" \
"*     !0   Failed                            *\n" \
"*                                            *\n" \
"* Please visit;                              *\n" \
"*                                            *\n" \
"*      http://www.unixodbc.org               *\n" \
"*      pharvey@codebydesign.com              *\n" \
"**********************************************\n\n";

char    szError[ODBC_FILENAME_MAX+1];
DWORD   nError;
char    cVerbose;
int from_stdin = 0;
int system_dsn = 0;
int user_dsn = 0;

/*!
 * \brief   Invoke UI to Create Data Source wizard.
 *
 *          This exists so we can test calling SQLCreateDataSource from an
 *          app which does not provide the UI used by SQLCreateDataSource.
 *
 *          At the moment we have "odbcinstQ4" (the Qt4 based UI) requested
 *          explicitly but this could be changed to simply request the default
 *          or to use a ncurses based UI when that becomes available.
 *
 *          There are at least 3 ways to invoke SQLCreateDataSource;
 *
 *          \li ODBCCreateDataSourceQ4 at the command-line
 *          \li a custom application
 *          \li "odbcinst -c [-nMyDsn]" at the command-line
 *
 * \sa      ManageDataSources
 */
int CreateDataSource( char *pszDataSourceName )
{
    ODBCINSTWND odbcinstwnd;

    odbcinstwnd.hWnd = 0;
    strcpy( odbcinstwnd.szUI, ODBCINSTPLUGIN );

    if ( SQLCreateDataSource( (HWND)&(odbcinstwnd), ( (pszDataSourceName && *pszDataSourceName) ? pszDataSourceName : 0 ) ) == FALSE )
        return 1;

    return 0;
}

/*!
 * \brief   Invoke UI to Manage Data Sources.
 *
 *          This exists so we can test calling SQLManageDataSources from an
 *          app which does not provide the UI used by SQLManageDataSources.
 *
 *          At the moment we have "odbcinstQ4" (the Qt4 based UI) requested
 *          explicitly but this could be changed to simply request the default
 *          or to use a ncurses based UI when that becomes available.
 *
 *          There are at least 3 ways to invoke SQLManageDataSources;
 *
 *          \li ODBCManageDataSourcesQ4 at the command-line
 *          \li a custom application
 *          \li "odbcinst -m" at the command-line
 *
 * \sa      CreateDataSource
 */
int ManageDataSources()
{
    ODBCINSTWND odbcinstwnd;

    odbcinstwnd.hWnd = 0;
    strcpy( odbcinstwnd.szUI, ODBCINSTPLUGIN );

    if ( SQLManageDataSources( (HWND)&(odbcinstwnd) ) == FALSE )
        return 1;

    return 0;
}

int DriverInstall( char *pszTemplate )
{
    HINI    hIni;
    char    szObject[INI_MAX_OBJECT_NAME+1];
    char    szProperty[INI_MAX_PROPERTY_NAME+1];
    char    szValue[INI_MAX_PROPERTY_VALUE+1];
    char    szDriver[10000];
    char    szPathOut[ODBC_FILENAME_MAX+1];
    DWORD   nUsageCount = 0;
    char    *pChar      = NULL;

#ifdef __OS2__
    if ( iniOpen( &hIni, pszTemplate, "#;", '[', ']', '=', FALSE, 0L ) != INI_SUCCESS )
#else
    if ( iniOpen( &hIni, pszTemplate, "#;", '[', ']', '=', FALSE ) != INI_SUCCESS )
#endif
    {
        if ( cVerbose == 0 ) printf( "odbcinst: iniOpen failed on %s.\n", pszTemplate );
        return 1;
    }

    memset( szDriver, '\0', 10000 );
    pChar = szDriver;

    iniObjectFirst( hIni );
    while ( iniObjectEOL( hIni ) == FALSE )
    {
        iniObject( hIni, szObject );
        sprintf( pChar, "%s", szObject );
        pChar += ( strlen( szObject ) + 1 );

        iniPropertyFirst( hIni );
        while ( iniPropertyEOL( hIni ) == FALSE )
        {
            iniProperty( hIni, szProperty );
            iniValue( hIni, szValue );
#ifdef HAVE_SNPRINTF
            snprintf( pChar, 10000 - ( pChar - szDriver ), "%s=%s", szProperty, szValue );
#else
            sprintf( pChar, "%s=%s", szProperty, szValue );
#endif
            pChar += ( strlen( szProperty ) + strlen( szValue ) + 2 );
            iniPropertyNext( hIni );
        }
        if ( SQLInstallDriverEx( szDriver, NULL, szPathOut, ODBC_FILENAME_MAX, NULL, ODBC_INSTALL_COMPLETE, &nUsageCount ) == FALSE )
        {
            SQLInstallerError( 1, &nError, szError, ODBC_FILENAME_MAX, NULL );
            if ( cVerbose == 0 ) printf( "odbcinst: SQLInstallDriverEx failed with %s.\n", szError );
            iniClose( hIni );
            return 1;
        }
        if ( cVerbose == 0 ) printf( "odbcinst: Driver installed. Usage count increased to %d. \n    Target directory is %s\n", (int)nUsageCount, szPathOut );
        memset( szDriver, '\0', 10000 );
        pChar = szDriver;
        iniObjectNext( hIni );
    }
    iniClose( hIni );


    return 0;
}

int DriverUninstall( char *pszDriver )
{
    DWORD   nUsageCount;

    if ( SQLRemoveDriver( pszDriver, FALSE, &nUsageCount ) == FALSE )
    {
        SQLInstallerError( 1, &nError, szError, ODBC_FILENAME_MAX, NULL );
        if ( cVerbose == 0 ) printf( "odbcinst: SQLRemoveDriver failed with %s.\n", szError );
        return 1;
    }

    if ( nUsageCount == 0 )
    {
        if ( cVerbose == 0 ) printf( "%s has been deleted (if it existed at all) because its usage count became zero\n", pszDriver );
    }
    else
    {
        if ( cVerbose == 0 ) printf( "%s usage count has been reduced to %d\n", pszDriver, (int)nUsageCount );
    }

    return 0;
}

int DriverQuery( char *pszDriver )
{
    char    szResults[4048];
    char    szValue[501];
    char *ptr;

    if ( pszDriver && (*pszDriver) )
    {
        /* list Driver details */
        if ( SQLGetPrivateProfileString( pszDriver, NULL, NULL, szResults, sizeof( szResults ) - 1, "ODBCINST.INI" ) < 1 )
        {
            SQLInstallerError( 1, &nError, szError, ODBC_FILENAME_MAX, NULL );
            if ( cVerbose == 0 ) printf( "odbcinst: SQLGetPrivateProfileString failed with %s.\n", szError );
            return 1;
        }
        printf( "[%s]\n", pszDriver );
        ptr = szResults;
        while ( *ptr )
        {
            printf( "%s=", ptr );
            if ( SQLGetPrivateProfileString( pszDriver, ptr, "", szValue, sizeof( szValue ) - 1, "ODBCINST.INI" ) > 0 )
            {
                printf( "%s", szValue );
            }
            printf( "\n" );
            ptr += strlen( ptr ) + 1;
        }
    }
    else
    {
        /* list Drivers */
        if ( SQLGetPrivateProfileString( NULL, NULL, NULL, szResults, sizeof( szResults ) - 1, "ODBCINST.INI" ) < 1 )
        {
            SQLInstallerError( 1, &nError, szError, ODBC_FILENAME_MAX, NULL );
            if ( cVerbose == 0 ) printf( "odbcinst: SQLGetPrivateProfileString failed with %s.\n", szError );
            return 1;
        }
        ptr = szResults;
        while ( *ptr )
        {
            printf( "[%s]\n", ptr );
            ptr += strlen( ptr ) + 1;
        }
    }

    return 0;
}

int DSNInstall( char *pszTemplate )
{
    HINI    hIni;
    char    szFileName[ODBC_FILENAME_MAX+1];
    char    szObject[INI_MAX_OBJECT_NAME+1];
    char    szProperty[INI_MAX_PROPERTY_NAME+1];
    char    szValue[INI_MAX_PROPERTY_VALUE+1];

#ifdef __OS2__
    if ( iniOpen( &hIni, pszTemplate, "#;", '[', ']', '=', FALSE, 0L ) != INI_SUCCESS )
#else
    if ( iniOpen( &hIni, pszTemplate, "#;", '[', ']', '=', FALSE ) != INI_SUCCESS )
#endif
    {
        if ( cVerbose == 0 ) printf( "odbcinst: iniOpen failed on %s.\n", pszTemplate );
        return 1;
    }

    if ( system_dsn )
    {
        SQLSetConfigMode( ODBC_SYSTEM_DSN );
    }
    else if ( user_dsn )
    {
        SQLSetConfigMode( ODBC_USER_DSN );
    }

    strcpy( szFileName, "ODBC.INI" );
    iniObjectFirst( hIni );
    while ( iniObjectEOL( hIni ) == FALSE )
    {
        iniObject( hIni, szObject );
        if ( SQLWritePrivateProfileString( szObject, NULL, NULL, szFileName ) == FALSE )
        {
            int i = 1;
            int ret;

            do
            {
                ret = SQLInstallerError( i, &nError, szError, ODBC_FILENAME_MAX, NULL );
                if ( cVerbose == 0 ) printf( "odbcinst: SQLWritePrivateProfileString failed with %s.\n", szError );
                i ++;
            }
            while ( ret == SQL_SUCCESS );

            iniClose( hIni );
            SQLSetConfigMode( ODBC_BOTH_DSN );
            return 1;
        }

        iniPropertyFirst( hIni );
        while ( iniPropertyEOL( hIni ) == FALSE )
        {
            iniProperty( hIni, szProperty );
            iniValue( hIni, szValue );

            if ( SQLWritePrivateProfileString( szObject, szProperty, szValue, szFileName ) == FALSE )
            {
                SQLInstallerError( 1, &nError, szError, ODBC_FILENAME_MAX, NULL );
                if ( cVerbose == 0 ) printf( "odbcinst: SQLWritePrivateProfileString failed with %s.\n", szError );
                iniClose( hIni );
                SQLSetConfigMode( ODBC_BOTH_DSN );
                return 1;
            }

            iniPropertyNext( hIni );
        }
        iniObjectNext( hIni );
    }

    iniClose( hIni );

    if ( cVerbose == 0 && from_stdin )
        printf( "odbcinst: Sections and Entries from stdin have been added to %s\n", szFileName );
    else if ( cVerbose )
        printf( "odbcinst: Sections and Entries from %s have been added to %s\n", pszTemplate, szFileName );

    return 0;
}

int DSNUninstall( char *pszDSN )
{
    UWORD   nConfigMode;
    char    *pMode;

    if ( system_dsn )
    {
        SQLSetConfigMode( ODBC_SYSTEM_DSN );
    }
    else if ( user_dsn )
    {
        SQLSetConfigMode( ODBC_USER_DSN );
    }

    if ( SQLGetConfigMode( &nConfigMode ) == FALSE )
    {
        SQLInstallerError( 1, &nError, szError, ODBC_FILENAME_MAX, NULL );
        if ( cVerbose == 0 ) printf( "odbcinst: SQLGetConfigMode failed with %s.\n", szError );
        return 1;
    }
    if ( SQLRemoveDSNFromIni( pszDSN ) == FALSE )
    {
        SQLInstallerError( 1, &nError, szError, ODBC_FILENAME_MAX, NULL );
        if ( cVerbose == 0 ) printf( "odbcinst: SQLRemoveDSNFromIni failed with %s.\n", szError );
        return 1;
    }

    switch ( nConfigMode )
    {
        case ODBC_SYSTEM_DSN:
            pMode = "ODBC_SYSTEM_DSN";
            break;
        case ODBC_USER_DSN:
            pMode = "ODBC_USER_DSN";
            break;
        case ODBC_BOTH_DSN:
            pMode = "ODBC_BOTH_DSN";
            break;
        default:
            pMode = "Unknown mode";
    }
    if ( cVerbose == 0 ) printf( "odbcinst: DSN removed (if it existed at all). %s was used as the search path.\n", pMode );

    return 0;
}

int DSNQuery( char *pszDSN )
{
    char    szResults[9601];
    char    szValue[501];
    char    *ptr;

    szResults[0] = '\0';

    if ( system_dsn )
        SQLSetConfigMode( ODBC_SYSTEM_DSN );
    else if ( user_dsn )
        SQLSetConfigMode( ODBC_USER_DSN );

    if ( pszDSN && (*pszDSN) )
    {
        /* list DSN details */
        if ( SQLGetPrivateProfileString( pszDSN, NULL, NULL, szResults, sizeof( szResults ) - 1, "ODBC.INI" ) < 1 )
        {
            SQLInstallerError( 1, &nError, szError, ODBC_FILENAME_MAX, NULL );
            if ( cVerbose == 0 ) printf( "odbcinst: SQLGetPrivateProfileString failed with %s.\n", szError );
            SQLSetConfigMode( ODBC_BOTH_DSN );
            return 1;
        }
        printf( "[%s]\n", pszDSN );
        ptr = szResults;
        while ( *ptr )
        {
            printf( "%s=", ptr );
            if ( SQLGetPrivateProfileString( pszDSN, ptr, "", szValue, sizeof( szValue ) - 1, "ODBC.INI" ) > 0 )
            {
                printf( "%s", szValue );
            }
            printf( "\n" );
            ptr += strlen( ptr ) + 1;
        }
    }
    else
    {
        /* list DSNs */
        if ( SQLGetPrivateProfileString( NULL, NULL, NULL, szResults, sizeof( szResults ) - 1, "ODBC.INI" ) < 1 )
        {
            SQLInstallerError( 1, &nError, szError, ODBC_FILENAME_MAX, NULL );
            if ( cVerbose == 0 ) printf( "odbcinst: SQLGetPrivateProfileString failed with %s.\n", szError );
            SQLSetConfigMode( ODBC_BOTH_DSN );
            return 1;
        }
        ptr = szResults;
        while ( *ptr )
        {
            printf( "[%s]\n", ptr );
            ptr += strlen( ptr ) + 1;
        }
    }

    SQLSetConfigMode( ODBC_BOTH_DSN );

    return 0;
}

void Syntax()
{
    if ( cVerbose != 0 )
        return;

    puts( szSyntax );
}

void PrintConfigInfo()
{
    char szFileName[ODBC_FILENAME_MAX+1];
    char b1[ ODBC_FILENAME_MAX + 1 ], b2[ ODBC_FILENAME_MAX + 1 ];

    printf( "unixODBC " VERSION "\n" );

    *szFileName = '\0';
    sprintf( szFileName, "%s/%s", odbcinst_system_file_path( b1 ), odbcinst_system_file_name( b2 ));
    printf( "DRIVERS............: %s\n", szFileName ); 

    *szFileName = '\0';
    _odbcinst_SystemINI( szFileName, FALSE );
    printf( "SYSTEM DATA SOURCES: %s\n", szFileName ); 

    *szFileName = '\0';
    _odbcinst_FileINI( szFileName );
    printf( "FILE DATA SOURCES..: %s\n", szFileName ); 

    *szFileName = '\0';
    _odbcinst_UserINI( szFileName, FALSE );
    printf( "USER DATA SOURCES..: %s\n", szFileName ); 

	printf( "SQLULEN Size.......: %ld\n", (long) sizeof( SQLULEN )); 
	printf( "SQLLEN Size........: %ld\n", (long) sizeof( SQLLEN )); 
	printf( "SQLSETPOSIROW Size.: %ld\n", (long) sizeof( SQLSETPOSIROW )); 
}

int main( int argc, char *argv[] )
{
    int     nArg;
    char    cAction     = 0;
    char    cObject     = 0;
    char    szTemplateINI[ODBC_FILENAME_MAX+1];
    char    szObjectName[INI_MAX_OBJECT_NAME+1];
    int     nReturn     = 0;

    cVerbose = 0;

    if ( argc < 2 )
    {
        Syntax();
        exit ( 1 );
    }

    szTemplateINI[0]    = '\0';
    szObjectName[0]     = '\0';
    for ( nArg = 1; nArg < argc; nArg++ )
    {
        if ( argv[nArg][0] == '-' )
        {
            switch ( argv[nArg][1] )
            {
                /* Action */
                case 'i':
                case 'u':
                case 'q':
                    cAction = argv[nArg][1];
                    break;
                case 'j':
                    PrintConfigInfo();
                    exit(0);
                case '-':
                    printf( "unixODBC " VERSION "\n" );
                    exit(0);
                    /* Object */
                case 'c':
                case 'd':
                case 's':
                case 'm':
                    cObject = argv[nArg][1];
                    break;
                    /* Options */
                case 'n':
                    if ( nArg < argc-1 )
                        strncpy( szObjectName, argv[nArg+1], INI_MAX_OBJECT_NAME );
                    break;
                case 'f':
                    if ( nArg < argc-1 )
                        strncpy( szTemplateINI, argv[nArg+1], ODBC_FILENAME_MAX );
                    break;
                case 'r':
                    from_stdin = 1;
                    break;
                case 'v':
                    cVerbose = argv[nArg][1];
                    break;
                case 'l':
                    system_dsn = 1;
                    if ( user_dsn )
                    {
                        if ( cVerbose == 0 ) printf( "odbcinst: cannot install both user and system dsn at the same time");
                        exit( -2 );
                    }
                    break;
                case 'h':
                    user_dsn = 1;
                    if ( system_dsn )
                    {
                        if ( cVerbose == 0 ) printf( "odbcinst: cannot install both user and system dsn at the same time");
                        exit( -2 );
                    }
                    break;
                default:
                    if ( cVerbose == 0 ) printf( "odbcinst: Unknown option %c\n", argv[nArg][1] );
                    exit( -1 );
            }
        }
    }

    /* DRIVERS */
    if ( cObject == 'd' )
    {
        /* install */
        if ( cAction == 'i' )
        {
            if ( szTemplateINI[0] != '\0' )
                nReturn = DriverInstall( szTemplateINI );
            else if ( from_stdin )
                nReturn = DriverInstall( STDINFILE );
            else
            {
                if ( cVerbose == 0 ) printf( "odbcinst: Please supply -f template.ini (The fileformat of template.ini is identical to odbcinst.ini and odbc.ini, respectively)\n" );
                Syntax();
                exit( 1 );
            }
        }
        /* uninstall */
        else if ( cAction == 'u' )
        {
            if ( szObjectName[0] != '\0' )
                nReturn = DriverUninstall( szObjectName );
            else
            {
                if ( cVerbose == 0 ) printf( "odbcinst: Please supply -n FriendlyDriverName \n" );
                Syntax();
                exit( 1 );
            }
        }
        /* query */
        else if ( cAction == 'q' )
            nReturn = DriverQuery( szObjectName );
        else
        {
            if ( cVerbose == 0 ) printf( "odbcinst: Invalid Action for Object\n" );
            Syntax();
            exit( 1 );
        }

    }
    /* DATA SOURCES */
    else if ( cObject == 's' )
    {
        /* install */
        if ( cAction == 'i' )
        {
            if ( szTemplateINI[0] != '\0' )
                nReturn = DSNInstall( szTemplateINI );
            else if ( from_stdin )
                nReturn = DSNInstall( STDINFILE );
            else
            {
                if ( cVerbose == 0 ) printf( "odbcinst: Please supply -f template.ini \n" );
                Syntax();
                exit( 1 );
            }
        }
        /* uninstall */
        else if ( cAction == 'u' )
        {
            if ( szObjectName[0] != '\0' )
                nReturn = DSNUninstall( szObjectName );
            else
            {
                if ( cVerbose == 0 ) printf( "odbcinst: Please supply -n DataSourceName \n" );
                Syntax();
                exit( 1 );
            }
        }
        /* query */
        else if ( cAction == 'q' )
            nReturn = DSNQuery( szObjectName );
        else
        {
            if ( cVerbose == 0 ) printf( "odbcinst: Invalid Action for Object\n" );
            Syntax();
            exit( 1 );
        }

    }
    else if ( cObject == 'c' )
    {
        nReturn = CreateDataSource( szObjectName );
    }
    else if ( cObject == 'm' )
    {
        nReturn = ManageDataSources();
    }
    else
    {
        if ( cVerbose == 0 ) printf( "odbcinst: Invalid Object\n" );
        Syntax();
        exit( 1 );
    }

    exit( nReturn );
}

