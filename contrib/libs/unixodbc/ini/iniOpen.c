/**********************************************************************************
 * iniOpen
 *
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * PAH = Peter Harvey		- pharvey@codebydesign.com
 * -----------------------------------------------
 *
 * PAH	06.MAR.99	Can now create file-less INI. Pass NULL for
 *					pszFileName. Then copy a file name into hIni->szFileName
 *					before calling iniCommit.
 **************************************************/

#include <config.h>
#include "ini.h"

/*
 * Changes sent by MQJoe, to avoid limit on number of open file handles
 */

/***************************************************
 * Override fstream command to overcome 255 file
 * handle limit
 ***************************************************/

#include <fcntl.h>
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <stdarg.h>
#include <errno.h>

#if defined( HAVE_VSNPRINTF ) && defined( USE_LL_FIO )

FILE *uo_fopen( const char *filename, const char *mode )
{
    int fp;
    long oMode = 0, pMode = 0;

    pMode = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;

    switch ( mode[0] )
    {
        case 'r':
            oMode = O_RDONLY;
            break;

        case 'w':
            oMode = O_RDWR | O_CREAT | O_TRUNC;
            break;

        case 'o':
            oMode = O_RDWR | O_CREAT | O_TRUNC;
            break;

        case 'a':
            oMode = O_CREAT | O_APPEND | O_WRONLY;
            break;

        default:
            return FALSE;
    }

    fp = open(filename, oMode, pMode );

    return(fp != -1) ? (FILE*)fp : NULL;
}

int uo_fclose( FILE *stream )
{
    close((int)stream);
    return 0;
}

char *uo_fgets( char *buffer, int n, FILE *stream )
{
    int fp = (int)stream;
    char ch;
    int i = 0, c = 0;

    buffer[0] = 0;

    do
    {
        c = read(fp, &ch, 1);

        if ( c == 1 )
        {
            buffer[i++] = ch;

            if ( ch == '\n' )
                break;
        }
    } while ( c && i < n );

    buffer[i] = 0;

    return(c) ? buffer : NULL;
}

int uo_vfprintf( FILE *stream, const char *fmt, va_list ap)
{
    int fp = (int)stream;
    long lNeededSize = 256;
    char* szBuffer = NULL;
    long lBufSize = 0;
    int r = 0;

    do
    {
        if ( lNeededSize > lBufSize )
        {
            if ( szBuffer )
                free(szBuffer);
            szBuffer = (char*)malloc(lNeededSize);
            lBufSize = lNeededSize;
        }

        lNeededSize =  vsnprintf(szBuffer, lBufSize, fmt, ap);
        lNeededSize++;
    }
    while ( lNeededSize > lBufSize );

    r = write(fp, szBuffer, (lNeededSize - 1) );

    if ( szBuffer )
        free(szBuffer);

    return r;
}

int uo_fprintf( FILE *stream, const char *fmt, ...)
{
    int r;
    va_list ap;

    va_start(ap, fmt);

    r = uo_vfprintf(stream,fmt,ap);

    va_end(ap);

    return r;
}

#endif

/***************************************************/

#ifdef __OS2__

int iniOpen( HINI *hIni, char *pszFileName, char *cComment, char cLeftBracket, char cRightBracket, char cEqual, int bCreate, int bFileType )
{
    FILE    *hFile;
    char    szLine[INI_MAX_LINE+1];
    char    szObjectName[INI_MAX_OBJECT_NAME+1];
    char    szPropertyName[INI_MAX_PROPERTY_NAME+1];
    char    szPropertyValue[INI_MAX_PROPERTY_VALUE+1];
    int     nValidFile;

    char   *ObjectList;
    char   *PropertyList;
    char   *ValueList;
    int     numberObject;
    int     ObjectNumber;
    int     numberProperty;
    int     PropertyNumber;
    int     nValidProperty;
    char    *tmpObjectName;
    char    *tmpPropertyName;
    char    *tmpProperyValue;

#ifdef __OS2DEBUG__
    printf("iniOpen entered \n");
#endif    

    /* INIT STATEMENT */
    *hIni = malloc( sizeof(INI) );
    if ( !*hIni )
        return INI_ERROR;
    if ( pszFileName && pszFileName != STDINFILE )
        strncpy((*hIni)->szFileName, pszFileName, ODBC_FILENAME_MAX );
    else if ( pszFileName == STDINFILE )
        strncpy((*hIni)->szFileName, "stdin", ODBC_FILENAME_MAX );
    else
        strncpy((*hIni)->szFileName, "", ODBC_FILENAME_MAX );

    strcpy( (*hIni)->cComment, cComment );
    (*hIni)->cLeftBracket       = cLeftBracket;
    (*hIni)->cRightBracket      = cRightBracket;
    (*hIni)->cEqual             = cEqual;
    (*hIni)->bChanged           = FALSE;
    (*hIni)->hCurObject         = NULL;
    (*hIni)->hFirstObject       = NULL;
    (*hIni)->hLastObject        = NULL;
    (*hIni)->nObjects           = 0;
    (*hIni)->bReadOnly          = 0;

    (*hIni)->iniFileType    = bFileType;


#ifdef __OS2DEBUG__
    printf("iniOpen file is mode %d \n", bFileType);
#endif    

    /* OPEN FILE */
    if ( pszFileName )
    {
        if ( pszFileName == STDINFILE )
        {
            hFile = stdin;
            (*hIni)->iniFileType    = 0; /* stdin is always text */
        }
        else
        {
            if ( (*hIni)->iniFileType == 0 )
                hFile = uo_fopen( pszFileName, "r" );
            else
                hFile = (FILE *)iniOS2Open( pszFileName);
        }

        if ( !hFile )
        {
            /*
             * This could fail because of something other than the file not existing...
             */

            if ( bCreate == TRUE )
            {
                if ( (*hIni)->iniFileType == 0 )
                    hFile = uo_fopen( pszFileName, "w+" );
                else
                    hFile = (FILE *)iniOS2Open( pszFileName);
            }
        }

        if ( !hFile )
        {
            free( *hIni );
            *hIni = NULL;
            return INI_ERROR;
        }

        if ( (*hIni)->iniFileType == 1 )
        {

            nValidFile = INI_ERROR;
            ObjectList = (char *)iniOS2LoadObjectList( hFile, &numberObject);
            if ( numberObject > 0 )
            {
                nValidFile = INI_SUCCESS;

                ObjectNumber = 0;
                do
                {
                    tmpObjectName = (char *)(ObjectList + ObjectNumber);
                    strcpy(szObjectName, tmpObjectName);
                    iniObjectInsert( (*hIni),  szObjectName  );


                    PropertyList = (char *)iniOS2LoadPropertyList( hFile, szObjectName, &numberProperty);
                    if ( numberProperty > 0 )
                    {


                        PropertyNumber = 0;
                        do
                        {

                            tmpPropertyName = PropertyList + PropertyNumber;
                            strcpy(szPropertyName, tmpPropertyName);
                            ValueList = (char *)iniOS2Read( hFile, szObjectName, szPropertyName, szPropertyValue);
                            strcpy(szPropertyValue, ValueList);
                            iniPropertyInsert( (*hIni), szPropertyName, szPropertyValue);

                            PropertyNumber = PropertyNumber + strlen(szPropertyName) + 1;
                        } while ( PropertyNumber < numberProperty );
                        free(PropertyList);
                    }

                    ObjectNumber = ObjectNumber + strlen(szObjectName) + 1;
                } while ( ObjectNumber < numberObject );
                free(ObjectList);

            }
        }
        else
        {

            nValidFile = _iniScanUntilObject( *hIni, hFile, szLine );
            if ( nValidFile == INI_SUCCESS )
            {
                char *ptr;
                do
                {
                    if ( szLine[0] == cLeftBracket )
                    {
                        _iniObjectRead( (*hIni), szLine, szObjectName );
                        iniObjectInsert( (*hIni), szObjectName );
                    }
                    else if ( (strchr( cComment, szLine[0] ) == NULL ) && !isspace(szLine[0]) )
                    {
                        _iniPropertyRead( (*hIni), szLine, szPropertyName, szPropertyValue );
                        iniPropertyInsert( (*hIni), szPropertyName, szPropertyValue );
                    }

                } while ( (ptr = uo_fgets( szLine, INI_MAX_LINE, hFile )) != NULL );
            }

        }

        if ( nValidFile == INI_ERROR )
        {
            /* INVALID FILE */
            if ( hFile != NULL )
            {
                if ( (*hIni)->iniFileType == 0 )
                    uo_fclose( hFile );
                else
                    iniOS2Close(hFile);
            }
            free( *hIni );
            *hIni = NULL;
            return INI_ERROR;
        }

        /* CLEANUP */
        if ( hFile != NULL )
        {
            if ( (*hIni)->iniFileType == 0 )
            {
                uo_fclose( hFile );
            }
            else
                iniOS2Close(hFile);
        }

        iniObjectFirst( *hIni );

    } /* if file given */

    return INI_SUCCESS;
}




#else
int iniOpen( HINI *hIni, char *pszFileName, char *cComment, char cLeftBracket, char cRightBracket, char cEqual, int bCreate )
{
    FILE    *hFile;
    char    szLine[INI_MAX_LINE+1];
    char    szObjectName[INI_MAX_OBJECT_NAME+1];
    char    szPropertyName[INI_MAX_PROPERTY_NAME+1];
    char    szPropertyValue[INI_MAX_PROPERTY_VALUE+1];
    int     nValidFile;

    /* INIT STATEMENT */
    *hIni = malloc( sizeof(INI) );
    if ( !*hIni )
        return INI_ERROR;
    if ( pszFileName && pszFileName != STDINFILE )
        strncpy((*hIni)->szFileName, pszFileName, ODBC_FILENAME_MAX );
    else if ( pszFileName == STDINFILE )
        strncpy((*hIni)->szFileName, "stdin", ODBC_FILENAME_MAX );
    else
        strncpy((*hIni)->szFileName, "", ODBC_FILENAME_MAX );

    strcpy( (*hIni)->cComment, cComment );
    (*hIni)->cLeftBracket       = cLeftBracket;
    (*hIni)->cRightBracket      = cRightBracket;
    (*hIni)->cEqual             = cEqual;
    (*hIni)->bChanged           = FALSE;
    (*hIni)->hCurObject         = NULL;
    (*hIni)->hFirstObject       = NULL;
    (*hIni)->hLastObject        = NULL;
    (*hIni)->nObjects           = 0;
    (*hIni)->bReadOnly          = 0;

    /* OPEN FILE */
    if ( pszFileName )
    {
	errno = 0;
        if ( pszFileName == STDINFILE )
        {
            hFile = stdin;
        }
        else
        {
            hFile = uo_fopen( pszFileName, "r" );
        }

    if ( ( !hFile ) &&
        ( errno != ENFILE ) && ( errno != EMFILE ) &&
        ( errno != ENOMEM ) && ( errno != EACCES ) && 
        ( errno != EFBIG ) && ( errno != EINTR ) &&
        ( errno != ENOSPC ) && ( errno != EOVERFLOW ) &&
        ( errno != EWOULDBLOCK ))
        {

            /*
             * This could fail because of something other than the file not existing...
             * so open as w+ just in case
             */

            if ( bCreate == TRUE )
            {
                hFile = uo_fopen( pszFileName, "w+" );
            }
        }

        if ( !hFile )
        {
            free( *hIni );
            *hIni = NULL;
            return INI_ERROR;
        }

        nValidFile = _iniScanUntilObject( *hIni, hFile, szLine );
        if ( nValidFile == INI_SUCCESS )
        {
            char *ptr;
            ptr = szLine;
            do
            {
                /*
                 * remove leading spaces
                 */
                while( isspace( *ptr )) {
                    ptr ++;
                }
                if ( *ptr == '\0' ) {
                    continue;
                }
                if ( *ptr == cLeftBracket )
                {
                    _iniObjectRead( (*hIni), ptr, szObjectName );
                    iniObjectInsert( (*hIni), szObjectName );
                }
                else if ((strchr( cComment, *ptr ) == NULL ))
                {
                    _iniPropertyRead( (*hIni), ptr, szPropertyName, szPropertyValue );
                    iniPropertyInsert( (*hIni), szPropertyName, szPropertyValue );
                }

            } while ( (ptr = uo_fgets( szLine, INI_MAX_LINE, hFile )) != NULL );
        }
        else if ( nValidFile == INI_ERROR )
        {
            /* INVALID FILE */
            if ( hFile != NULL )
                uo_fclose( hFile );
            free( *hIni );
            *hIni = NULL;
            return INI_ERROR;
        }

        /* CLEANUP */
        if ( hFile != NULL )
            uo_fclose( hFile );

        iniObjectFirst( *hIni );

    } /* if file given */

    return INI_SUCCESS;
}

#endif

