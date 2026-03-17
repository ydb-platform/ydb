/**********************************************************************************
 * _iniDump
 *
 * Dump contents to hStream.
 *
 * - iniCommit calls this. You can bypass iniCommit restrictions to get some debugging information by calling directly.
 * - Make sure the stream is open before calling.
 * - leaves list position at iniObjectFirst()
 * - always returns true
 *
 **************************************************
 * This code was created by Peter Harvey @ CodeByDesign.
 * Released under LGPL 28.JAN.99
 *
 * Contributions from...
 * -----------------------------------------------
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/

#include <config.h>
#include "ini.h"

int __iniDebug( HINI hIni )
{
    /* SANITY CHECK */
    if ( hIni == NULL )
        return INI_ERROR;

    /* SCAN OBJECTS */
    iniObjectFirst( hIni );
    while ( iniObjectEOL( hIni ) == FALSE )
    {
        printf( "%c%s%c\n", hIni->cLeftBracket, hIni->hCurObject->szName, hIni->cRightBracket );

        iniPropertyFirst( hIni );
        while ( iniPropertyEOL( hIni ) == FALSE )
        {
            printf( "%s%c%s\n", hIni->hCurProperty->szName, hIni->cEqual, hIni->hCurProperty->szValue );
            iniPropertyNext( hIni );
        }
        printf( "\n" );
        iniPropertyFirst( hIni );
        iniObjectNext( hIni );
    }
    iniObjectFirst( hIni );

    return INI_SUCCESS;
}

int _iniDump( HINI hIni, FILE *hStream )
{
    /* SANITY CHECK */
    if ( hIni == NULL )
        return INI_ERROR;

    if ( !hStream )
        return INI_ERROR;

    /* SCAN OBJECTS */
    iniObjectFirst( hIni );
    while ( iniObjectEOL( hIni ) == FALSE )
    {
#ifdef __OS2__
        if ( hIni->iniFileType == 0 )
#endif
            uo_fprintf( hStream, "%c%s%c\n", hIni->cLeftBracket, hIni->hCurObject->szName, hIni->cRightBracket );
        iniPropertyFirst( hIni );
        while ( iniPropertyEOL( hIni ) == FALSE )
        {
#ifdef __OS2__
            if ( hIni->iniFileType == 0 )
            {
#endif
                uo_fprintf( hStream, "%s%c%s\n", hIni->hCurProperty->szName, hIni->cEqual, hIni->hCurProperty->szValue );
#ifdef __OS2__
            }
            else
            {
                iniOS2Write( hStream,  hIni->hCurObject->szName,  hIni->hCurProperty->szName, hIni->hCurProperty->szValue);
            }  
#endif
            iniPropertyNext( hIni );
        }
#ifdef __OS2__
        if ( hIni->iniFileType == 0 )
#endif
            uo_fprintf( hStream, "\n" );

        iniPropertyFirst( hIni );
        iniObjectNext( hIni );
    }
    iniObjectFirst( hIni );

    return INI_SUCCESS;
}


