/**********************************************************************************
 * iniCommit
 *
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

int iniCommit( HINI hIni )
{
    FILE    *hFile;

    /* SANITY CHECK */
    if ( hIni == NULL )
        return INI_ERROR;

    if ( hIni->bReadOnly )
        return INI_ERROR;

    /* OPEN FILE */

#ifdef __OS2__
    if (hIni->iniFileType == 0)
    {
#endif
        hFile = uo_fopen( hIni->szFileName, "w" );
#ifdef __OS2__
    }
    else
    {
        hFile = (FILE *)iniOS2Open (hIni->szFileName);
    }
#endif
    if ( !hFile )
        return INI_ERROR;

	_iniDump( hIni, hFile );

    /* CLEANUP */
    if ( hFile != NULL )
    {
#ifdef __OS2__
        if (hIni->iniFileType == 0)
#endif
		    uo_fclose( hFile );
#ifdef __OS2__
        else
            iniOS2Close( hFile);
#endif
     }

    return INI_SUCCESS;
}

