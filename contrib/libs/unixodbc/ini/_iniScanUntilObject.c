/**********************************************************************************
 * _iniScanUntilObject
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

int _iniScanUntilObject( HINI hIni, FILE *hFile, char *pszLine )
{
	/* SCAN UNTIL WE GET TO AN OBJECT NAME OR EOF */
	pszLine[0] = '\0';
    while ( 1 )
    {
        if ( uo_fgets( pszLine, INI_MAX_LINE, hFile ) == NULL )
        {
            return INI_NO_DATA;
        }
/*
printf( "[PAH][%s][%d] Line=[%s]\n", __FILE__, __LINE__, pszLine );
*/
        if ( pszLine[0] == hIni->cLeftBracket )
        {
			return INI_SUCCESS;
        }
        iniAllTrim( pszLine );
        if ( pszLine[0] == '\0' )
        {
            continue;
        }
        if ( strchr( hIni->cComment, pszLine[0] ) == NULL )
        {
			return INI_ERROR;
        }
    }

    return INI_SUCCESS;
}

int _iniScanUntilNextObject( HINI hIni, FILE *hFile, char *pszLine )
{
	/* SCAN UNTIL WE GET TO AN OBJECT NAME OR EOF, SKIPPING BODY */
	pszLine[0] = '\0';
    while ( 1 )
    {
        if ( uo_fgets( pszLine, INI_MAX_LINE, hFile ) == NULL )
        {
            return INI_NO_DATA;
        }

        if ( pszLine[0] == hIni->cLeftBracket )
        {
			return INI_SUCCESS;
        }
    }

    return INI_SUCCESS;
}



