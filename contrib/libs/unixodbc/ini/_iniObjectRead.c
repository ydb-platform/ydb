/**********************************************************************************
 * _iniObjectRead
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

int _iniObjectRead( HINI hIni, char *szLine, char *pszObjectName )
{
    int     	nChar;

    /* SANITY CHECK */
    if ( hIni == NULL )
        return INI_ERROR;

    /* SCAN LINE TO EXTRACT OBJECT NAME WITH NO BRACKETS	*/
    nChar = 1;
    while ( 1 )
    {
        if ( (szLine[nChar] == '\0') || (nChar == INI_MAX_OBJECT_NAME) )
        {
            pszObjectName[nChar-1] = '\0';
            break;
        }

        if ( szLine[nChar] == hIni->cRightBracket )
        {
            pszObjectName[nChar-1] = '\0';
            break;
        }
        pszObjectName[nChar-1] = szLine[nChar];
        nChar++;
    }
	iniAllTrim( pszObjectName );

    return INI_SUCCESS;
}


