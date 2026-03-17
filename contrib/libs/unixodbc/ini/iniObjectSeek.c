/**********************************************************************************
 * .
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
 * PAH	19.MAR.99	Now sets hCurProperty to hFirstProperty when found
 **************************************************/

#include <config.h>
#include "ini.h"

int iniObjectSeek( HINI hIni, char *pszObject )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

	iniObjectFirst( hIni );
	while ( iniObjectEOL( hIni ) == FALSE )
	{
        if ( strcasecmp( pszObject, hIni->hCurObject->szName ) == 0 )
            return INI_SUCCESS;
		iniObjectNext( hIni );
	}

	return INI_NO_DATA;
}


