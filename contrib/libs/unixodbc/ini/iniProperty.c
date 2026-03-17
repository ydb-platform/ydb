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
 * Peter Harvey		- pharvey@codebydesign.com
 **************************************************/

#include <config.h>
#include "ini.h"

int iniProperty( HINI hIni, char *pszProperty )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

	if ( hIni->hCurObject == NULL )
		return INI_NO_DATA;
	
	if ( hIni->hCurProperty == NULL )
		return INI_NO_DATA;

    /* Ok */
    strncpy( pszProperty, hIni->hCurProperty->szName, INI_MAX_PROPERTY_NAME );

    return INI_SUCCESS;
}



