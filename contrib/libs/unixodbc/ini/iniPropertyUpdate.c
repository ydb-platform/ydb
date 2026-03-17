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

int iniPropertyUpdate( HINI hIni, char *pszProperty, char *pszValue )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

	if ( hIni->hCurObject == NULL )
        return INI_ERROR;
	
	if ( hIni->hCurProperty == NULL )
        return INI_ERROR;

    /* Ok */
    strncpy( hIni->hCurProperty->szName, pszProperty, INI_MAX_PROPERTY_NAME );
    strncpy( hIni->hCurProperty->szValue, pszValue, INI_MAX_PROPERTY_VALUE );

    return INI_SUCCESS;
}



