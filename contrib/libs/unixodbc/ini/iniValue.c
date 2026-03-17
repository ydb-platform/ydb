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

/******************************
 * iniValue
 *
 ******************************/
int iniValue( HINI hIni, char *pszValue )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

	if ( hIni->hCurObject == NULL )
		return INI_NO_DATA;
	
	if ( hIni->hCurProperty == NULL )
		return INI_NO_DATA;
	
    strncpy( pszValue, hIni->hCurProperty->szValue, INI_MAX_PROPERTY_VALUE );

    return INI_SUCCESS;
}



