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

int iniObjectUpdate( HINI hIni, char *pszObject )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

	if ( hIni->hCurObject == NULL )
        return INI_ERROR;
	
    /* Ok */
    strncpy( hIni->hCurObject->szName, pszObject, INI_MAX_OBJECT_NAME );

    return INI_SUCCESS;
}



