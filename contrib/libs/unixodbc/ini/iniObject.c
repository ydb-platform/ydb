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
 * iniObject
 *
 ******************************/
int iniObject( HINI hIni, char *pszObject )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

	if ( hIni->hCurObject == NULL )
		return INI_NO_DATA;
	
    /* Ok */
	strncpy( pszObject, hIni->hCurObject->szName, INI_MAX_OBJECT_NAME );

    return INI_SUCCESS;
}



