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

int iniObjectNext( HINI hIni )
{
    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;

	if ( hIni->hCurObject == NULL )
		return INI_NO_DATA;
	
	hIni->hCurObject 	= hIni->hCurObject->pNext;
	iniPropertyFirst( hIni );

	if ( hIni->hCurObject == NULL )
		return INI_NO_DATA;

	return INI_SUCCESS;
}


