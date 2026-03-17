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
 * iniPropertyDelete
 *
 ******************************/
int iniPropertyDelete( HINI hIni )
{
	HINIPROPERTY	hProperty;
	HINIOBJECT		hObject;

    /* SANITY CHECKS */
    if ( hIni == NULL )
        return INI_ERROR;
	if ( hIni->hCurObject == NULL )
		return INI_ERROR;
	if ( hIni->hCurProperty == NULL )
		return INI_NO_DATA;
	
	hObject		= hIni->hCurObject;
	hProperty	= hIni->hCurProperty;

	if ( hObject->hFirstProperty == hProperty )
		hObject->hFirstProperty = hProperty->pNext;
	if ( hObject->hLastProperty == hProperty )
		hObject->hLastProperty = hProperty->pPrev;

	hIni->hCurProperty		= NULL;
	if ( hProperty->pNext )
	{
		hProperty->pNext->pPrev = hProperty->pPrev;
		hIni->hCurProperty		= hProperty->pNext;
	}
	if ( hProperty->pPrev )
	{
		hProperty->pPrev->pNext = hProperty->pNext;
		hIni->hCurProperty		= hProperty->pPrev;
	}
	hObject->nProperties--;

	/* FREE MEMORY */
	free( hProperty );

	return INI_SUCCESS;
}


